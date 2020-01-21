import pandas as pd
import numpy as np
from scipy.optimize import root
from cboe_exchange_holidays_v3 import datelike_to_timestamp

DAY_OFFSET = pd.DateOffset(days=1)
ONE_YEAR = pd.Timedelta(days=365)


def change_6_months(datelike):
    """ Return date that is 6 months forward
        NOTE: an end of month date will return the last day of the month that is 6 months forward
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    next_date = date + DAY_OFFSET
    if date.month != next_date.month:
        # Date is end-of-month; handle end of month differences
        return next_date + pd.DateOffset(months=6) - DAY_OFFSET
    else:
        return date + pd.DateOffset(months=6)


def is_leap_year(year):
    """ Return True iff year is a leap year """
    if year % 4 == 0:
        if year % 100 != 0 or year % 400 == 0:
            return True
    return False


def change_year(datelike, new_year):
    """ Return date with year changed to specified year
        NOTE: last day of February will return last day of February in the specified year
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param new_year: year to change given date's year to
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    # Handle end of February, which may change depending on year
    if date.month == 2 and date.day == 28 and is_leap_year(new_year):
        return date.replace(year=new_year, day=29)  # 28->29
    elif date.month == 2 and date.day == 29 and not is_leap_year(new_year):
        return date.replace(day=28, year=new_year)  # 29->28
    else:
        return date.replace(year=new_year)


def get_coupon_status(maturity_datelike, settle_datelike):
    """ Derive current semiannual coupon period details - start, end, number of days, elapsed days
    :param maturity_datelike: maturity date of bond used as basis for coupon schedule
    :param settle_datelike: settlement date of bond (business day after trade date) used for coupon accrual
    :return: (previous coupon date, next coupon date,
              number of days in current coupon period, number of days since last coupon)
    """
    maturity_date = datelike_to_timestamp(maturity_datelike)
    settle_date = datelike_to_timestamp(settle_datelike)
    year = settle_date.year
    maturity_date_6_months = change_6_months(maturity_date)
    # Create timeline of coupons close to settle date
    nearest_coupons_timeline = pd.Series(sorted(
        [change_year(maturity_date, year - 1), change_year(maturity_date_6_months, year - 1),
         change_year(maturity_date, year), change_year(maturity_date_6_months, year),
         change_year(maturity_date, year + 1), change_year(maturity_date_6_months, year + 1)]))
    # Find actual coupon period details
    elapsed_days = settle_date - nearest_coupons_timeline
    prev_idx = elapsed_days[elapsed_days >= pd.Timedelta(0)].idxmin()  # Most recent coupon
    next_idx = prev_idx + 1
    prev_coupon_date = nearest_coupons_timeline[prev_idx]
    next_coupon_date = nearest_coupons_timeline[next_idx]
    days_in_period = next_coupon_date - prev_coupon_date
    days_since_coupon = elapsed_days[prev_idx]
    return prev_coupon_date, next_coupon_date, days_in_period.days, days_since_coupon.days


def get_remaining_coupon_periods(maturity_datelike=None, settle_datelike=None,
                                 n_remaining_coupons=None, remaining_first_period=1.0):
    """ Calculate array of (potentially non-whole) discount rate (coupon) periods
        NOTE: every parameter defaults to None in order to allow 2 input configurations, in order of precedence:
              1) maturity_datelike and settle_datelike
              2) n_remaining_coupons and (optional) remaining_first_period
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining coupons up to maturity of bond
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :return: numpy/pandas array of periods, e.g. 1, 2, 3... or 0.690, 1.690, 2.690...
    """
    if n_remaining_coupons is None:
        if maturity_datelike is None or settle_datelike is None:
            raise ValueError("Input parameter configuration used incorrectly.")
        # Use settlement and maturity to calculate number of remaining coupons and remaining first period
        maturity_date = datelike_to_timestamp(maturity_datelike)
        settle_date = datelike_to_timestamp(settle_datelike)
        prev_coupon_date, _, days_in_period, days_since_coupon = get_coupon_status(maturity_date, settle_date)
        n_remaining_coupons = round(
            (maturity_date - prev_coupon_date) / ONE_YEAR * 2)  # Number of semiannual coupons remaining
        remaining_first_period = 1 - days_since_coupon/days_in_period
    remaining_coupon_periods = remaining_first_period + np.arange(n_remaining_coupons)
    return remaining_coupon_periods


def get_price_from_yield(coupon, ytm_bey, maturity_datelike=None, settle_datelike=None,
                         n_remaining_coupons=None, remaining_first_period=1.0, remaining_coupon_periods=None,
                         get_clean=False, verbose=False):
    """ Calculate dirty price of semiannual coupon bond
        NOTE: In order to derive the maturity, please supply one of the following three configurations:
              1) maturity_datelike and settle_datelike
              2) n_remaining_coupons and (optional) remaining_first_period
              3) remaining_coupon_periods
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param ytm_bey: bond equivalent yield to maturity percentage of bond, e.g. 2.858
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining coupons up to maturity of bond; set not None for configuration 2
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array; set not None for configuration 3
    :param get_clean: set True to return clean price rather than dirty price
    :param verbose: set True to print discounted cash flows
    :return: dirty price (unless get_clean is True) of bond with 100 as par
    """
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = get_remaining_coupon_periods(maturity_datelike, settle_datelike,
                                                                n_remaining_coupons, remaining_first_period)
    coupon_payment = coupon/2
    cash_flows = np.full_like(remaining_coupon_periods, coupon_payment)
    cash_flows[-1] += 100  # Face value delivered at maturity
    ytm_semiannual = ytm_bey/2 / 100    # Convert to non-percentage
    discount_factors = 1 / (1 + ytm_semiannual)**remaining_coupon_periods
    discounted_cash_flows = cash_flows * discount_factors
    calc_dirty_price = discounted_cash_flows.sum()
    elapsed_first_period = 1 - remaining_coupon_periods[0]
    accrued_interest = elapsed_first_period * coupon_payment
    calc_clean_price = calc_dirty_price - accrued_interest
    if verbose:
        for i, discounted_payment in enumerate(discounted_cash_flows, 1):
            print(f"Discounted Payment {i}: {discounted_payment}")
        print(f"Calculated Dirty Price: {calc_dirty_price}")
        print(f"Calculated Clean Price: {calc_clean_price}")
    if get_clean:
        return calc_clean_price
    else:
        return calc_dirty_price


def get_yield_to_maturity(coupon, price, maturity_datelike=None, settle_datelike=None,
                          n_remaining_coupons=None, remaining_first_period=1.0, remaining_coupon_periods=None,
                          is_clean_price=False):
    """ Back out bond equivalent yield to maturity from bond specs, price, and time to maturity
        NOTE: In order to derive the maturity, please supply one of the following three configurations:
              1) maturity_datelike and settle_datelike
              2) n_remaining_coupons and (optional) remaining_first_period
              3) remaining_coupon_periods
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param price: dirty price (unless is_clean_price is True) of bond
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining coupons up to maturity of bond; set not None for configuration 2
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array; set not None for configuration 3
    :param is_clean_price: set True if input price is clean (quoted) price rather than dirty price
    :return: BEY yield to maturity in percent
    """
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = get_remaining_coupon_periods(maturity_datelike, settle_datelike,
                                                                n_remaining_coupons, remaining_first_period)
    # Back out the yield
    solved_root = root(lambda ytm_bey:
                       get_price_from_yield(coupon, ytm_bey, None, None,
                                            remaining_coupon_periods=remaining_coupon_periods,
                                            get_clean=is_clean_price, verbose=False)
                       - price, x0=np.array(2))
    return solved_root.x[0]


def get_duration(coupon, ytm_bey, maturity_datelike=None, settle_datelike=None,
                 n_remaining_coupons=None, remaining_first_period=1.0, remaining_coupon_periods=None,
                 get_macaulay=False):
    """ Calculate duration (modified or Macaulay) from coupon, yield, and time to maturity
        NOTE: In order to derive the maturity, please supply one of the following three configurations:
              1) maturity_datelike and settle_datelike
              2) n_remaining_coupons and (optional) remaining_first_period
              3) remaining_coupon_periods
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param ytm_bey: bond equivalent yield to maturity percentage of bond, e.g. 2.858
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining coupons up to maturity of bond; set not None for configuration 2
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array; set not None for configuration 3
    :param get_macaulay: set True for Macaulay duration; default modified duration
    :return: modified or Macaulay (known as just "duration") duration in years
    """
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = get_remaining_coupon_periods(maturity_datelike, settle_datelike,
                                                                n_remaining_coupons, remaining_first_period)
    # Calculate cash flows and discount factors
    coupon_payment = coupon/2
    cash_flows = np.full_like(remaining_coupon_periods, coupon_payment)
    cash_flows[-1] += 100   # Face value delivered at maturity
    ytm_semiannual = ytm_bey/2 / 100
    discount_factors = 1 / (1 + ytm_semiannual)**remaining_coupon_periods
    # Macaulay duration is (cash flows*discount factors*period numbers)/(cash flows*discount factors) / periods in year
    cf_present_value_weighted_n_periods = (cash_flows * discount_factors * remaining_coupon_periods).sum()
    cf_present_value = (cash_flows * discount_factors).sum()
    duration_n_periods = cf_present_value_weighted_n_periods / cf_present_value
    macaulay_duration = duration_n_periods / 2  # 2 coupon periods in a year
    if get_macaulay:
        return macaulay_duration
    else:
        # Modified duration is Macaulay duration / (1 + yield/compounding frequency)
        modified_duration = macaulay_duration / (1 + ytm_semiannual)
        return modified_duration


def get_implied_repo_rate(coupon, bond_price, maturity_datelike, settle_datelike,
                          futures_price, conver_factor, delivery_datelike):
    """ Calculate implied repo rate from bond and futures specs
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param bond_price: clean price of bond
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param futures_price: price of futures contract
    :param conver_factor: conversion factor of futures contract
    :param delivery_datelike: last delivery date (NOT maturity date) of futures
    :return: implied repo rate in percent
    """
    maturity_date = datelike_to_timestamp(maturity_datelike)
    settle_date = datelike_to_timestamp(settle_datelike)
    delivery_date = datelike_to_timestamp(delivery_datelike)
    # Find coupon dates and related accruals
    coupon_payment = coupon/2
    settle_prev_coupon_date, settle_next_coupon_date, settle_days_in_period, settle_days_since_coupon = \
        get_coupon_status(maturity_date, settle_date)
    settle_elapsed_period = settle_days_since_coupon / settle_days_in_period
    settle_accrued_interest = settle_elapsed_period * coupon_payment
    if delivery_date > settle_next_coupon_date:
        # Special case that there is a coupon between settle date and futures delivery
        _, _, delivery_days_in_period, delivery_days_since_coupon = \
            get_coupon_status(maturity_date, delivery_date)
        delivery_elapsed_period = delivery_days_since_coupon / delivery_days_in_period
        special_coupon_payment = coupon_payment     # Yes special coupon
    else:
        delivery_elapsed_period = (delivery_date - settle_prev_coupon_date).days / settle_days_in_period
        special_coupon_payment = 0  # No special coupon
    delivery_accrued_interest = delivery_elapsed_period * coupon_payment
    # Calculate named factors needed for rate
    futures_gain = futures_price * conver_factor    # Immediate gain from short futures
    bond_cost = bond_price + settle_accrued_interest    # Immediate cost from long bond
    bond_eventual_gain = delivery_accrued_interest + special_coupon_payment     # Delivery date gain from long bond
    settle_elapsed_period_360 = (delivery_date - settle_date).days / 360
    special_coupon_elapsed_period_360 = (delivery_date - settle_next_coupon_date).days / 360    # Only for special case
    implied_repo_rate = \
        ((futures_gain - bond_cost + bond_eventual_gain) /
         (bond_cost*settle_elapsed_period_360 - special_coupon_payment*special_coupon_elapsed_period_360)) * 100
    return implied_repo_rate


###############################################################################

if __name__ == '__main__':
    print("\nExample 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)\n")
    true_ytm = 2.594
    print(f"True Yield to Maturity: {true_ytm}")
    calculated_price = get_price_from_yield(2.875, true_ytm, n_remaining_coupons=4, verbose=True)
    print(f"get_price_from_yield(2.875, true_ytm, n_remaining_coupons=4, verbose=True): {calculated_price}")
    calculated_ytm = get_yield_to_maturity(2.875, calculated_price, '2010-06-30', '2008-06-30')
    print(f"get_yield_to_maturity(2.875, calculated_price, '2010-06-30', '2008-06-30'): {calculated_ytm}")
    if np.isclose(true_ytm, calculated_ytm):
        print("PASS")
    else:
        print("****FAILED****")
    
    print("\nExample 2: 20 Non-Whole Coupon Periods\n")
    true_clean_price = 99.4375
    print(f"True Clean Price: {true_clean_price}")
    calculated_yield = get_yield_to_maturity(3.875, true_clean_price, '2018-05-15', '2008-07-11', is_clean_price=True)
    print(f"get_yield_to_maturity(3.875, true_clean_price, '2018-05-15', '2008-07-11', is_clean_price=True): "
          f"{calculated_yield}")
    calculated_clean_price = get_price_from_yield(3.875, calculated_yield,
                                                  n_remaining_coupons=20, remaining_first_period=1-57/184,
                                                  get_clean=True)
    print(f"get_price_from_yield(3.875, calculated_yield,\n"
          f"                     n_remaining_coupons=20, remaining_first_period=1-57/184,\n"
          f"                     get_clean=True): {calculated_clean_price}")
    if np.isclose(true_clean_price, calculated_clean_price):
        print("PASS")
    else:
        print("****FAILED****")
    print("On settlement date, we are 57 days into the 184 days of coupon period")
    true_dirty_price = true_clean_price + 57/184 * 3.875/2
    print(f"True Dirty Price: true_clean_price + 57/184 * 3.875/2 = {true_dirty_price}")
    calculated_dirty_price = get_price_from_yield(3.875, calculated_yield,
                                                  n_remaining_coupons=20, remaining_first_period=1-57/184)
    print(f"get_price_from_yield(3.875, calculated_yield,\n"
          f"                     n_remaining_coupons=20, remaining_first_period=1-57/184): {calculated_dirty_price}")
    if np.isclose(true_dirty_price, calculated_dirty_price):
        print("PASS")
    else:
        print("****FAILED****")

    print("\nExample 3: Duration\n")
    true_macaulay = 1.2215
    calculated_macaulay = get_duration(2.875, 402.278216, '2010-06-30', '2008-10-22', get_macaulay=True)
    print(f"get_duration(2.875, 402.278216, '2010-06-30', '2008-10-22', get_macaulay=True): {calculated_macaulay}")
    if np.isclose(true_macaulay, calculated_macaulay):
        print("PASS")
    else:
        print("****FAILED****")
    true_modified = 0.4055
    calculated_modified = get_duration(2.875, 402.388618, '2010-06-30', '2008-10-22')
    print(f"get_duration(2.875, 402.388618, '2010-06-30', '2008-10-22'): {calculated_modified}")
    if np.isclose(true_modified, calculated_modified):
        print("PASS")
    else:
        print("****FAILED****")

    print("\nExample 4: Implied Repo Rate\n")
    true_implied_repo_rate = -2.129
    test_coupon = 2.25
    test_bond_price = 103 + 22.75/32
    test_maturity_datelike = '2/15/27'
    test_settle_datelike = '1/22/20'
    test_futures_price = 129 + 16/32
    test_conver_factor = 0.7943
    test_delivery_datelike = '03/31/20'
    calculated_implied_repo_rate = \
        get_implied_repo_rate(test_coupon, test_bond_price, test_maturity_datelike, test_settle_datelike,
                              test_futures_price, test_conver_factor, test_delivery_datelike)
    print(f"get_implied_repo_rate({test_coupon}, {test_bond_price}, {test_maturity_datelike}, {test_settle_datelike},\n"
          f"                      {test_futures_price}, {test_conver_factor}, {test_delivery_datelike}): "
          f"{calculated_implied_repo_rate}")
    if np.isclose(true_implied_repo_rate, round(calculated_implied_repo_rate, 3)):
        print("PASS")
    else:
        print("****FAILED****")

""" Expected Output:
Example 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)

True Yield to Maturity: 2.594
Discounted Payment 1: 1.4190943463281245
Discounted Payment 2: 1.4009243574124846
Discounted Payment 3: 1.3829870158173336
Discounted Payment 4: 96.34123362044427
Calculated Dirty Price: 100.5442393400022
Calculated Clean Price: 100.5442393400022
get_price_from_yield(2.875, true_ytm, n_remaining_coupons=4, verbose=True): 100.5442393400022
get_yield_to_maturity(2.875, calculated_price, '2010-06-30', '2008-06-30'): 2.5940000000000043
PASS

Example 2: 20 Non-Whole Coupon Periods

True Clean Price: 99.4375
get_yield_to_maturity(3.875, true_clean_price, '2018-05-15', '2008-07-11', is_clean_price=True): 3.9439989648691136
get_price_from_yield(3.875, calculated_yield,
                     n_remaining_coupons=20, remaining_first_period=1-57/184,
                     get_clean=True): 99.43749999999979
PASS
On settlement date, we are 57 days into the 184 days of coupon period
True Dirty Price: true_clean_price + 57/184 * 3.875/2 = 100.03770380434783
get_price_from_yield(3.875, calculated_yield,
                     n_remaining_coupons=20, remaining_first_period=1-57/184): 100.03770380434761
PASS

Example 3: Duration

get_duration(2.875, 402.278216, '2010-06-30', '2008-10-22', get_macaulay=True): 1.2215000001576344
PASS
get_duration(2.875, 402.388618, '2010-06-30', '2008-10-22'): 0.40550000045334145
PASS

Example 4: Implied Repo Rate

get_implied_repo_rate(2.25, 103.7109375, 2/15/27, 1/22/20,
                      129.5, 0.7943, 03/31/20): -2.128949495660515
PASS
"""
