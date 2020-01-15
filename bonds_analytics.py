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


def get_price_from_yield(coupon, ytm_bey, maturity_datelike, settle_datelike,
                         n_remaining_coupons=None, remaining_first_period=1.0,
                         remaining_coupon_periods=None, get_clean=False, verbose=False):
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


def get_yield_to_maturity(coupon, price, maturity_datelike, settle_datelike,
                          n_remaining_coupons=None, remaining_first_period=1.0,
                          remaining_coupon_periods=None, is_clean_price=False):
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


def get_macaulay_duration(coupon, ytm_bey, maturity_datelike, settle_datelike,
                          n_remaining_coupons=None, remaining_first_period=1.0,
                          remaining_coupon_periods=None):
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
    return duration_n_periods / 2   # 2 coupon periods in a year


###############################################################################

if __name__ == '__main__':
    print("\nTest 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)\n")
    backed_out_price = get_price_from_yield(2.875, 2.594, None, None, 4, verbose=True)
    print(f"get_price_from_yield(2.875, 2.594, None, None, 4, verbose=True): {backed_out_price}")
    print(f"get_yield_to_maturity(2.875, 100.5442393400022, '2010-06-30', '2008-06-30'): "
          f"{get_yield_to_maturity(2.875, 100.5442393400022, '2010-06-30', '2008-06-30')}")
    
    print("\nTest 2: 20 Non-Whole Coupon Periods\n")
    print("Clean Price: 99.4375")
    calculated_yield = get_yield_to_maturity(3.875, 99.4375, '2018-05-15', '2008-07-11', is_clean_price=True)
    print(f"get_yield_to_maturity(3.875, 99.4375, '2018-05-15', '2008-07-11', is_clean_price=True): {calculated_yield}")
    print("On settlement date, we are 57 days into the 184 days of coupon period")
    print("Real Dirty Price: 99.4375 + 57/184 * 3.875/2 = 100.03770380434783")
    backed_out_dirty_price = get_price_from_yield(3.875, 3.9439989648691136, None, None, 20, 1-57/184)
    print(f"get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184): {backed_out_dirty_price}")
    backed_out_clean_price = get_price_from_yield(3.875, 3.9439989648691136, None, None, 20, 1-57/184, get_clean=True)
    print(f"get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184, get_clean=True): {backed_out_clean_price}")

""" Expected Output:
Test 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)

Discounted Payment 1: 1.4190943463281245
Discounted Payment 2: 1.4009243574124846
Discounted Payment 3: 1.3829870158173336
Discounted Payment 4: 96.34123362044427
Calculated Dirty Price: 100.5442393400022
Calculated Clean Price: 100.5442393400022
get_price_from_yield(2.875, 2.594, None, None, 4, verbose=True): 100.5442393400022
get_yield_to_maturity(2.875, 100.5442393400022, '2010-06-30', '2008-06-30'): 2.5940000000000043

Test 2: 20 Non-Whole Coupon Periods

Clean Price: 99.4375
get_yield_to_maturity(3.875, 99.4375, '2018-05-15', '2008-07-11', is_clean_price=True): 3.9439989648691136
On settlement date, we are 57 days into the 184 days of coupon period
Real Dirty Price: 99.4375 + 57/184 * 3.875/2 = 100.03770380434783
get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184): 100.03770380434761
get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184, get_clean=True): 99.43749999999979
"""
