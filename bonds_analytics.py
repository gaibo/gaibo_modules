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


def get_price_from_yield(coupon, ytm_bey, n_remaining_coupons, remaining_first_period=1.0,
                         remaining_coupon_periods=None, verbose=True):
    """ Calculate dirty price of semiannual coupon bond
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param ytm_bey: bond equivalent yield to maturity percentage of bond, e.g. 2.858
    :param n_remaining_coupons: number of remaining coupons up to maturity of bond
    :param remaining_first_period: proportion of the current coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array; if not None, replaces function of previous two fields
    :param verbose: set True to print discounted cash flows
    :return: dirty price of bond with 100 as par
    """
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = remaining_first_period + np.arange(n_remaining_coupons)
    ytm_semiannual = ytm_bey/2 / 100    # Convert to non-percentage
    coupons_pv = (coupon/2) / (1 + ytm_semiannual)**remaining_coupon_periods
    face_pv = 100 / (1 + ytm_semiannual)**remaining_coupon_periods[-1]
    calc_dirty_price = coupons_pv.sum() + face_pv
    if verbose:
        for i, coupon_pv in enumerate(coupons_pv, 1):
            print(f"Coupon {i}: {coupon_pv}")
        print(f"Face: {face_pv}")
        print(f"Calculated Dirty Price: {calc_dirty_price}")
    return calc_dirty_price


def get_yield_to_maturity(coupon, price_clean, maturity_datelike, settle_datelike):
    """ Back out bond equivalent yield to maturity from bond price, bond specs, and settlement date
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param price_clean: clean price of bond (quoted price)
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :return: BEY yield to maturity in percent
    """
    maturity_date = datelike_to_timestamp(maturity_datelike)
    settle_date = datelike_to_timestamp(settle_datelike)
    # Get coupon period details
    prev_coupon_date, _, days_in_period, days_since_coupon = get_coupon_status(maturity_date, settle_date)
    # Calculate dirty price of bond
    elapsed_period = days_since_coupon/days_in_period
    accrued_interest = coupon/2 * elapsed_period
    price_dirty = price_clean + accrued_interest
    # Calculate potentially non-whole discount rate periods
    remaining_period = 1 - elapsed_period
    n_remaining_coupons = round((maturity_date-prev_coupon_date)/ONE_YEAR * 2)  # Number of semiannual coupons remaining
    remaining_coupon_periods = remaining_period + np.arange(n_remaining_coupons)
    # Back out the yield
    solved_root = root(lambda ytm_bey:
                       get_price_from_yield(coupon, ytm_bey, None,
                                            remaining_coupon_periods=remaining_coupon_periods, verbose=False)
                       - price_dirty, x0=np.array(2))
    return solved_root.x[0]


# def get_macaulay_duration(coupon, ytm_bey, maturity_datelike, settle_datelike,
#                           n_remaining_coupons=None, remaining_first_period=1):
#     ytm_semiannual = ytm_bey/2 / 100
#     if n_remaining_coupons is not None:
#         # Get (potentially non-whole) discount rate periods without settlement and maturity
#         remaining_coupon_periods = remaining_first_period + np.arange(n_remaining_coupons)
#     else:
#         maturity_date = datelike_to_timestamp(maturity_datelike)
#         settle_date = datelike_to_timestamp(settle_datelike)
#         prev_coupon_date, _, days_in_period, days_since_coupon = get_coupon_status(maturity_date, settle_date)
#         remaining_period = 1 - days_since_coupon/days_in_period
#         n_remaining_coupons = round(
#             (maturity_date - prev_coupon_date) / ONE_YEAR * 2)  # Number of semiannual coupons remaining
#         remaining_coupon_periods = remaining_period + np.arange(n_remaining_coupons)
#     n = remaining_coupon_periods[-1]
#     r = coupon/2 / 100
#     i = ytm_semiannual
#     n_periods_duration = (1+i)/i - ((1+i) + n*(r-i)) / (r*((1+i)**n - 1) + i)
#     return n_periods_duration / 2


def get_macaulay_duration(coupon, ytm_bey, maturity_datelike, settle_datelike,
                          n_remaining_coupons=None, remaining_first_period=1, remaining_coupon_periods=None):
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        if n_remaining_coupons is not None:
            # Use number of remaining coupons and remaining first period
            remaining_coupon_periods = remaining_first_period + np.arange(n_remaining_coupons)
        else:
            # Use settlement and maturity
            maturity_date = datelike_to_timestamp(maturity_datelike)
            settle_date = datelike_to_timestamp(settle_datelike)
            prev_coupon_date, _, days_in_period, days_since_coupon = get_coupon_status(maturity_date, settle_date)
            remaining_period = 1 - days_since_coupon/days_in_period
            n_remaining_coupons = round(
                (maturity_date - prev_coupon_date) / ONE_YEAR * 2)  # Number of semiannual coupons remaining
            remaining_coupon_periods = remaining_period + np.arange(n_remaining_coupons)
    # Calculate cash flows and discount factors
    cash_flows = np.full(n_remaining_coupons, coupon/2)
    cash_flows[-1] += 100   # Face value delivered at maturity
    ytm_semiannual = ytm_bey/2 / 100
    discount_factors = 1 / (1+ytm_semiannual)**np.arange(1, n_remaining_coupons+1)
    # Macaulay duration is (cash flows*discount factors*period numbers)/(cash flows*discount factors) / periods in year
    cf_present_value_weighted_n_periods = (cash_flows*discount_factors*remaining_coupon_periods).sum()
    cf_present_value = (cash_flows*discount_factors).sum()
    duration_n_periods = cf_present_value_weighted_n_periods / cf_present_value
    return duration_n_periods / 2   # 2 coupon periods in a year


###############################################################################

if __name__ == '__main__':
    print("\nTest 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)\n")
    print(f"get_price_from_yield(2.875, 2.594, 4): {get_price_from_yield(2.875, 2.594, 4)}")
    print(f"get_yield_to_maturity(2.875, 100.5442393400022, '2010-06-30', '2008-06-30'): "
          f"{get_yield_to_maturity(2.875, 100.5442393400022, '2010-06-30', '2008-06-30')}")
    
    print("\nTest 2: 20 Non-Whole Coupon Periods\n")
    print("Clean Price: 99.4375")
    calculated_yield = get_yield_to_maturity(3.875, 99.4375, '2018-05-15', '2008-07-11')
    print(f"get_yield_to_maturity(3.875, 99.4375, '2018-05-15', '2008-07-11'): {calculated_yield}")
    print("On settlement date, we are 57 days into the 184 days of coupon period")
    print("Real Dirty Price: 99.4375 + 57/184 * 3.875/2 = 100.03770380434783")
    backed_out_dirty_price = get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184)
    print(f"get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184): {backed_out_dirty_price}")

""" Expected Output:
Test 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)
Coupon 1: 1.4190943463281243
Coupon 2: 1.4009243574124846
Coupon 3: 1.3829870158173336
Coupon 4: 1.3652793427419705
Face: 94.9759542777023
Calculated Dirty Price: 100.5442393400022
get_price_from_yield(2.875, 2.594, 4): 100.5442393400022
get_yield_to_maturity(2.875, 100.5442393400022, '2010-06-30', '2008-06-30'): 2.594000000000004
Test 2: 20 Non-Whole Coupon Periods
Clean Price: 99.4375
get_yield_to_maturity(3.875, 99.4375, '2018-05-15', '2008-07-11'): 3.943998964869112
On settlement date, we are 57 days into the 184 days of coupon period
Real Dirty Price: 99.4375 + 57/184 * 3.875/2 = 100.03770380434783
Coupon 1: 1.9115603877091476
Coupon 2: 1.87459341526242
Coupon 3: 1.8383413336769305
Coupon 4: 1.8027903179378162
Coupon 5: 1.7679268103871597
Coupon 6: 1.7337375155536676
Coupon 7: 1.7002093950823398
Coupon 8: 1.6673296627621916
Coupon 9: 1.63508577965013
Coupon 10: 1.6034654492891312
Coupon 11: 1.5724566130188906
Coupon 12: 1.5420474453771578
Coupon 13: 1.5122263495900037
Coupon 14: 1.4829819531493018
Coupon 15: 1.4543031034757306
Coupon 16: 1.4261788636656527
Coupon 17: 1.3985985083202401
Coupon 18: 1.3715515194552592
Coupon 19: 1.3450275824899551
Coupon 20: 1.3190165823135067
Face: 68.07827521618098
Calculated Dirty Price: 100.03770380434761
get_price_from_yield(3.875, 3.943998964869112, 20, 1-57/184): 100.03770380434761
"""
