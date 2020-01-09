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


def get_price_from_yield(coupon, ytm_bey, n_remaining_coupons, remaining_first_period=1,
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
    ytm_semiannual = ytm_bey / 2
    if remaining_coupon_periods is None:
        remaining_coupon_periods = remaining_first_period + np.arange(n_remaining_coupons)
    coupons_pv = (coupon/2) / (1 + ytm_semiannual/100)**remaining_coupon_periods
    face_pv = 100 / (1 + ytm_semiannual/100)**remaining_coupon_periods[-1]
    calc_dirty_price = coupons_pv.sum() + face_pv
    if verbose:
        for i, coupon_pv in enumerate(coupons_pv, 1):
            print(f"Coupon {i}: {coupon_pv}")
        print(f"Face: {face_pv}")
        print(f"Calculated Dirty Price: {calc_dirty_price}")
    return calc_dirty_price


def get_yield_to_maturity(coupon, maturity_datelike, price_clean, settle_datelike):
    """ Back out bond equivalent yield to maturity from bond price, bond specs, and settlement date
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param maturity_datelike: maturity date of bond
    :param price_clean: clean price of bond (quoted price)
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
