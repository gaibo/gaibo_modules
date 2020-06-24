import pandas as pd
import numpy as np
from os import listdir
from scipy.optimize import root
from cboe_exchange_holidays_v3 import datelike_to_timestamp, strip_to_date
from options_futures_expirations_v3 import BUSDAY_OFFSET, ONE_YEAR, \
    forward_6_months, change_year, is_end_of_month, n_before_month_last_day

NOTESBONDS_UNIVERSE_HISTORY_FILEDIR = 'P:/ProductDevelopment/Database/Production/Treasury_VIX/Files/'


def get_coupon_status(maturity_datelike, settle_datelike):
    """ Derive current semiannual coupon period details - start, end, number of days, elapsed days
    :param maturity_datelike: maturity date of bond used as basis for coupon schedule
    :param settle_datelike: settlement date of bond (business day after trade date) used for coupon accrual
    :return: (previous coupon date, next coupon date,
              number of days in current coupon period, number of days since last coupon)
    """
    maturity_date = strip_to_date(datelike_to_timestamp(maturity_datelike))
    settle_date = strip_to_date(datelike_to_timestamp(settle_datelike))
    year = settle_date.year
    maturity_date_6_months = forward_6_months(maturity_date)
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


def clean_to_dirty(price, coupon, maturity_datelike, settle_datelike, reverse=False):
    """ Convert bond price from clean to dirty (or the reverse)
    :param price: clean (quoted) price of bond, unless reverse is True
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param reverse: set True to input dirty price and convert to clean
    :return: dirty price of bond, unless reverse is True
    """
    _, _, days_in_period, days_since_coupon = get_coupon_status(maturity_datelike, settle_datelike)
    accrued_interest = days_since_coupon/days_in_period * coupon/2
    if reverse:
        clean_price = price - accrued_interest
        return clean_price
    else:
        dirty_price = price + accrued_interest
        return dirty_price


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
        maturity_date = strip_to_date(datelike_to_timestamp(maturity_datelike))
        prev_coupon_date, _, days_in_period, days_since_coupon = get_coupon_status(maturity_date, settle_datelike)
        n_remaining_coupons = round(
            (maturity_date - prev_coupon_date) / ONE_YEAR * 2)  # Number of semiannual coupons remaining
        remaining_first_period = 1 - days_since_coupon/days_in_period
    remaining_coupon_periods = remaining_first_period + np.arange(n_remaining_coupons)
    return remaining_coupon_periods


def get_price_from_yield(ytm_bey, coupon, maturity_datelike=None, settle_datelike=None,
                         n_remaining_coupons=None, remaining_first_period=1.0,
                         remaining_coupon_periods=None, remaining_payments=None,
                         get_dirty=False, verbose=False):
    """ Calculate price from yield to maturity for either semiannual coupon bond or generic cash flows
        NOTE: All cash flows can be reduced to:
              a) numbers of (semiannual for USA) periods forward at which payments arrive
              b) payment amounts that arrive at those points in time
              Combining a) and b) with yield, a (dirty in context of notes/bonds) price can be obtained.
              This function was originally designed for Treasury notes/bonds, where
              a) is derived from 1) settlement date and 2) maturity date and
              b) is derived from coupon.
              For ease of use, a) can alternatively be derived from 1) number of remaining coupons
              and 2) remaining (opposite of elapsed) first period.
              To expand (reduce is more accurate) this function to price general cash flows,
              a) can be directly given as remaining_coupon_periods and
              b) can be directly given as remaining_payments.
        NOTE: when pricing generic cash flows, there is no concept of clean price, so get_dirty does nothing
    :param ytm_bey: bond equivalent yield to maturity percentage of bond, e.g. 2.858
    :param coupon: coupon percentage of bond, e.g. 2.875; set None for generic cash flows
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining fixed coupons to maturity of bond
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array of upcoming periods-until-payments
    :param remaining_payments: numpy/pandas array of upcoming payment amounts
    :param get_dirty: set True to return dirty price rather than clean (quoted) price
    :param verbose: set True to print discounted cash flows
    :return: if pricing fixed-coupon bond, clean price of bond (unless get_dirty is True) with 100 as par
    """
    ytm_semiannual = ytm_bey/2 / 100    # Convert to non-percentage
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = get_remaining_coupon_periods(maturity_datelike, settle_datelike,
                                                                n_remaining_coupons, remaining_first_period)
    if remaining_payments is None:
        # Derive upcoming payments of coupon bond with face value 100
        if coupon is None:
            raise ValueError("coupon is None so function is receiving generic cash flows;\n"
                             "both remaining_coupon_periods and remaining_payments are expected.")
        coupon_payment = coupon/2
        remaining_payments = np.full_like(remaining_coupon_periods, coupon_payment)
        remaining_payments[-1] += 100  # Face value delivered at maturity
    # Calculate discount factors and price the cash flows
    discount_factors = 1 / (1 + ytm_semiannual)**remaining_coupon_periods
    discounted_cash_flows = remaining_payments * discount_factors
    calc_dirty_price = discounted_cash_flows.sum()
    if coupon is not None:
        # Fixed-coupon mode: can obtain a clean price
        elapsed_first_period = 1 - remaining_coupon_periods[0]
        coupon_payment = coupon/2
        accrued_interest = elapsed_first_period * coupon_payment
        calc_clean_price = calc_dirty_price - accrued_interest
    else:
        calc_clean_price = None
    if verbose:
        for i, discounted_payment in enumerate(discounted_cash_flows, 1):
            print(f"Discounted Payment {i}: {discounted_payment}")
        print(f"Calculated Dirty Price: {calc_dirty_price}")
        print(f"Calculated Clean Price: {calc_clean_price}")
    if coupon is None or get_dirty:
        return calc_dirty_price
    else:
        return calc_clean_price


def get_yield_to_maturity(price, coupon, maturity_datelike=None, settle_datelike=None,
                          n_remaining_coupons=None, remaining_first_period=1.0,
                          remaining_coupon_periods=None, remaining_payments=None,
                          is_dirty_price=False):
    """ Back out bond equivalent yield to maturity from price
        NOTE: please see NOTE of get_price_from_yield() for context on parameter names and usage
        NOTE: when pricing generic cash flows, there is no concept of clean price, so is_dirty_price does nothing
    :param price: clean (quoted) price of bond, unless is_dirty_price is True
    :param coupon: coupon percentage of bond, e.g. 2.875; set None for generic cash flows
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining fixed coupons to maturity of bond
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array of upcoming periods-until-payments
    :param remaining_payments: numpy/pandas array of upcoming payment amounts
    :param is_dirty_price: set True if input price is dirty price
    :return: BEY yield to maturity in percent
    """
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = get_remaining_coupon_periods(maturity_datelike, settle_datelike,
                                                                n_remaining_coupons, remaining_first_period)
    # Back out the yield
    solved_root = root(lambda ytm_bey:
                       get_price_from_yield(ytm_bey, coupon, None, None,
                                            remaining_coupon_periods=remaining_coupon_periods,
                                            remaining_payments=remaining_payments,
                                            get_dirty=is_dirty_price, verbose=False)
                       - price, x0=np.array(2))
    return solved_root.x[0]


def get_duration(ytm_bey, coupon, maturity_datelike=None, settle_datelike=None,
                 n_remaining_coupons=None, remaining_first_period=1.0,
                 remaining_coupon_periods=None, remaining_payments=None,
                 get_modified=False):
    """ Calculate duration (modified or Macaulay) from yield and cash flows
        NOTE: please see NOTE of get_price_from_yield() for context on parameter names and usage
    :param ytm_bey: bond equivalent yield to maturity percentage of bond, e.g. 2.858
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param n_remaining_coupons: number of remaining fixed coupons to maturity of bond
    :param remaining_first_period: fraction of the first upcoming coupon period still remaining
    :param remaining_coupon_periods: numpy/pandas array of upcoming periods-until-payments
    :param remaining_payments: numpy/pandas array of upcoming payment amounts
    :param get_modified: set True for modified duration instead of Macaulay
    :return: modified or Macaulay (known as just "duration") duration in years
    """
    ytm_semiannual = ytm_bey/2 / 100
    if remaining_coupon_periods is None:
        # Derive (potentially non-whole) discount rate periods since they are not given
        remaining_coupon_periods = get_remaining_coupon_periods(maturity_datelike, settle_datelike,
                                                                n_remaining_coupons, remaining_first_period)
    if remaining_payments is None:
        # Derive upcoming payments of coupon bond with face value 100
        if coupon is None:
            raise ValueError("coupon is None so function is receiving generic cash flows;\n"
                             "both remaining_coupon_periods and remaining_payments are expected.")
        coupon_payment = coupon/2
        remaining_payments = np.full_like(remaining_coupon_periods, coupon_payment)
        remaining_payments[-1] += 100  # Face value delivered at maturity
    # Calculate the discount factors
    discount_factors = 1 / (1 + ytm_semiannual)**remaining_coupon_periods
    # Macaulay duration is:
    # sum(payments*discount factors*period numbers)/sum(payments*discount factors) / periods in year
    cf_present_value_weighted_n_periods = (remaining_payments * discount_factors * remaining_coupon_periods).sum()
    cf_present_value = (remaining_payments * discount_factors).sum()
    duration_n_periods = cf_present_value_weighted_n_periods / cf_present_value
    macaulay_duration = duration_n_periods / 2  # 2 coupon periods in a year
    if get_modified:
        # Modified duration is Macaulay duration / (1 + yield/compounding frequency)
        modified_duration = macaulay_duration / (1 + ytm_semiannual)
        return modified_duration
    else:
        return macaulay_duration


def get_last_delivery_date(delivery_monthlike, tenor):
    """ Derive futures last delivery date from contract's named maturity month and tenor
    :param delivery_monthlike: delivery/maturity month of futures, e.g. for TYH0 Comdty, it's 2020-03
    :param tenor: 2, 3, 5, 10, 30, etc. to indicate 2-, 3-, 5-, 10-, 30-year Treasury futures
    :return: pd.Timestamp
    """
    month_last_busday = strip_to_date(n_before_month_last_day(delivery_monthlike, use_busdays=True))
    if tenor in [10, 30]:
        # Last trading day of month
        last_delivery_date = month_last_busday
    else:
        # 3rd trading day of next month
        last_delivery_date = month_last_busday + 3*BUSDAY_OFFSET
    return last_delivery_date


def get_implied_repo_rate(bond_price, coupon, maturity_datelike, settle_datelike,
                          futures_price, conver_factor, delivery_datelike, delivery_monthlike=None, tenor=None):
    """ Calculate implied repo rate from bond and futures specs
        NOTE: delivery_monthlike and tenor can be supplied together as an alternative to delivery_datelike
    :param bond_price: clean price of bond
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param maturity_datelike: maturity date of bond
    :param settle_datelike: settlement date of bond (business day after trade date)
    :param futures_price: price of futures contract
    :param conver_factor: conversion factor of futures contract
    :param delivery_datelike: last delivery date (NOT maturity date) of futures; set None to use month and tenor
    :param delivery_monthlike: delivery/maturity month of futures, e.g. for TYH0 Comdty, it's 2020-03
    :param tenor: 2, 3, 5, 10, 30, etc. to indicate 2-, 3-, 5-, 10-, 30-year Treasury futures
    :return: implied repo rate in percent
    """
    maturity_date = datelike_to_timestamp(maturity_datelike)
    settle_date = strip_to_date(datelike_to_timestamp(settle_datelike))
    if delivery_datelike is None:
        # Derive from maturity month and tenor
        delivery_date = get_last_delivery_date(delivery_monthlike, tenor)
    else:
        delivery_date = datelike_to_timestamp(delivery_datelike)
    if settle_date >= delivery_date:
        raise ValueError(f"Settle date of bond ({settle_date.strftime('%Y-%m-%d')}) must be prior\n"
                         f"to delivery date of futures ({delivery_date.strftime('%Y-%m-%d')})\n"
                         f"for implied repo rate to exist.")
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


def get_whole_year_month_day_difference(datelike_1, datelike_2):
    """ Calculate number of whole years, whole spare months, and spare days between two dates
        NOTE: an end of month date will be a whole number of months from another end of month date
    :param datelike_1: earlier date
    :param datelike_2: later date
    :return: numerical tuple - (whole_years, whole_months, whole_days)
    """
    date_1 = datelike_to_timestamp(datelike_1)
    date_2 = datelike_to_timestamp(datelike_2)
    if pd.isna(date_1) or pd.isna(date_2):
        return np.NaN, np.NaN, np.NaN   # Necessary because pd.DateOffset does not accept np.NaN months
    # Set flag if dates are reversed in order
    if date_2 < date_1:
        earlier_later_reversed = True
        date_1, date_2 = date_2, date_1     # Make date_1 before date_2 for uniform calculation
    else:
        earlier_later_reversed = False
    date_1_in_date_2_year = change_year(date_1, date_2.year)    # Solve issues with Feb 28/29 being the same day
    # Set flag if both dates are end of month so they are specifically treated as a whole number of months apart
    if is_end_of_month(date_1) and is_end_of_month(date_2):
        end_of_month_match = True
    else:
        end_of_month_match = False
    # Calculate whole years and whole months difference
    nominal_years = date_2.year - date_1.year
    if date_1_in_date_2_year.month == date_2.month:
        if date_1_in_date_2_year.day <= date_2.day or end_of_month_match:
            # Just over a whole year; no spare months
            whole_years = nominal_years
            whole_months = 0
        else:
            # Just under a whole year, i.e. one less than nominal years with 11 spare months
            whole_years = nominal_years - 1
            whole_months = 11
    elif date_1_in_date_2_year.month < date_2.month:
        # Over a whole year; spare months depends on whether day-of-month has passed
        whole_years = nominal_years
        nominal_months = date_2.month - date_1_in_date_2_year.month
        if date_1_in_date_2_year.day <= date_2.day or end_of_month_match:
            whole_months = nominal_months
        else:
            whole_months = nominal_months - 1
    else:
        # Under a whole year; spare months depends on whether day-of-month has passed
        whole_years = nominal_years - 1
        nominal_months = (date_2.month - date_1_in_date_2_year.month) % 12
        if date_1_in_date_2_year.day <= date_2.day or end_of_month_match:
            whole_months = nominal_months
        else:
            whole_months = nominal_months - 1
    # Calculate spare days difference
    if end_of_month_match:
        # Avoid trying to calculate day difference - if both are end of month, then no spare days
        whole_days = 0
    else:
        whole_year_month_forward_date = (change_year(date_1, date_1.year+whole_years)
                                         + pd.DateOffset(months=whole_months))
        whole_days = (date_2 - whole_year_month_forward_date).days
    # Account for date order reversal
    if earlier_later_reversed:
        return -whole_years, -whole_months, -whole_days
    else:
        return whole_years, whole_months, whole_days


def get_day_difference_30_360(datelike_1, datelike_2):
    """ Return difference in days between two dates, in 30/360 day count convention
        NOTE: this exists as a special function because Actual/Actual day count convention
              subtraction can be done natively with pd.Timestamps
    :param datelike_1: earlier date
    :param datelike_2: later date
    :return: number of days (negative if earlier and later are switched)
    """
    date_1 = datelike_to_timestamp(datelike_1)
    date_2 = datelike_to_timestamp(datelike_2)
    # Mark if dates are chronologically backwards
    if date_2 < date_1:
        earlier_later_reversed = True
        date_1, date_2 = date_2, date_1     # Make date_1 before date_2 for uniform calculation
    else:
        earlier_later_reversed = False
    # Get whole years and months difference, then figure out days in 30/360 convention
    n_years, n_months, _ = get_whole_year_month_day_difference(date_1, date_2)
    if date_2.day >= date_1.day:
        spare_days_30_360 = date_2.day - date_1.day
    else:
        spare_days_30_360 = 30-date_1.day + date_2.day  # Alternatively, use modulo 30
    total_days_30_360 = n_years*360 + n_months*30 + spare_days_30_360
    return -total_days_30_360 if earlier_later_reversed else total_days_30_360


def get_conversion_factor(coupon, maturity_datelike, delivery_monthlike, tenor, no_rounding=False):
    """ Calculate Treasury futures conversion factor - approximate decimal price
        at which $1 par of security would trade if it had a 6% yield to maturity
    :param coupon: coupon percentage of bond, e.g. 2.875
    :param maturity_datelike: maturity date of bond
    :param delivery_monthlike: delivery/maturity month of futures, e.g. for TYH0 Comdty, it's 2020-03
    :param tenor: 2, 3, 5, 10, 30, etc. to indicate 2-, 3-, 5-, 10-, 30-year Treasury futures
    :param no_rounding: set True to prevent final rounding to 4 decimal places
    :return: numerical conversion factor
    """
    coupon = coupon / 100
    delivery_month_first = strip_to_date(datelike_to_timestamp(delivery_monthlike)).replace(day=1)
    maturity_date = strip_to_date(datelike_to_timestamp(maturity_datelike))
    whole_years, whole_months, _ = get_whole_year_month_day_difference(delivery_month_first, maturity_date)
    # Officially defined calculation
    n = whole_years
    z = whole_months//3*3 if tenor in [10, 30] else whole_months  # Round down to quarter for 10-, 30-year
    v = z if z < 7 else (3 if tenor in [10, 30] else z-6)
    a = (1/1.03)**(v/6)
    b = coupon/2 * (6-v)/6
    c = (1/1.03)**(2*n) if z < 7 else (1/1.03)**(2*n+1)
    d = coupon/0.06 * (1-c)
    factor = a * (coupon/2 + c + d) - b
    if no_rounding:
        return factor
    else:
        return round(factor, 4)     # CME officially rounds it to 4 decimal places


def _get_cme_yearmonth_differences(earlier_dates, later_dates, tenor, return_full_df=False):
    """ Helper: Calculate CME-style year-month differences between two arrays of Timestamps
    :param earlier_dates: array of earlier Timestamps
    :param later_dates: array of later Timestamps
    :param tenor: 2, 3, 5, 10, 30, etc. to indicate 2-, 3-, 5-, 10-, 30-year Treasury futures
    :param return_full_df: set True to return full DataFrame with exact year, month, day
    :return: pd.Series of years plus whole month fractions, e.g. 15, 10.25, 11.1666
    """
    combined_df = pd.DataFrame({'earlier': earlier_dates, 'later': later_dates})
    ymd_tuples = \
        combined_df.apply(lambda row: get_whole_year_month_day_difference(row['earlier'], row['later']), axis=1)
    ymd_df = pd.DataFrame(ymd_tuples.tolist(), index=ymd_tuples.index, columns=['years', 'months', 'days'])
    if tenor in [10, 30]:
        ymd_df['months_rounded'] = ymd_df['months']//3*3
    else:
        ymd_df['months_rounded'] = ymd_df['months']
    ymd_df['CME_difference'] = ymd_df['years'] + ymd_df['months_rounded']/12
    if return_full_df:
        return ymd_df
    else:
        return ymd_df['CME_difference']


def load_notesbonds_universe_history(file_dir=NOTESBONDS_UNIVERSE_HISTORY_FILEDIR):
    """ Read Treasury Direct notes/bonds universe from disk and load it into a DataFrame
    :param file_dir: optional directory to search for data file (overrides default directory)
    :return: pd.DataFrame with relevant fields
    """
    latest_universe_file = sorted([f for f in listdir(file_dir)
                                   if f.endswith('_notesbonds_universe_history.csv')])[-1]
    fields = ['cusip', 'interestRate', 'maturityDate',
              'announcementDate', 'auctionDate', 'issueDate', 'originalIssueDate',
              'securityType', 'securityTerm', 'originalSecurityTerm',
              'callDate', 'interestPaymentFrequency']
    date_fields = ['maturityDate', 'announcementDate', 'auctionDate', 'issueDate',
                   'originalIssueDate', 'callDate']
    universe = pd.read_csv(f'{file_dir}{latest_universe_file}',
                           usecols=fields, parse_dates=date_fields)[fields]     # Enforce fields order
    print(latest_universe_file + " read.")
    return universe


def get_delivery_basket(delivery_monthlike, tenor, loaded_universe_history=None, as_of_datelike=None, verbose=False):
    """ Return delivery basket for given futures month and tenor
        NOTE: automatically looks up latest locally available notes/bonds universe file
        NOTE: if this function is to be used multiple times, please provide optional parameter loaded_universe_history
              with the source DataFrame loaded through load_notesbonds_universe_history for speed/efficiency purposes
    :param delivery_monthlike: delivery/maturity month of futures, e.g. for TYH0 Comdty, it's 2020-03
    :param tenor: 2, 3, 5, 10, 30, etc. to indicate 2-, 3-, 5-, 10-, 30-year Treasury futures
    :param loaded_universe_history: DataFrame loaded through load_notesbonds_universe_history
    :param as_of_datelike: historical date on which to deduce delivery basket; set None for present day
    :param verbose: set True to return year, month, day difference information in output DataFrame
    :return: pd.DataFrame of deliverable notes/bonds
    """
    # View latest reopenings of universe history ready in delivery month and announced by as-of date
    if as_of_datelike is not None:
        as_of_date = datelike_to_timestamp(as_of_datelike)
    else:
        as_of_date = pd.Timestamp('now')
    if loaded_universe_history is None:
        loaded_universe_history = load_notesbonds_universe_history()
    delivery_month_first = strip_to_date(datelike_to_timestamp(delivery_monthlike)).replace(day=1)
    last_delivery_date = get_last_delivery_date(delivery_month_first, tenor)
    available_universe = (loaded_universe_history[(loaded_universe_history['maturityDate'] >= delivery_month_first)
                                                  & (loaded_universe_history['issueDate'] <= last_delivery_date)
                                                  & (loaded_universe_history['announcementDate'] <= as_of_date)]
                          .drop_duplicates('cusip', keep='first').set_index('cusip'))
    # Throw out non-semiannual notes/bonds since Treasury futures do not deliver them
    semiannuals = available_universe[available_universe['interestPaymentFrequency'] == 'Semi-Annual'].copy()
    # Calculate 1) "original term to maturity (from latest note/bond re-issue)"
    #           2) "remaining term to maturity (from futures delivery month)" ("to first call" if callable)
    otm_df = _get_cme_yearmonth_differences(semiannuals['issueDate'], semiannuals['maturityDate'],
                                            tenor, return_full_df=True)
    otm_fields = ['OTMYears', 'OTMMonths', 'OTMDays', 'OTMMonthsRounded', 'originalTermToMaturity']
    semiannuals[otm_fields] = otm_df
    delivery_month_series = pd.Series(delivery_month_first, index=semiannuals.index)
    rtm_df = _get_cme_yearmonth_differences(delivery_month_series, semiannuals['maturityDate'],
                                            tenor, return_full_df=True)
    rtfc_df = _get_cme_yearmonth_differences(delivery_month_series, semiannuals['callDate'],
                                             tenor, return_full_df=True)
    callable_index = semiannuals['callDate'].notna()
    rtm_df.loc[callable_index] = rtfc_df.loc[callable_index]
    rtm_fields = ['RTMYears', 'RTMMonths', 'RTMDays', 'RTMMonthsRounded', 'remainingTermToMaturity']
    semiannuals[rtm_fields] = rtm_df
    semiannuals = semiannuals.sort_values(['RTMYears', 'RTMMonths', 'RTMDays'])  # Sort by time to maturity
    if verbose:
        semiannuals = semiannuals.drop(['interestPaymentFrequency', 'callDate'], axis=1)
    else:
        semiannuals = semiannuals.drop(otm_fields[:4]+rtm_fields[:4]+['interestPaymentFrequency', 'callDate'], axis=1)
    # Use the two fields to determine subset that is deliverable for tenor
    if tenor == 2:
        basket = semiannuals[(semiannuals['originalTermToMaturity'] <= (5+3/12))
                             & (semiannuals['remainingTermToMaturity'] >= (1+9/12))
                             & (semiannuals['remainingTermToMaturity'] <= 2)].copy()
    elif tenor == 5:
        basket = semiannuals[(semiannuals['originalTermToMaturity'] <= (5+3/12))
                             & (semiannuals['remainingTermToMaturity'] >= (4+2/12))].copy()
    elif tenor == 10:
        basket = semiannuals[(semiannuals['originalTermToMaturity'] <= 10)
                             & (semiannuals['remainingTermToMaturity'] >= (6+6/12))].copy()
    elif tenor == 30:
        basket = semiannuals[(semiannuals['remainingTermToMaturity'] >= 15)
                             & (semiannuals['remainingTermToMaturity'] < 25)].copy()
    else:
        raise ValueError(f"Tenor of '{tenor}' is currently unsupported.")
    # Calculate futures conversion factor to help with eventually calculating implied repo rate/cheapest-to-deliver
    basket['conversionFactor'] = \
        basket.apply(lambda row: get_conversion_factor(row['interestRate'], row['maturityDate'],
                                                       delivery_month_first, tenor), axis=1)
    return basket


###############################################################################

if __name__ == '__main__':
    print("\nExample 1: 4 Whole Coupon Periods (Dirty Price = Clean Price)\n")
    true_ytm = 2.594
    print(f"True Yield to Maturity: {true_ytm}")
    calculated_price = get_price_from_yield(true_ytm, 2.875, n_remaining_coupons=4, verbose=True)
    print(f"get_price_from_yield(true_ytm, 2.875, n_remaining_coupons=4, verbose=True):\n{calculated_price}")
    calculated_ytm = get_yield_to_maturity(calculated_price, 2.875, '2010-06-30', '2008-06-30')
    print(f"get_yield_to_maturity(calculated_price, 2.875, '2010-06-30', '2008-06-30'):\n{calculated_ytm}")
    if np.isclose(true_ytm, calculated_ytm):
        print("PASS")
    else:
        print("****FAILED****")

    print("\nExample 2: 20 Non-Whole Coupon Periods\n")
    true_clean_price = 99.4375
    print(f"True Clean Price: {true_clean_price}")
    calculated_yield = get_yield_to_maturity(true_clean_price, 3.875, '2018-05-15', '2008-07-11', is_dirty_price=False)
    print(f"get_yield_to_maturity(true_clean_price, 3.875, '2018-05-15', '2008-07-11', is_dirty_price=False):\n"
          f"{calculated_yield}")
    calculated_clean_price = get_price_from_yield(calculated_yield, 3.875, n_remaining_coupons=20,
                                                  remaining_first_period=1-57/184, get_dirty=False)
    print(f"get_price_from_yield(calculated_yield, 3.875, n_remaining_coupons=20,\n"
          f"                     remaining_first_period=1-57/184, get_dirty=False):\n{calculated_clean_price}")
    if np.isclose(true_clean_price, calculated_clean_price):
        print("PASS")
    else:
        print("****FAILED****")
    print("On settlement date, we are 57 days into the 184 days of coupon period")
    true_dirty_price = true_clean_price + 57/184 * 3.875/2
    print(f"True Dirty Price: true_clean_price + 57/184 * 3.875/2 = {true_dirty_price}")
    calculated_converted_dirty_price = clean_to_dirty(true_clean_price, 3.875, '2018-05-15', '2008-07-11')
    print(f"clean_to_dirty(true_clean_price, 3.875, '2018-05-15', '2008-07-11'):\n{calculated_converted_dirty_price}")
    if np.isclose(true_dirty_price, calculated_converted_dirty_price):
        print("PASS")
    else:
        print("****FAILED****")
    calculated_converted_clean_price = clean_to_dirty(true_dirty_price, 3.875, '2018-05-15', '2008-07-11', reverse=True)
    print(f"clean_to_dirty(true_dirty_price, 3.875, '2018-05-15', '2008-07-11', reverse=True):\n"
          f"{calculated_converted_dirty_price}")
    if np.isclose(true_clean_price, calculated_converted_clean_price):
        print("PASS")
    else:
        print("****FAILED****")
    calculated_dirty_price = get_price_from_yield(calculated_yield, 3.875, n_remaining_coupons=20,
                                                  remaining_first_period=1-57/184, get_dirty=True)
    print(f"get_price_from_yield(calculated_yield, 3.875, n_remaining_coupons=20,\n"
          f"                     remaining_first_period=1-57/184, get_dirty=True):\n{calculated_dirty_price}")
    if np.isclose(true_dirty_price, calculated_dirty_price):
        print("PASS")
    else:
        print("****FAILED****")

    print("\nExample 3: Duration\n")
    true_macaulay = 1.2215
    calculated_macaulay = get_duration(402.278216, 2.875, '2010-06-30', '2008-10-22')
    print(f"get_duration(402.278216, 2.875, '2010-06-30', '2008-10-22'):\n{calculated_macaulay}")
    if np.isclose(true_macaulay, calculated_macaulay):
        print("PASS")
    else:
        print("****FAILED****")
    true_modified = 0.4055
    calculated_modified = get_duration(402.388618, 2.875, '2010-06-30', '2008-10-22', get_modified=True)
    print(f"get_duration(402.388618, 2.875, '2010-06-30', '2008-10-22', get_modified=True):\n{calculated_modified}")
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
    test_delivery_monthlike = '03/01/20'    # Delivery date is 03/31/20, but we'll derive that
    test_tenor = 10
    calculated_implied_repo_rate = \
        get_implied_repo_rate(test_bond_price, test_coupon, test_maturity_datelike, test_settle_datelike,
                              test_futures_price, test_conver_factor, None, test_delivery_monthlike, test_tenor)
    print(f"get_implied_repo_rate({test_bond_price}, {test_coupon}, {test_maturity_datelike}, {test_settle_datelike}, "
          f"{test_futures_price}, {test_conver_factor}, None, {test_delivery_monthlike}, {test_tenor}):\n"
          f"{calculated_implied_repo_rate}")
    if np.isclose(true_implied_repo_rate, round(calculated_implied_repo_rate, 3)):
        print("PASS")
    else:
        print("****FAILED****")

    print("\nExample 5: Whole Year, Month, Day Difference\n")
    print("First day of December 2008 delivery month to October 31, 2010 is {0} year(s), {1} months, {2} days."
          .format(*get_whole_year_month_day_difference('2008-12-01', '2010-10-31')))
    print("First day of March 2009 delivery month to January 15, 2012 is {0} year(s), {1} months, {2} days."
          .format(*get_whole_year_month_day_difference('2009-03-01', '2012-01-15')))
    print("First day of December 2008 delivery month to October 31, 2013 is {0} year(s), {1} months, {2} days."
          .format(*get_whole_year_month_day_difference('2008-12-01', '2013-10-31')))
    print("First day of December 2008 delivery month to November 15, 2018 is {0} year(s), {1} months, {2} days."
          .format(*get_whole_year_month_day_difference('2008-12-01', '2018-11-15')))
    print("First day of December 2008 delivery month to May 15, 2038 is {0} year(s), {1} months, {2} days."
          .format(*get_whole_year_month_day_difference('2008-12-01', '2038-05-15')))
    if (get_whole_year_month_day_difference('2008-12-01', '2010-10-31') == (1, 10, 30)
            and get_whole_year_month_day_difference('2009-03-01', '2012-01-15') == (2, 10, 14)
            and get_whole_year_month_day_difference('2008-12-01', '2013-10-31') == (4, 10, 30)
            and get_whole_year_month_day_difference('2008-12-01', '2018-11-15') == (9, 11, 14)
            and get_whole_year_month_day_difference('2008-12-01', '2038-05-15') == (29, 5, 14)
            and get_whole_year_month_day_difference('2008-02-29', '2038-02-28') == (30, 0, 0)
            and get_whole_year_month_day_difference('2007-03-31', '2047-04-30') == (40, 1, 0)):
        print("PASS")
    else:
        print("****FAILED****")

    print("\nExample 6: Conversion Factors\n")
    print("The following are the 5 examples provided on the CME website for calculating "
          "Treasury futures conversion factors:")
    true_cfs = (0.922939, 0.874675, 0.865330, 0.835651, 0.794274)
    test_parameters = ((1.5, '2010-10-31', '2008-12', 2),
                       (1.125, '2012-01-15', '2009-3', 3),
                       (2.75, '2013-10-31', '2008-12', 5),
                       (3.75, '2018-11-15', '2008-12', 10),
                       (4.5, '2038-05-15', '2008-12', 30))
    for true_cf, (test_coupon, test_maturity, test_delivery_month, test_tenor) in zip(true_cfs, test_parameters):
        result = get_conversion_factor(test_coupon, test_maturity, test_delivery_month, test_tenor, no_rounding=True)
        print(f"{test_tenor}-year:\n"
              f"    {test_delivery_month} futures delivering {test_coupon}s of {test_maturity}:\n"
              f"    {result}")
        if np.isclose(true_cf, result):
            print("PASS")
        else:
            print("****FAILED****")

    print("\nExample 7: Delivery Baskets\n")
    delivery_baskets_df = pd.read_csv('P:/PrdDevSharedDB/Treasury VIX/TYVIX Basis Point Vol Documentation/Deliverables/'
                                      'delivery_baskets_2020HMU_2020-01-09.csv')
    test_universe_history = load_notesbonds_universe_history()
    for test_tenor in [('TU', 2), ('FV', 5), ('TY', 10), ('US', 30)]:
        for test_month in [('H', '03'), ('M', '06'), ('U', '09')]:
            for test_year in [('0', '2020')]:
                true_basket = set(delivery_baskets_df[test_tenor[0]+test_month[0]+test_year[0]].dropna())
                calculated_basket = set(get_delivery_basket(test_year[1]+'-'+test_month[1], test_tenor[1],
                                                            test_universe_history, '2020-01-09').index)
                print(f'Test: {test_tenor[0]+test_month[0]+test_year[0]}')
                if calculated_basket == true_basket:
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
get_price_from_yield(2.875, true_ytm, n_remaining_coupons=4, verbose=True):
100.5442393400022
get_yield_to_maturity(2.875, calculated_price, '2010-06-30', '2008-06-30'):
2.5940000000000043
PASS

Example 2: 20 Non-Whole Coupon Periods

True Clean Price: 99.4375
get_yield_to_maturity(3.875, true_clean_price, '2018-05-15', '2008-07-11', is_dirty_price=False):
3.9439989648691136
get_price_from_yield(3.875, calculated_yield, n_remaining_coupons=20,
                     remaining_first_period=1-57/184, get_dirty=False):
99.43749999999979
PASS
On settlement date, we are 57 days into the 184 days of coupon period
True Dirty Price: true_clean_price + 57/184 * 3.875/2 = 100.03770380434783
clean_to_dirty(true_clean_price, 3.875, '2018-05-15', '2008-07-11'):
100.03770380434783
PASS
clean_to_dirty(true_dirty_price, 3.875, '2018-05-15', '2008-07-11', reverse=True):
100.03770380434783
PASS
get_price_from_yield(3.875, calculated_yield, n_remaining_coupons=20,
                     remaining_first_period=1-57/184, get_dirty=True):
100.03770380434761
PASS

Example 3: Duration

get_duration(2.875, 402.278216, '2010-06-30', '2008-10-22', get_macaulay=True):
1.2215000001576344
PASS
get_duration(2.875, 402.388618, '2010-06-30', '2008-10-22'):
0.40550000045334145
PASS

Example 4: Implied Repo Rate

get_implied_repo_rate(2.25, 103.7109375, 2/15/27, 1/22/20, 129.5, 0.7943, None, 03/01/20, 10):
-2.128949495660515
PASS

Example 5: Whole Year, Month, Day Difference

First day of December 2008 delivery month to October 31, 2010 is 1 year(s), 10 months, 30 days.
First day of March 2009 delivery month to January 15, 2012 is 2 year(s), 10 months, 14 days.
First day of December 2008 delivery month to October 31, 2013 is 4 year(s), 10 months, 30 days.
First day of December 2008 delivery month to November 15, 2018 is 9 year(s), 11 months, 14 days.
First day of December 2008 delivery month to May 15, 2038 is 29 year(s), 5 months, 14 days.
PASS

Example 6: Conversion Factors

The following are the 5 examples provided on the CME website for calculating Treasury futures conversion factors:
2-year:
    2008-12 futures delivering 1.5s of 2010-10-31:
    0.9229387996542004
PASS
3-year:
    2009-3 futures delivering 1.125s of 2012-01-15:
    0.8746751408285686
PASS
5-year:
    2008-12 futures delivering 2.75s of 2013-10-31:
    0.8653299817325179
PASS
10-year:
    2008-12 futures delivering 3.75s of 2018-11-15:
    0.8356505424979301
PASS
30-year:
    2008-12 futures delivering 4.5s of 2038-05-15:
    0.7942738875657215
PASS

Example 7: Delivery Baskets

2020-02-05_notesbonds_universe_history.csv read.
Test: TUH0
PASS
Test: TUM0
PASS
Test: TUU0
PASS
Test: FVH0
PASS
Test: FVM0
PASS
Test: FVU0
PASS
Test: TYH0
PASS
Test: TYM0
PASS
Test: TYU0
PASS
Test: USH0
PASS
Test: USM0
PASS
Test: USU0
PASS
"""
