# Adapted from code by Anshul Palavajjhala

import datetime as dt
import pandas as pd
from cboe_exchange_holidays_v3 import get_cboe_holidays


###############################################################################
# Helper functions

# Recursively find the previous business day
# Useful because CME expirations/maturities are sometimes defined as a certain
# number of days from the last business day (e.g. treasury futures and options)
def get_prev_business_day(date):
    date = pd.to_datetime(date).tz_localize(None)   # Ensure date is object
    date -= dt.timedelta(days=1)  # Try one day prior
    # If day is weekday and not a weekday holiday, it's a business day;
    # otherwise try again (with prior day)
    if (date.weekday() in range(5)) and \
       (date not in get_cboe_holidays(date.year)):
        return date
    else:
        return get_prev_business_day(date)


# Find expiration/maturity date defined as "n business days before the last
# business day of the month" (e.g. treasury futures and options)
def n_before_last_bus_day(date_in_month, n):
    # Ensure date is timezone-naive object
    date_in_month = pd.to_datetime(date_in_month).tz_localize(None)
    # Set first day of next month as seed
    seed = (dt.datetime(date_in_month.year, date_in_month.month, 1)
            + pd.DateOffset(months=1))
    # Get to (n+1)th-to-last business day of original month, i.e. n days before
    # the last business day of the month
    for i in range(n+1):
        seed = get_prev_business_day(seed)
    return seed


# Return the next quarterly month using a clever calculus
def next_quarter_month(curr_month):
    return (curr_month // 3 % 4 + 1) * 3


###############################################################################
# Expiration date generators

# Given a date, return the expiration date of month, accounting for holidays,
# that is based on the third Friday of the month
# NOTE: return date could be before input date
def third_friday(start_date):
    start_date = pd.to_datetime(start_date).tz_localize(None)   # Ensure date is object
    # Set earliest day of 3rd week as seed
    exp_date_seed = dt.datetime(start_date.year, start_date.month, 15)
    # Get offset from Friday (4)
    day_diff = 4 - exp_date_seed.weekday()
    # Apply offset to get Friday of 3rd week
    if day_diff >= 0:
        exp_date = exp_date_seed + dt.timedelta(days=day_diff)
    else:
        exp_date = exp_date_seed + dt.timedelta(days=day_diff + 7)
    # If third Friday is an exchange holiday, actually return Thursday
    if exp_date in get_cboe_holidays(start_date.year):
        exp_date -= dt.timedelta(days=1)
    return exp_date


# Given a date, return the expiration date of month, accounting for holidays,
# that is based on the third Saturday of the month (used instead of third Friday
# for SPX options up until February 2015)
# NOTE: return date could be before input date
def third_saturday(start_date):
    start_date = pd.to_datetime(start_date).tz_localize(None)   # Ensure date is object
    # Set earliest day of 3rd week as seed
    exp_date_seed = dt.datetime(start_date.year, start_date.month, 15)
    # Get offset from Saturday (5)
    day_diff = 5 - exp_date_seed.weekday()
    # Apply offset to get Saturday of 3rd week
    if day_diff >= 0:
        exp_date = exp_date_seed + dt.timedelta(days=day_diff)
    else:
        exp_date = exp_date_seed + dt.timedelta(days=day_diff + 7)
    return exp_date


# Given a date, return the expiration date of month, accounting for holidays,
# that is based on the last Friday at least 2 business days from the last
# business day of the month (e.g. treasury futures and options)
# NOTE: return date could be before input date
def last_friday(start_date):
    start_date = pd.to_datetime(start_date).tz_localize(None)   # Ensure date is object
    # Get latest possible day - 2 business days before last business day
    exp_date_seed = n_before_last_bus_day(start_date, 2)
    # Get offset from Friday (4)
    day_diff = 4 - exp_date_seed.weekday()
    # Apply offset to get Friday of last applicable week
    if day_diff == 0:
        exp_date = exp_date_seed    # It is Friday
    elif day_diff > 0:
        exp_date = exp_date_seed + dt.timedelta(days=-7+day_diff)   # Go last week
    else:
        print("IMPOSSIBLE")
        return None
    # If last Friday is an exchange holiday, actually return Thursday
    if exp_date in get_cboe_holidays(start_date.year):
        exp_date -= dt.timedelta(days=1)
    return exp_date


###############################################################################

# Given a date and number of terms (n), return the nth expiration after date
# NOTE: if input date is on its month's expiration date, 1st term is next month
def expiry_by_terms(start_date, n_terms, expiry_by_date=third_friday):
    if n_terms <= 0:
        print("ERROR expiry_by_term: 0th expiration makes no sense")
        return None
    start_date = pd.to_datetime(start_date).tz_localize(None)   # Ensure date is object
    # Check if date is already past its month's expiration
    if start_date >= expiry_by_date(start_date):
        # Input date is after expiration
        offset = 1
    else:
        offset = 0
    # Find appropriate month for expiration
    exp_month_seed = (dt.datetime(start_date.year, start_date.month, 15)
                      + pd.DateOffset(months=n_terms+offset-1))
    return expiry_by_date(exp_month_seed)


# Given a date, return the next maturity of TY futures, accounting for holidays,
# that is based on 7 business days from the last business day of the next
# quarterly month
def next_ty_futures_maturity(curr_date):
    # Ensure date is timezone-naive object
    curr_date = pd.to_datetime(curr_date).tz_localize(None)
    # TY futures only exist for quarterly months; if it is currently a quarterly
    # month, check whether current date is past the month's expiration date
    if curr_date.month in [3, 6, 9, 12]:
        exp_date_candidate = n_before_last_bus_day(curr_date, 7)
        if curr_date >= exp_date_candidate:
            use_month = next_quarter_month(curr_date.month)
            exp_date = n_before_last_bus_day(curr_date.replace(month=use_month), 7)
        else:
            exp_date = exp_date_candidate
    else:
        use_month = next_quarter_month(curr_date.month)
        exp_date = n_before_last_bus_day(curr_date.replace(month=use_month), 7)
    return exp_date


###############################################################################

if __name__ == '__main__':
    print("expiry_by_terms on 2019-04-09, 3 terms:\n{}"
          .format(expiry_by_terms('2019-04-09', 3)))
    print("expiry_by_terms on 2019-12-09, 1 terms:\n{}"
          .format(expiry_by_terms('2019-12-09', 1, last_friday)))
    print("expiry_by_terms on 2019-12-27, 1 terms:\n{}"
          .format(expiry_by_terms('2019-12-27', 1, last_friday)))
    print("expiry_by_terms on 2016-03-09, 1 terms:\n{}"
          .format(expiry_by_terms('2016-03-09', 1, last_friday)))
    print("expiry_by_terms on 2016-02-09, 2 terms:\n{}"
          .format(expiry_by_terms('2016-02-09', 2, last_friday)))
    print("next_ty_futures_maturity on 2019-02-09:\n{}"
          .format(next_ty_futures_maturity('2019-02-09')))
    print("next_ty_futures_maturity on 2019-03-19:\n{}"
          .format(next_ty_futures_maturity('2019-03-19')))
    print("next_ty_futures_maturity on 2019-03-20:\n{}"
          .format(next_ty_futures_maturity('2019-03-20')))

""" Example Output:
expiry_by_terms on 2019-04-09, 3 terms:
2019-06-21 00:00:00
expiry_by_terms on 2019-12-09, 1 terms:
2019-12-27 00:00:00
expiry_by_terms on 2019-12-27, 1 terms:
2020-01-24 00:00:00
expiry_by_terms on 2016-03-09, 1 terms:
2016-03-24 00:00:00
expiry_by_terms on 2016-02-09, 2 terms:
2016-03-24 00:00:00
next_ty_futures_maturity on 2019-02-09:
2019-03-20 00:00:00
next_ty_futures_maturity on 2019-03-19:
2019-03-20 00:00:00
next_ty_futures_maturity on 2019-03-20:
2019-06-19 00:00:00
"""
