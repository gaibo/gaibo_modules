import pandas as pd
from cboe_exchange_holidays_v3 import CboeTradingCalendar

CBOE_TRADING_CALENDAR = CboeTradingCalendar()
BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=CBOE_TRADING_CALENDAR)
DAY_NAME_TO_WEEKDAY_NUMBER_DICT = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
                                   'Friday': 4, 'Saturday': 5, 'Sunday': 6}


###############################################################################
# Helper functions

# Return the previous business day using CustomBusinessDay with Cboe trading calendar
# Useful because CME expirations/maturities are sometimes defined as a certain
# number of days from the last business day (e.g. Treasury futures and options)
def get_prev_business_day(date):
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)
    return date - BUSDAY_OFFSET


# Find expiration/maturity date defined as "n business days before the last
# business day of the month" (e.g. treasury futures and options)
def n_before_last_bus_day(date_in_month, n):
    if not isinstance(date_in_month, pd.Timestamp):
        date_in_month = pd.to_datetime(date_in_month)
    next_month_first = date_in_month.replace(day=1) + pd.DateOffset(months=1)
    this_month_last_bd = next_month_first - BUSDAY_OFFSET
    return this_month_last_bd - n*BUSDAY_OFFSET


# Return the next quarterly month using a clever calculus
# NOTE: default is [3, 3, 6, 6, 6, 9, 9, 9, 12, 12, 12, 3], i.e. quarterly months return
#       the next quarterly month; to return itself, set quarter_return_self to True
def next_quarter_month(curr_month, quarter_return_self=False):
    if quarter_return_self:
        return (((curr_month - 1) // 3) + 1) * 3
    else:
        return (curr_month // 3 % 4 + 1) * 3


# Return the date of the next day-of-week specified; see DAY_NAME_TO_WEEKDAY_NUMBER_DICT
# for day-of-week conversion
# NOTE: default return one week forward if current weekday is the desired day-of-week;
#       to return current date, set weekday_return_self to True
def next_weekday(date, weekday_number, weekday_return_self=False):
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)
    days_ahead = weekday_number - date.weekday()
    # Account for date already being past this week's desired day-of-week
    if weekday_return_self:
        days_ahead = days_ahead if days_ahead >= 0 else days_ahead + 7
    else:
        days_ahead = days_ahead if days_ahead > 0 else days_ahead + 7
    return date + pd.DateOffset(days=days_ahead)


# Return the date of the previous day-of-week specified; see DAY_NAME_TO_WEEKDAY_NUMBER_DICT
# for day-of-week conversion
# NOTE: default return one week back if current weekday is the desired day-of-week;
#       to return current date, set weekday_return_self to True
def prev_weekday(date, weekday_number, weekday_return_self=False):
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)
    days_behind = date.weekday() - weekday_number
    # Account for date already being before this week's desired day-of-week
    if weekday_return_self:
        days_behind = days_behind if days_behind >= 0 else days_behind + 7
    else:
        days_behind = days_behind if days_behind > 0 else days_behind + 7
    return date - pd.DateOffset(days=days_behind)


###############################################################################
# Expiration date functions

# Given a date, return the expiration date of month, accounting for holidays,
# that is based on the third Friday of the month
# NOTE: return date could be before input date
def third_friday(date_in_month):
    if not isinstance(date_in_month, pd.Timestamp):
        date_in_month = pd.to_datetime(date_in_month)
    earliest_third_week_day = date_in_month.replace(day=15)     # 15th is start of third week
    third_week_friday = next_weekday(earliest_third_week_day, 4, weekday_return_self=True)
    # If third Friday is an exchange holiday, return business day before
    return third_week_friday + BUSDAY_OFFSET - BUSDAY_OFFSET


# Given a date, return the expiration date of month, accounting for holidays,
# that is based on the third Saturday of the month (used instead of third Friday
# for SPX options up until February 2015)
# NOTE: return date could be before input date
def third_saturday(date_in_month):
    if not isinstance(date_in_month, pd.Timestamp):
        date_in_month = pd.to_datetime(date_in_month)
    earliest_third_week_day = date_in_month.replace(day=15)     # 15th is start of third week
    third_week_saturday = next_weekday(earliest_third_week_day, 5, weekday_return_self=True)
    # No issue of third Saturday falling on exchange holiday, I think
    return third_week_saturday


# Given a date, return the expiration date of month, accounting for holidays,
# that is based on the last Friday at least 2 business days from the last
# business day of the month (e.g. treasury futures and options)
# NOTE: return date could be before input date
def last_friday(date_in_month):
    latest_applicable_day = n_before_last_bus_day(date_in_month, 2)
    latest_applicable_friday = prev_weekday(latest_applicable_day, 4, weekday_return_self=True)
    # If last Friday is an exchange holiday, return business day before
    return latest_applicable_friday + BUSDAY_OFFSET - BUSDAY_OFFSET


###############################################################################

# Given a date, monthly expiry function (returns expiry date of month given day in month),
# and number of terms, return the designated expiry
# NOTE: if date given is the expiration date, it will be returned as the "next" expiry
#       since expiration would technically happen at the end of that day
# NOTE: set curr_month_as_first_term to True to ensure that expiry for given date's month
#       will be given as first term, even if date is past month's expiration date
def next_expiry(date_in_month, expiry_func=third_friday, n_terms=1,
                curr_month_as_first_term=False):
    if n_terms <= 0:
        print("ERROR: 0th expiration makes no sense. Please use prev_expiry() for past expiries.")
        return None
    if not isinstance(date_in_month, pd.Timestamp):
        date_in_month = pd.to_datetime(date_in_month)
    curr_month_expiry = expiry_func(date_in_month)
    if date_in_month <= curr_month_expiry or curr_month_as_first_term:
        months_forward = n_terms - 1
    else:
        months_forward = n_terms
    if months_forward == 0:
        return curr_month_expiry
    else:
        designated_month_first = (date_in_month.replace(day=1)
                                  + pd.DateOffset(months=months_forward))
        return expiry_func(designated_month_first)


# Given a date, monthly expiry function (returns expiry date of month given day in month),
# and number of terms, return the designated expiry
# NOTE: if date given is the expiration date, it will NOT be returned as the "previous" expiry
#       since expiration would technically happen at the end of that day
# NOTE: set curr_month_as_first_term to True to ensure that expiry for given date's month
#       will be given as first term, even if date is before month's expiration date
def prev_expiry(date_in_month, expiry_func=third_friday, n_terms=1,
                curr_month_as_first_term=False):
    if n_terms <= 0:
        print("ERROR: 0th expiration makes no sense. Please use next_expiry() for future expiries.")
        return None
    if not isinstance(date_in_month, pd.Timestamp):
        date_in_month = pd.to_datetime(date_in_month)
    curr_month_expiry = expiry_func(date_in_month)
    if curr_month_expiry < date_in_month or curr_month_as_first_term:
        months_backward = n_terms - 1
    else:
        months_backward = n_terms
    if months_backward == 0:
        return curr_month_expiry
    else:
        designated_month_first = (date_in_month.replace(day=1)
                                  - pd.DateOffset(months=months_backward))
        return expiry_func(designated_month_first)


# Given a date and tenor, return the next Treasury futures maturity, which is the
# 7th business day preceding the last business day of the next quarterly month
def next_treasury_futures_maturity(date, tenor=10):
    # Different tenors have different rules for maturity date
    if tenor in [2, 5]:
        n_days_before_last = 0
    elif tenor in [10, 30]:
        n_days_before_last = 7
    else:
        print("ERROR: unrecognized tenor - {}.".format(tenor))
        return None
    date = pd.to_datetime(date)
    if date.month in [3, 6, 9, 12]:
        curr_month_maturity = n_before_last_bus_day(date, n_days_before_last)
        if date <= curr_month_maturity:
            return curr_month_maturity
    # Go to next quarterly month
    return n_before_last_bus_day(date.replace(month=next_quarter_month(date.month)),
                                 n_days_before_last)


###############################################################################

if __name__ == '__main__':
    print("expiry_by_terms on 2019-04-09, 3 terms:\n{}"
          .format(next_expiry('2019-04-09', n_terms=3)))
    print("expiry_by_terms on 2019-12-09, 1 terms:\n{}"
          .format(next_expiry('2019-12-09', last_friday, 1)))
    print("expiry_by_terms on 2019-12-27, 1 terms:\n{}"
          .format(next_expiry('2019-12-27', last_friday, 1)))
    print("expiry_by_terms on 2016-03-09, 1 terms:\n{}"
          .format(next_expiry('2016-03-09', last_friday, 1)))
    print("expiry_by_terms on 2016-02-09, 2 terms:\n{}"
          .format(next_expiry('2016-02-09', last_friday, 2)))
    print("next_ty_futures_maturity on 2019-02-09:\n{}"
          .format(next_treasury_futures_maturity('2019-02-09')))
    print("next_ty_futures_maturity on 2019-03-19:\n{}"
          .format(next_treasury_futures_maturity('2019-03-19')))
    print("next_ty_futures_maturity on 2019-03-20:\n{}"
          .format(next_treasury_futures_maturity('2019-03-20')))

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
next_ty_futures_maturity on 2019-03-21:
2019-06-19 00:00:00
"""
