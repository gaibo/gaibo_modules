import pandas as pd
from cboe_exchange_holidays_v3 import CboeTradingCalendar, datelike_to_timestamp, timelike_to_timedelta

CBOE_TRADING_CALENDAR = CboeTradingCalendar()
BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=CBOE_TRADING_CALENDAR)
DAY_NAME_TO_WEEKDAY_NUMBER_DICT = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
                                   'Friday': 4, 'Saturday': 5, 'Sunday': 6}
TREASURY_FUTURES_MATURITY_TIME = pd.Timedelta('16:00:00')
TREASURY_OPTIONS_EXPIRY_TIME = TREASURY_FUTURES_MATURITY_TIME


###############################################################################
# Utilities

def get_prev_business_day(datelike):
    """ Return previous business day using CustomBusinessDay with Cboe trading calendar
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    return date - BUSDAY_OFFSET


def n_before_last_bus_day(datelike_in_month, n):
    """ Find date that is "n days preceding the last business day of the month";
        useful for certain expiries/maturities (e.g. Treasury options and futures)
    :param datelike_in_month: date-like representation of any day in the month
    :param n: number of days preceding last business day of month
    :return: pd.Timestamp
    """
    date_in_month = datelike_to_timestamp(datelike_in_month)
    next_month_first = date_in_month.replace(day=1) + pd.DateOffset(months=1)
    this_month_last_bd = next_month_first - BUSDAY_OFFSET
    return this_month_last_bd - n*BUSDAY_OFFSET


###############################################################################
# Simple next and previous iteration

def next_weekday(datelike, weekday_number, weekday_return_self=False):
    """ Return date of the next day-of-week specified
        NOTE: default return one week forward if current weekday is the desired day-of-week;
              to return current date, set weekday_return_self to True
        NOTE: see DAY_NAME_TO_WEEKDAY_NUMBER_DICT for day-of-week conversion
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param weekday_number: numbering starts with 0 for Monday and goes through 6 for Sunday
    :param weekday_return_self: set True to include current date in consideration
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    days_ahead = weekday_number - date.weekday()
    # Account for date already being past this week's desired day-of-week
    if weekday_return_self:
        days_ahead = days_ahead if days_ahead >= 0 else days_ahead + 7
    else:
        days_ahead = days_ahead if days_ahead > 0 else days_ahead + 7
    return date + pd.DateOffset(days=days_ahead)


def prev_weekday(datelike, weekday_number, weekday_return_self=False):
    """ Return date of the previous day-of-week specified
        NOTE: default return one week backward if current weekday is the desired day-of-week;
              to return current date, set weekday_return_self to True
        NOTE: see DAY_NAME_TO_WEEKDAY_NUMBER_DICT for day-of-week conversion
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param weekday_number: numbering starts with 0 for Monday and goes through 6 for Sunday
    :param weekday_return_self: set True to include current date in consideration
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    days_behind = date.weekday() - weekday_number
    # Account for date already being before this week's desired day-of-week
    if weekday_return_self:
        days_behind = days_behind if days_behind >= 0 else days_behind + 7
    else:
        days_behind = days_behind if days_behind > 0 else days_behind + 7
    return date - pd.DateOffset(days=days_behind)


def next_quarterly_month(datelike, quarter_return_self=False):
    """ Return date with month changed to the next quarterly month
        NOTE: default map is [3, 3, 6, 6, 6, 9, 9, 9, 12, 12, 12, 3], i.e. quarterly month
              returns the next quarterly month (allows iterative use of function);
              to return itself, set quarter_return_self to True
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param quarter_return_self: set True to map [3, 3, 3, 6, 6, 6, 9, 9, 9, 12, 12, 12]
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    date_month = date.month
    if quarter_return_self:
        next_quarter_month = (((date_month - 1) // 3) + 1) * 3
    else:
        next_quarter_month = (date_month // 3 % 4 + 1) * 3  # This expression is super flexible
    if next_quarter_month < date_month:
        year_increment = 1
    else:
        year_increment = 0
    return date.replace(month=next_quarter_month) + pd.DateOffset(years=year_increment)


def prev_quarterly_month(datelike, quarter_return_self=False):
    """ Return date with month changed to the previous quarterly month
        NOTE: default map is [12, 12, 12, 3, 3, 3, 6, 6, 6, 9, 9, 9], i.e. quarterly month
              returns the previous quarterly month (allows iterative use of function);
              to return itself, set quarter_return_self to True
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param quarter_return_self: set True to map [12, 12, 3, 3, 3, 6, 6, 6, 9, 9, 9, 12]
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    date_month = date.month
    if quarter_return_self:
        prev_quarter_month = ((date_month-3) // 3 % 4 + 1) * 3
    else:
        prev_quarter_month = ((date_month-4) // 3 % 4 + 1) * 3
    if prev_quarter_month > date_month:
        year_decrement = 1
    else:
        year_decrement = 0
    return date.replace(month=prev_quarter_month) - pd.DateOffset(years=year_decrement)


###############################################################################
# Monthly expiration date functions

def third_friday(datelike_in_month):
    """ Return third-Friday options expiration date of month
        NOTE: return date could be before input date; only month (and year) matters
    :param datelike_in_month: date-like representation of any day in the month
    :return: pd.Timestamp
    """
    date_in_month = datelike_to_timestamp(datelike_in_month)
    earliest_third_week_day = date_in_month.replace(day=15)     # 15th is start of third week
    third_week_friday = next_weekday(earliest_third_week_day, 4, weekday_return_self=True)
    # If third Friday is an exchange holiday, return business day before
    return third_week_friday + BUSDAY_OFFSET - BUSDAY_OFFSET


def third_saturday(datelike_in_month):
    """ Return third-Saturday options expiration date of month (SPX, up until 2015-02)
        NOTE: return date could be before input date; only month (and year) matters
    :param datelike_in_month: date-like representation of any day in the month
    :return: pd.Timestamp
    """
    date_in_month = datelike_to_timestamp(datelike_in_month)
    earliest_third_week_day = date_in_month.replace(day=15)     # 15th is start of third week
    third_week_saturday = next_weekday(earliest_third_week_day, 5, weekday_return_self=True)
    # No issue of third Saturday falling on exchange holiday, I think
    return third_week_saturday


def last_friday(datelike_in_month):
    """ Return last-Friday (at least 2 business days preceding last business day of month)
        options expiration date of month (Treasury)
        NOTE: return date could be before input date; only month (and year) matters
    :param datelike_in_month: date-like representation of any day in the month
    :return: pd.Timestamp
    """
    date_in_month = datelike_to_timestamp(datelike_in_month)
    latest_applicable_day = n_before_last_bus_day(date_in_month, 2)
    latest_applicable_friday = prev_weekday(latest_applicable_day, 4, weekday_return_self=True)
    # If last Friday is an exchange holiday, return business day before
    return latest_applicable_friday + BUSDAY_OFFSET - BUSDAY_OFFSET


###############################################################################
# Complex product expiry/maturity tools

def next_expiry(datelike_in_month, expiry_func=third_friday, n_terms=1,
                curr_month_as_first_term=False, expiry_time=None):
    """ Find designated expiration date
        NOTE: if input date is the expiration date, it will be returned as the "next"
              expiry, since expiration would technically happen at the end of that day
    :param datelike_in_month: date-like representation of any day in the month
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :param n_terms: number of terms forward (1 or more)
    :param curr_month_as_first_term: set True to force input date's month as the first term,
                                     even if input date is after month's expiration
    :param expiry_time: specific time of expiration on expiration date; e.g. '16:15:00' for 4:15pm
    :return: pd.Timestamp
    """
    if n_terms <= 0:
        print("ERROR: 0th expiration makes no sense. Please use prev_expiry() for past expiries.")
        return None
    date_in_month = datelike_to_timestamp(datelike_in_month)
    if expiry_time is None:
        curr_month_expiry = expiry_func(date_in_month)
    else:
        curr_month_expiry = expiry_func(date_in_month) + timelike_to_timedelta(expiry_time)
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


def prev_expiry(datelike_in_month, expiry_func=third_friday, n_terms=1,
                curr_month_as_first_term=False, expiry_time=None):
    """ Find designated expiration date
        NOTE: if input date is the expiration date, it will NOT be returned as the "previous"
              expiry, since expiration would technically happen at the end of that day
    :param datelike_in_month: date-like representation of any day in the month
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :param n_terms: number of terms backward (1 or more)
    :param curr_month_as_first_term: set True to force input date's month as the first term,
                                     even if input date is before month's expiration
    :param expiry_time: specific time of expiration on expiration date; e.g. '16:15:00' for 4:15pm
    :return: pd.Timestamp
    """
    if n_terms <= 0:
        print("ERROR: 0th expiration makes no sense. Please use next_expiry() for future expiries.")
        return None
    date_in_month = datelike_to_timestamp(datelike_in_month)
    if expiry_time is None:
        curr_month_expiry = expiry_func(date_in_month)
    else:
        curr_month_expiry = expiry_func(date_in_month) + timelike_to_timedelta(expiry_time)
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


def next_treasury_futures_maturity(datelike, n_terms=1, tenor=10):
    """ Find designated CBOT Treasury futures maturity date, 7th business day preceding
        the last business day of the quarterly month
        NOTE: if input date is the maturity date, it will be returned as the "next"
              maturity, since maturation would technically happen at the end of that day
    :param datelike: date-like representation of any day in the month
    :param n_terms: number of terms forward (1 or more)
    :param tenor: 2, 5, 10, or 30 for 2-, 5-, 10-, or 30-year Treasury note futures
    :return: pd.Timestamp
    """
    if n_terms <= 0:
        print("ERROR: 0th expiration makes no sense.\n"
              "       Please use prev_treasury_futures_maturity() for past expiries.")
        return None
    # Different tenors have different rules for maturity date
    if tenor in [2, 5]:
        n_days_before_last = 0
    elif tenor in [10, 30]:
        n_days_before_last = 7
    else:
        print("ERROR: unrecognized tenor - {}.".format(tenor))
        return None
    date = datelike_to_timestamp(datelike)
    if date.month in [3, 6, 9, 12]:
        # Evaluate whether current quarterly month's maturity has already passed
        curr_month_maturity = n_before_last_bus_day(date, n_days_before_last) + TREASURY_FUTURES_MATURITY_TIME
        if date <= curr_month_maturity:
            terms_forward = n_terms - 1
        else:
            terms_forward = n_terms
        # Take a shortcut that is slightly more efficient than next_quarterly_month()
        if terms_forward == 0:
            return curr_month_maturity
        else:
            designated_quarter_month_date = date + pd.DateOffset(months=terms_forward*3)
            return n_before_last_bus_day(designated_quarter_month_date, n_days_before_last)
    else:
        # Iterate through next quarterly months
        for _ in range(n_terms):
            date = next_quarterly_month(date)
        return n_before_last_bus_day(date, n_days_before_last) + TREASURY_FUTURES_MATURITY_TIME


def prev_treasury_futures_maturity(datelike, n_terms=1, tenor=10):
    """ Find designated CBOT Treasury futures maturity date, 7th business day preceding
        the last business day of the quarterly month
        NOTE: if input date is the maturity date, it will NOT be returned as the "previous"
              maturity, since maturation would technically happen at the end of that day
    :param datelike: date-like representation of any day in the month
    :param n_terms: number of terms backward (1 or more)
    :param tenor: 2, 5, 10, or 30 for 2-, 5-, 10-, or 30-year Treasury note futures
    :return: pd.Timestamp
    """
    if n_terms <= 0:
        print("ERROR: 0th expiration makes no sense.\n"
              "       Please use next_treasury_futures_maturity() for future expiries.")
        return None
    # Different tenors have different rules for maturity date
    if tenor in [2, 5]:
        n_days_before_last = 0
    elif tenor in [10, 30]:
        n_days_before_last = 7
    else:
        print("ERROR: unrecognized tenor - {}.".format(tenor))
        return None
    date = datelike_to_timestamp(datelike)
    if date.month in [3, 6, 9, 12]:
        # Evaluate whether current quarterly month's maturity has already passed
        curr_month_maturity = n_before_last_bus_day(date, n_days_before_last) + TREASURY_FUTURES_MATURITY_TIME
        if curr_month_maturity < date:
            terms_backward = n_terms - 1
        else:
            terms_backward = n_terms
        # Take a shortcut that is slightly more efficient than next_quarterly_month()
        if terms_backward == 0:
            return curr_month_maturity
        else:
            designated_quarter_month_date = date - pd.DateOffset(months=terms_backward*3)
            return n_before_last_bus_day(designated_quarter_month_date, n_days_before_last)
    else:
        # Iterate through next quarterly months
        for _ in range(n_terms):
            date = prev_quarterly_month(date)
        return n_before_last_bus_day(date, n_days_before_last) + TREASURY_FUTURES_MATURITY_TIME


###############################################################################

if __name__ == '__main__':
    # next_expiry(datelike_in_month, expiry_func=third_friday, n_terms=1,
    #             curr_month_as_first_term=False)
    print("next_expiry('2019-04-09'):\n{}"
          .format(next_expiry('2019-04-09')))
    print("next_expiry('2019-04-09', n_terms=2):\n{}"
          .format(next_expiry('2019-04-09', n_terms=2)))
    print("next_expiry('2019-04-09', n_terms=3):\n{}"
          .format(next_expiry('2019-04-09', n_terms=3)))
    print("next_expiry('2019-04-18', n_terms=1):\n{}"
          .format(next_expiry('2019-04-18', n_terms=1)))
    print("next_expiry('2019-04-18', n_terms=2):\n{}"
          .format(next_expiry('2019-04-18', n_terms=2)))
    print("next_expiry('2019-04-19', n_terms=1, curr_month_as_first_term=True):\n{}"
          .format(next_expiry('2019-04-19', n_terms=1, curr_month_as_first_term=True)))
    print("prev_expiry('2019-06-22', n_terms=3):\n{}"
          .format(prev_expiry('2019-06-22', n_terms=3)))
    print("prev_expiry('2019-05-17', n_terms=2, curr_month_as_first_term=True):\n{}"
          .format(prev_expiry('2019-05-17', n_terms=2, curr_month_as_first_term=True)))
    print("next_expiry('2016-03-09', last_friday, 1):\n{}"
          .format(next_expiry('2016-03-09', last_friday, 1)))
    print("next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12):\n{}"
          .format(next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12)))
    print("prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,\n"
          "            curr_month_as_first_term=True):\n{}"
          .format(prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,
                              curr_month_as_first_term=True)))
    # next_treasury_futures_maturity(datelike, n_terms=1, tenor=10)
    print("next_treasury_futures_maturity('2019-02-09'):\n{}"
          .format(next_treasury_futures_maturity('2019-02-09')))
    print("next_treasury_futures_maturity('2019-02-09', 2):\n{}"
          .format(next_treasury_futures_maturity('2019-02-09', 2)))
    print("next_treasury_futures_maturity('2019-02-09', 3):\n{}"
          .format(next_treasury_futures_maturity('2019-02-09', 3)))
    print("prev_treasury_futures_maturity('2019-09-19', 2):\n{}"
          .format(prev_treasury_futures_maturity('2019-09-19', 2)))
    print("next_treasury_futures_maturity('2019-02-09', tenor=5):\n{}"
          .format(next_treasury_futures_maturity('2019-02-09', tenor=5)))
    print("next_treasury_futures_maturity('2019-02-09', 2, tenor=5):\n{}"
          .format(next_treasury_futures_maturity('2019-02-09', 2, tenor=5)))

""" Expected Output:
next_expiry('2019-04-09'):
2019-04-18 00:00:00
next_expiry('2019-04-09', n_terms=2):
2019-05-17 00:00:00
next_expiry('2019-04-09', n_terms=3):
2019-06-21 00:00:00
next_expiry('2019-04-18', n_terms=1):
2019-04-18 00:00:00
next_expiry('2019-04-18', n_terms=2):
2019-05-17 00:00:00
next_expiry('2019-04-19', n_terms=1, curr_month_as_first_term=True):
2019-04-18 00:00:00
prev_expiry('2019-06-22', n_terms=3):
2019-04-18 00:00:00
prev_expiry('2019-05-17', n_terms=2, curr_month_as_first_term=True):
2019-04-18 00:00:00
next_expiry('2016-03-09', last_friday, 1):
2016-03-24 00:00:00
next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12):
2017-02-24 00:00:00
prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,
            curr_month_as_first_term=True):
2016-03-24 00:00:00
next_treasury_futures_maturity('2019-02-09'):
2019-03-20 00:00:00
next_treasury_futures_maturity('2019-02-09', 2):
2019-06-19 00:00:00
next_treasury_futures_maturity('2019-02-09', 3):
2019-09-19 00:00:00
prev_treasury_futures_maturity('2019-09-19', 2):
2019-03-20 00:00:00
next_treasury_futures_maturity('2019-02-09', tenor=5):
2019-03-29 00:00:00
next_treasury_futures_maturity('2019-02-09', 2, tenor=5):
2019-06-28 00:00:00
"""
