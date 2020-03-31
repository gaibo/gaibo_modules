import pandas as pd
from cboe_exchange_holidays_v3 import CboeTradingCalendar, FICCGSDBusinessCalendar, \
                                      datelike_to_timestamp, timelike_to_timedelta, strip_to_date

CBOE_TRADING_CALENDAR = CboeTradingCalendar()
BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=CBOE_TRADING_CALENDAR)
TREASURY_BUSINESS_CALENDAR = FICCGSDBusinessCalendar()
TREASURY_BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=TREASURY_BUSINESS_CALENDAR)
DAY_OFFSET = pd.Timedelta(days=1)   # 2x speed of pd.offsets.Day() in date arithmetic
DAY_NAME_TO_WEEKDAY_NUMBER_DICT = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
                                   'Friday': 4, 'Saturday': 5, 'Sunday': 6}
TREASURY_FUTURES_MATURITY_TIME = pd.Timedelta('16:00:00')
TREASURY_OPTIONS_EXPIRY_TIME = TREASURY_FUTURES_MATURITY_TIME
ONE_YEAR = pd.Timedelta(days=365)
THIRTY_DAYS = pd.Timedelta(days=30)


###############################################################################
# Utilities

def get_prev_business_day(datelike):
    """ Return previous business day using CustomBusinessDay with Cboe trading calendar
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    return date - BUSDAY_OFFSET


def n_before_last_bus_day(monthlike, n):
    """ Find date that is "n days preceding the last business day of the month";
        useful for certain expiries/maturities (e.g. Treasury options and futures)
    :param monthlike: date-like representation with precision to month
                      (i.e. can be any day in the month)
    :param n: number of days preceding last business day of month
    :return: pd.Timestamp
    """
    curr_month_last_busday = next_month_first_day(monthlike) - BUSDAY_OFFSET
    return curr_month_last_busday - n*BUSDAY_OFFSET


def n_before_month_last_day(monthlike, n=0, use_busdays=False):
    """ Find date that is n days preceding the last (optionally business) day of the month
        NOTE: n_before_last_bus_day(m, n) == n_before_month_last_day(m, n, use_busdays=True)
              last_day_of_month(m) = n_before_month_last_day(m)
    :param monthlike: date-like representation with precision to month
    :param n: number of days preceding last day of month
    :param use_busdays: set True to use business (trading) days
    :return: pd.Timestamp
    """
    if use_busdays:
        offset = BUSDAY_OFFSET
    else:
        offset = DAY_OFFSET
    curr_month_last = next_month_first_day(monthlike) - offset
    return curr_month_last - n*offset


def is_end_of_month(datelike):
    """ Return True iff date is last date in month
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :return: Boolean
    """
    date = datelike_to_timestamp(datelike)
    next_date = date + DAY_OFFSET
    return True if date.month != next_date.month else False


def change_month(datelike, new_month):
    """ Return date with month changed to specified month
        NOTE: an end of month date will return the last day of the new month
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param new_month: month to change the given date's month to
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    new_month_first = date.replace(day=1, month=new_month)
    if is_end_of_month(date) or date.day > new_month_first.days_in_month:
        # Either 1) date is end-of-month; use last day of new month too
        #        2) date's day-of-month doesn't exist in new month; use last day of new month
        return new_month_first + pd.DateOffset(months=1) - DAY_OFFSET
    else:
        return date.replace(month=new_month)


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
    if pd.isna(new_year):
        return pd.NaT   # Necessary because real Timestamps cannot have year replaced with np.NaN
    date = datelike_to_timestamp(datelike)
    # Handle end of February, which may change depending on year
    if date.month == 2 and date.day == 28 and is_leap_year(new_year):
        return date.replace(year=new_year, day=29)  # 28->29
    elif date.month == 2 and date.day == 29 and not is_leap_year(new_year):
        return date.replace(year=new_year, day=28)  # 29->28
    else:
        return date.replace(year=new_year)


def ensure_bus_day(datelike, shift_to='prev'):
    """ Ensure that all dates are business days, shifting to either previous or next if not
        NOTE: as of 2020-02-26, function produces PerformanceWarning due to vectorization of
              CustomBusinessDay addition not being implemented in pandas.offsets
    :param datelike: date-like representation, e.g. ['2019-01-03', '2020-02-25'], datetime object, etc.
    :param shift_to: 'prev' or 'next' to indicate which business day to correct to
    :return: pd.Timestamps of business dates
    """
    date = datelike_to_timestamp(datelike)
    if isinstance(date, pd.Timestamp):
        # Single-element - no need to optimize
        if shift_to == 'prev':
            bus_date = date + BUSDAY_OFFSET - BUSDAY_OFFSET
        elif shift_to == 'next':
            bus_date = date - BUSDAY_OFFSET + BUSDAY_OFFSET
        else:
            raise ValueError("shift_to must indicate either 'prev' or 'next' business day")
        return bus_date
    else:
        # Multi-element - optimize based on logic that there are not too many unique dates in history
        unique_date = date.drop_duplicates()
        if shift_to == 'prev':
            unique_bus_date = unique_date + BUSDAY_OFFSET - BUSDAY_OFFSET
        elif shift_to == 'next':
            unique_bus_date = unique_date - BUSDAY_OFFSET + BUSDAY_OFFSET
        else:
            raise ValueError("shift_to must indicate either 'prev' or 'next' business day")
        date_df = pd.DataFrame({'date': date})  # Can't use .to_frame() in case date is np.ndarray
        unique_date_df = pd.DataFrame({'unique_date': unique_date, 'unique_bus_date': unique_bus_date})
        merged_df = date_df.merge(unique_date_df, how='left', left_on='date', right_on='unique_date')
        return merged_df['unique_bus_date']


###############################################################################
# Simple next and previous iteration

def next_month_first_day(monthlike):
    """ Return first date of next month
    :param monthlike: date-like representation with precision to month
    :return: pd.Timestamp
    """
    month = datelike_to_timestamp(monthlike)
    return month.replace(day=1) + pd.DateOffset(months=1)


def prev_month_first_day(monthlike):
    """ Return first date of previous month
    :param monthlike: date-like representation with precision to month
    :return: pd.Timestamp
    """
    month = datelike_to_timestamp(monthlike)
    return month.replace(day=1) - pd.DateOffset(months=1)


def forward_6_months(datelike):
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


def backward_6_months(datelike):
    """ Return date that is 6 months backward
        NOTE: an end of month date will return the last day of the month that is 6 months backward
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    next_date = date + DAY_OFFSET
    if date.month != next_date.month:
        # Date is end-of-month; handle end of month differences
        return next_date - pd.DateOffset(months=6) - DAY_OFFSET
    else:
        return date - pd.DateOffset(months=6)


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
        next_quarter_month_year = date.year + 1
    else:
        next_quarter_month_year = date.year
    try:
        change_month_date = date.replace(month=next_quarter_month,
                                         year=next_quarter_month_year)
    except ValueError:
        # Day-of-month past end of new year-month
        change_month_first = date.replace(day=1, month=next_quarter_month,
                                          year=next_quarter_month_year)
        change_month_date = n_before_month_last_day(change_month_first)
    return change_month_date


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
        prev_quarter_month_year = date.year - 1
    else:
        prev_quarter_month_year = date.year
    try:
        change_month_date = date.replace(month=prev_quarter_month,
                                         year=prev_quarter_month_year)
    except ValueError:
        # Day-of-month past end of new year-month
        change_month_first = date.replace(day=1, month=prev_quarter_month,
                                          year=prev_quarter_month_year)
        change_month_date = n_before_month_last_day(change_month_first)
    return change_month_date


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


def vix_thirty_days_before(expiry_func=third_friday):
    """ Create function (through augmenting input function) to:
        Return VIX-style expiration date of month, i.e. 30 days (31 if 30 yields holiday)
        prior to expiration of next month's options on asset underlying VIX
        Usage: vix_thirty_days_before()('2020-02-24') yields Timestamp('2020-02-19 00:00:00')
    :param expiry_func: monthly expiration date function for asset underlying VIX;
                        third_friday for S&P 500 VIX, last_friday for Treasury VIX
    :return: function that take parameter datelike_in_month and returns pd.Timestamp
    """
    def wrapper(datelike_in_month):
        """ Wrap asset expiration date of month function to return VIX-style expiration """
        date_in_month = datelike_to_timestamp(datelike_in_month)
        date_in_next_month = date_in_month + pd.DateOffset(months=1)
        base_expiry = expiry_func(date_in_next_month)
        base_minus_thirty = base_expiry - THIRTY_DAYS
        # Ensure date is not a holiday - shift to date prior if needed
        # To our knowledge, the singular precedent is VIX Weeklys on 2018-12-05 - George
        # H. W. Bush's Day of Mourning fell on a Wednesday and shifted expiration to Tuesday
        base_minus_thirty_bus_day = ensure_bus_day(base_minus_thirty, shift_to='prev')
        return base_minus_thirty_bus_day
    return wrapper


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
    curr_month_expiry = strip_to_date(expiry_func(date_in_month))
    if expiry_time is not None:
        curr_month_expiry += timelike_to_timedelta(expiry_time)
    if date_in_month <= curr_month_expiry or curr_month_as_first_term:
        months_forward = n_terms - 1
    else:
        months_forward = n_terms
    if months_forward == 0:
        return curr_month_expiry
    else:
        designated_month_first = date_in_month.replace(day=1) + pd.DateOffset(months=months_forward)
        designated_month_expiry = strip_to_date(expiry_func(designated_month_first))
        if expiry_time is not None:
            designated_month_expiry += timelike_to_timedelta(expiry_time)
        return designated_month_expiry


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
    curr_month_expiry = strip_to_date(expiry_func(date_in_month))
    if expiry_time is not None:
        curr_month_expiry += timelike_to_timedelta(expiry_time)
    if curr_month_expiry < date_in_month or curr_month_as_first_term:
        months_backward = n_terms - 1
    else:
        months_backward = n_terms
    if months_backward == 0:
        return curr_month_expiry
    else:
        designated_month_first = date_in_month.replace(day=1) - pd.DateOffset(months=months_backward)
        designated_month_expiry = strip_to_date(expiry_func(designated_month_first))
        if expiry_time is not None:
            designated_month_expiry += timelike_to_timedelta(expiry_time)
        return designated_month_expiry


def next_treasury_futures_maturity(datelike, n_terms=1, tenor=10):
    """ Find designated CBOT Treasury futures maturity date, 0th or 7th business day preceding
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
        curr_month_maturity = (strip_to_date(n_before_last_bus_day(date, n_days_before_last))
                               + TREASURY_FUTURES_MATURITY_TIME)
        if date <= curr_month_maturity:
            terms_forward = n_terms - 1
        else:
            terms_forward = n_terms
        # Take a shortcut that is slightly more efficient than next_quarterly_month()
        if terms_forward == 0:
            return curr_month_maturity
        else:
            designated_quarter_month_date = date + pd.DateOffset(months=terms_forward*3)
            return (strip_to_date(n_before_last_bus_day(designated_quarter_month_date, n_days_before_last))
                    + TREASURY_FUTURES_MATURITY_TIME)
    else:
        # Iterate through next quarterly months
        for _ in range(n_terms):
            date = next_quarterly_month(date)
        return (strip_to_date(n_before_last_bus_day(date, n_days_before_last))
                + TREASURY_FUTURES_MATURITY_TIME)


def prev_treasury_futures_maturity(datelike, n_terms=1, tenor=10):
    """ Find designated CBOT Treasury futures maturity date, 0th or 7th business day preceding
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
        curr_month_maturity = (strip_to_date(n_before_last_bus_day(date, n_days_before_last))
                               + TREASURY_FUTURES_MATURITY_TIME)
        if curr_month_maturity < date:
            terms_backward = n_terms - 1
        else:
            terms_backward = n_terms
        # Take a shortcut that is slightly more efficient than next_quarterly_month()
        if terms_backward == 0:
            return curr_month_maturity
        else:
            designated_quarter_month_date = date - pd.DateOffset(months=terms_backward*3)
            return (strip_to_date(n_before_last_bus_day(designated_quarter_month_date, n_days_before_last))
                    + TREASURY_FUTURES_MATURITY_TIME)
    else:
        # Iterate through next quarterly months
        for _ in range(n_terms):
            date = prev_quarterly_month(date)
        return (strip_to_date(n_before_last_bus_day(date, n_days_before_last))
                + TREASURY_FUTURES_MATURITY_TIME)


###############################################################################

if __name__ == '__main__':
    # next_expiry(datelike_in_month, expiry_func=third_friday, n_terms=1,
    #             curr_month_as_first_term=False, expiry_time=None)
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
    print("next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12, expiry_time='14:00:00'):\n{}"
          .format(next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12, expiry_time='14:00:00')))
    print("prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,\n"
          "            curr_month_as_first_term=True, expiry_time='14:00:00'):\n{}"
          .format(prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,
                              curr_month_as_first_term=True, expiry_time='14:00:00')))
    print("next_expiry('2019-03-20 16:00:00', expiry_func=last_friday, expiry_time='11:59:59'):\n{}"
          .format(next_expiry('2019-03-20 16:00:00', expiry_func=last_friday, expiry_time='11:59:59')))
    print("next_expiry('2018-10-12', expiry_func=vix_thirty_days_before(third_friday), n_terms=3):\n{}"
          .format(next_expiry('2018-10-12', expiry_func=vix_thirty_days_before(third_friday), n_terms=3)))
    print("next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=1):\n{}"
          .format(next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=1)))
    print("next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=2):\n{}"
          .format(next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=2)))
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
    print("prev_treasury_futures_maturity('2019-03-22 16:00:00', tenor=30):\n{}"
          .format(prev_treasury_futures_maturity('2019-03-22 16:00:00', tenor=30)))

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
next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12, expiry_time='14:00:00'):
2017-02-24 14:00:00
prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,
            curr_month_as_first_term=True, expiry_time='14:00:00'):
2016-03-24 14:00:00
next_expiry('2019-03-20 16:00:00', expiry_func=last_friday, expiry_time='11:59:59'):
2019-03-22 11:59:59
next_expiry('2018-10-12', expiry_func=vix_thirty_days_before(third_friday), n_terms=3):
2018-12-19 00:00:00
next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=1):
2019-05-22 00:00:00
next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=2):
2019-06-26 00:00:00
next_treasury_futures_maturity('2019-02-09'):
2019-03-20 16:00:00
next_treasury_futures_maturity('2019-02-09', 2):
2019-06-19 16:00:00
next_treasury_futures_maturity('2019-02-09', 3):
2019-09-19 16:00:00
prev_treasury_futures_maturity('2019-09-19', 2):
2019-03-20 16:00:00
next_treasury_futures_maturity('2019-02-09', tenor=5):
2019-03-29 16:00:00
next_treasury_futures_maturity('2019-02-09', 2, tenor=5):
2019-06-28 16:00:00
prev_treasury_futures_maturity('2019-03-22 16:00:00', tenor=30):
2019-03-20 16:00:00
"""
