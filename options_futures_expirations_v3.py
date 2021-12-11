import pandas as pd
from cboe_exchange_holidays_v3 import CboeTradingCalendar, FICCGSDBusinessCalendar, FederalReserveCalendar, \
                                      datelike_to_timestamp, timelike_to_timedelta, strip_to_date

CBOE_TRADING_CALENDAR = CboeTradingCalendar()
BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=CBOE_TRADING_CALENDAR)
TREASURY_BUSINESS_CALENDAR = FICCGSDBusinessCalendar()
TREASURY_BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=TREASURY_BUSINESS_CALENDAR)
AFX_BUSINESS_CALENDAR = FederalReserveCalendar()
AFX_BUSDAY_OFFSET = pd.offsets.CustomBusinessDay(calendar=AFX_BUSINESS_CALENDAR)
DAY_OFFSET = pd.Timedelta(days=1)   # 2x speed of pd.offsets.Day() in date arithmetic
DAY_NAME_TO_WEEKDAY_NUMBER_DICT = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
                                   'Friday': 4, 'Saturday': 5, 'Sunday': 6}
TREASURY_FUTURES_MATURITY_TIME = pd.Timedelta('16:00:00')
TREASURY_OPTIONS_EXPIRY_TIME = TREASURY_FUTURES_MATURITY_TIME
ONE_YEAR = pd.Timedelta(days=365)
THIRTY_DAYS = pd.Timedelta(days=30)


###############################################################################
# Utilities

def month_to_quarter_shifter(month, shift=-1):
    """ Obtain any quarterly month given an input month using flexible shifting
        Flexibility of this function lies in experimenting with the shift parameter, e.g.:
        - shift=-1 (default) returns [3,  3,  3,  6,  6,  6,  9,  9,  9, 12, 12, 12]
        - shift=0 returns            [3,  3,  6,  6,  6,  9,  9,  9, 12, 12, 12,  3]
        - shift=2 returns            [6,  6,  6,  9,  9,  9, 12, 12, 12,  3,  3,  3]
    :param month: input month number(s); arrays above are returned when np.arange(1, 13) is inputted
    :param shift: see explanation above
    :return: "shifted" quarterly month number(s)
    """
    return ((month+shift) // 3 % 4 + 1) * 3


def undl_fut_quarter_month(opt_contr_month):
    """ Find the Treasury future month underlying the Treasury option month
    :param opt_contr_month: numerical month of the options month code;
                            note that for example, September options (U) actually expire
                            in August, but here would be referred to as 9 instead of 8
    :return: numerical month of the quarterly futures (can be used with EXPMONTH_CODES_DICT)
    """
    # For actual month of expiration date, use: month_to_quarter_shifter(opt_exp_month, shift=0)
    return (((opt_contr_month-1) // 3) + 1) * 3     # month_to_quarter_shifter(opt_contr_month, shift=-1)


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
    """ Return date with month changed, accounting for end-of-month
        NOTE: an end of month date will return the last day of the new month
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param new_month: month to change the given date's month to
    :return: pd.Timestamp
    """
    if pd.isna(new_month):
        return pd.NaT   # Necessary because real Timestamps cannot have month replaced with np.NaN
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
    """ Return date with year changed, accounting for end-of-month
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


def change_year_month(datelike, new_year, new_month):
    """ Return date with year and month changed, accounting for end-of-month
        NOTE: equivalent to change_month(change_year(datelike, new_year), new_month)
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :param new_year: year to change given date's year to
    :param new_month: month to change the given date's month to
    :return: pd.Timestamp
    """
    date = datelike_to_timestamp(datelike)
    # If you think the following is overkill, you are probably right.
    # But the goal is to match end-of-monthness as perfectly as possible.
    if date.month == 2 and date.day == 28 and is_leap_year(date.year):
        # This is the only case in which changing the year first would cause the
        # creation of an artificial end-of-month, which may not be desired.
        # Change month first to avoid this less likely leap-year edge case
        # e.g. ('2020-02-28', 2019, 7) -> ('2020-07-28', 2019) -> '2019-07-28' (not EOM)
        # NOT                          -> ('2019-02-28', 7)    -> '2019-07-31' (EOM)
        return change_year(change_month(datelike, new_month), new_year)
    else:
        # In all other cases, changing the year first is better as it adjusts for leap year.
        # Change year first to avoid more likely leap-year edge cases
        # e.g. ('2019-07-28', 2020, 2) -> ('2020-07-28', 2)    -> '2020-02-28' (not EOM)
        # NOT                          -> ('2019-02-28', 2020) -> '2020-02-29' (EOM)
        return change_month(change_year(datelike, new_year), new_month)


def ensure_bus_day(datelike, shift_to='prev', busday_type='Cboe'):
    """ Ensure that all dates are business days, shifting to either previous or next if not
        NOTE: as of 2020-02-26, function produces PerformanceWarning due to vectorization of
              CustomBusinessDay addition not being implemented in pandas.offsets
    :param datelike: date-like representation, e.g. ['2019-01-03', '2020-02-25'], datetime object, etc.
    :param shift_to: 'prev' or 'next' to indicate which business day to correct to
    :param busday_type: recognizes: 'Cboe', 'NYSE', 'SIFMA', 'federal', 'FICC', 'GSD', 'FICCGSD', 'Treasury', 'AFX'
    :return: pd.Timestamps of business dates
    """
    date = datelike_to_timestamp(datelike)
    busday_type = busday_type.lower()
    if busday_type in ['cboe', 'nyse']:
        # Widely used exchange trading days
        busday_offset = BUSDAY_OFFSET
    elif busday_type in ['sifma', 'federal', 'ficc', 'gsd', 'ficcgsd', 'treasury']:
        # SIFMA's calendar of government securities trading days; basically federal business days on which
        # Treasury repo market is open; relevant to fixed income
        busday_offset = TREASURY_BUSDAY_OFFSET
    elif busday_type in ['afx', 'ameribor', 'federal reserve', 'bank']:
        # AFX uses US Federal bank (Federal Reserve) holidays
        busday_offset = AFX_BUSDAY_OFFSET
    else:
        raise ValueError(f"Cannot recognize busday_type \"{busday_type}\"")
    if isinstance(date, pd.Timestamp):
        # Single-element - no need to optimize
        if shift_to == 'prev':
            bus_date = date + busday_offset - busday_offset
        elif shift_to == 'next':
            bus_date = date - busday_offset + busday_offset
        else:
            raise ValueError("shift_to must indicate either 'prev' or 'next' business day")
        return bus_date
    else:
        # Multi-element - optimize based on logic that there are not too many unique dates in history
        unique_date = date.drop_duplicates()
        if shift_to == 'prev':
            unique_bus_date = unique_date + busday_offset - busday_offset
        elif shift_to == 'next':
            unique_bus_date = unique_date - busday_offset + busday_offset
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
        next_quarter_month = month_to_quarter_shifter(date_month, shift=-1)
    else:
        next_quarter_month = month_to_quarter_shifter(date_month, shift=0)
    if next_quarter_month < date_month:
        # Increment year
        return change_year_month(date, date.year+1, next_quarter_month)
    else:
        return change_month(date, next_quarter_month)


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
        prev_quarter_month = month_to_quarter_shifter(date_month, shift=-3)
    else:
        prev_quarter_month = month_to_quarter_shifter(date_month, shift=-4)
    if prev_quarter_month > date_month:
        # Decrement year
        return change_year_month(date, date.year-1, prev_quarter_month)
    else:
        return change_month(date, prev_quarter_month)


def days_between(datelike_1, datelike_2, use_busdays=False):
    """ Return number of calendar or business days between two dates
    :param datelike_1: one of the dates
    :param datelike_2: the other date
    :param use_busdays: set True to only count business days
    :return: number of days
    """
    date_1, date_2 = datelike_to_timestamp(datelike_1), datelike_to_timestamp(datelike_2)
    earlier, later = (date_1, date_2) if date_2 > date_1 else (date_2, date_1)
    if use_busdays:
        return len(pd.date_range(earlier, later, freq=BUSDAY_OFFSET, closed='left'))
    else:
        return (later - earlier).days


###############################################################################
# Monthly expiration date functions

def third_friday(datelike_in_month):
    """ Return third-Friday options expiration date of month [standard options expiry]
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
    """ Return third-Saturday options expiration date of month [SPX, up until 2015-02]
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
        options expiration date of month [CME Treasury options]
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
        Return VIX-style expiration date of month, i.e. 30 days (31 if 30 falls on holiday)
        prior to expiration of next month's options on asset underlying VIX
        Usage: vix_thirty_days_before()('2020-02-24') yields Timestamp('2020-02-19 00:00:00')
        [used on SPX options' third_friday(), this generates VIX futures/options expiries]
    :param expiry_func: monthly expiration date function for asset underlying VIX;
                        third_friday for S&P 500 VIX, last_friday for Treasury VIX
    :return: function that takes parameter datelike_in_month and returns pd.Timestamp
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
        return ensure_bus_day(base_minus_thirty, shift_to='prev')
    return wrapper


def last_of_month(datelike_in_month):
    """ Return last business day of month [CME Treasury Futures (2- and 5-year) maturity]
        NOTE: for Treasury Futures, this should be used with the quarterly_only() wrapper,
              e.g. quarterly_only(last_of_month)('2021-04-08') to get Timestamp('2021-06-30')
        NOTE: return date could be before input date; only month (and year) matters
    :param datelike_in_month: date-like representation of any day in the month (or quarter)
    :return: pd.Timestamp
    """
    return n_before_last_bus_day(datelike_in_month, 0)


def seventh_before_last_of_month(datelike_in_month):
    """ Return 7th business day preceding last business day of month
        [CME Treasury Futures (10- and 30-year) maturity]
        NOTE: for Treasury Futures, this should be used with the quarterly_only() wrapper,
              e.g. quarterly_only(seventh_before_last_of_month)('2021-04-08')
              to get Timestamp('2021-06-21')
        NOTE: return date could be before input date; only month (and year) matters
    :param datelike_in_month: date-like representation of any day in the month (or quarter)
    :return: pd.Timestamp
    """
    return n_before_last_bus_day(datelike_in_month, 7)


def first_of_month(datelike_in_month):
    """ Return first business day of month [iBoxx (IBHY and IBIG) Futures maturity]
        NOTE: return date could be before input date; only month (and year) matters
    :param datelike_in_month: date-like representation of any day in the month
    :return: pd.Timestamp
    """
    date_in_month = datelike_to_timestamp(datelike_in_month)
    first_date_in_month = date_in_month.replace(day=1)
    return ensure_bus_day(first_date_in_month, shift_to='next')


def quarterly_only(expiry_func=seventh_before_last_of_month):
    """ Create function (through augmenting input function) to:
        Return expiration date of current quarter (rather than current month)
        Usage: quarterly_only()('2020-02-24') yields Timestamp('2020-03-20 00:00:00')
        [used on 10-year Treasury futures' seventh_before_last_of_month(), this generates
         only quarterly maturities, useful because Treasury futures only list quarterlies]
    :param expiry_func: monthly expiration date function
    :return: function that takes parameter datelike_in_month and returns pd.Timestamp
    """
    def wrapper(datelike_in_month):
        """ Wrap asset expiration date of month function to return only quarterly results """
        date_in_quarterly_month = next_quarterly_month(datelike_in_month, quarter_return_self=True)
        return expiry_func(date_in_quarterly_month)
    return wrapper


###############################################################################
# Complex product expiry/maturity tools

def next_expiry(datelike, expiry_func=third_friday, n_terms=1,
                curr_as_first_term=False, expiry_time=None):
    """ Find designated expiration date
        Case 1: no expiry_time involved (i.e. imprecise use):
            if input date is the expiration date, it WILL be returned as the "next"
            expiry, since expiration would technically happen at the end of that day
        Case 2: expiry_time is provided (i.e. precise, iterative use):
            if input date-time is the expiration date-time, it WILL NOT be returned
            as the "next" expiry; the next month's expiration date-time will be returned
        NOTE: function originally designed for monthlies, but now works with quarterlies;
              try expiry_func=quarterly_only(monthly_func)
    :param datelike: date-like representation;
                     if expiry_time is None, precision to day; otherwise, precision to time
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :param n_terms: number of terms forward (1 or more)
    :param curr_as_first_term: set True to force input date's month/quarter as the first term,
                               even if input date is after month's expiration
    :param expiry_time: specific time of expiration on expiration date; e.g. '16:15:00' for 4:15pm
    :return: pd.Timestamp
    """
    if n_terms <= 0:
        raise ValueError("0th expiration makes no sense. Please use prev_expiry() for past expiries.")
    date = datelike_to_timestamp(datelike)  # date: agnostic; could be date-only or date-time
    curr_expiry = expiry_func(strip_to_date(date))    # curr_expiry: date-only
    # Account for whether date falls past its expiry_func() expiry, which is only precise to month
    if expiry_time is not None:
        curr_expiry += timelike_to_timedelta(expiry_time)     # curr_expiry: date-and-time
        if date < curr_expiry or curr_as_first_term:
            months_forward = n_terms - 1
        else:
            months_forward = n_terms
    else:
        date = strip_to_date(date)  # date: date-only
        if date <= curr_expiry or curr_as_first_term:
            months_forward = n_terms - 1
        else:
            months_forward = n_terms
    # Now that n_terms has been adjusted, return the appropriate expiry
    if months_forward == 0:
        return curr_expiry
    else:
        # Subtle feature: expiry_func() may return only quarterlies, rather than monthlies
        next_month_expiry = expiry_func(curr_expiry + pd.DateOffset(months=1))
        prev_month_expiry = expiry_func(curr_expiry - pd.DateOffset(months=1))
        if curr_expiry == next_month_expiry or curr_expiry == prev_month_expiry:
            months_forward *= 3     # Adjust n terms from months to quarters
        # Fast-forward to appropriate month and run expiry_func()
        designated_month_first = date.replace(day=1) + pd.DateOffset(months=months_forward)
        designated_month_expiry = expiry_func(strip_to_date(designated_month_first))
        if expiry_time is not None:
            designated_month_expiry += timelike_to_timedelta(expiry_time)
        return designated_month_expiry


def prev_expiry(datelike, expiry_func=third_friday, n_terms=1,
                curr_as_first_term=False, expiry_time=None):
    """ Find designated expiration date
        NOTE: if input date is the expiration date, it will NOT be returned as the "previous"
              expiry, since expiration would technically happen at the end of that day
    :param datelike: date-like representation of any day in the month
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :param n_terms: number of terms backward (1 or more)
    :param curr_as_first_term: set True to force input date's month/quarter as the first term,
                               even if input date is before month's expiration
    :param expiry_time: specific time of expiration on expiration date; e.g. '16:15:00' for 4:15pm
    :return: pd.Timestamp
    """
    if n_terms <= 0:
        raise ValueError("0th expiration makes no sense. Please use next_expiry() for future expiries.")
    date = datelike_to_timestamp(datelike)  # date: agnostic; could be date-only or date-time
    curr_expiry = expiry_func(strip_to_date(date))    # curr_expiry: date-only
    # Account for whether date falls past its expiry_func() expiry, which is only precise to month
    if expiry_time is not None:
        curr_expiry += timelike_to_timedelta(expiry_time)     # curr_expiry: date-and-time
    else:
        date = strip_to_date(date)  # date: date-only
    if date > curr_expiry or curr_as_first_term:
        months_backward = n_terms - 1
    else:
        months_backward = n_terms
    # Now that n_terms has been adjusted, return the appropriate expiry
    if months_backward == 0:
        return curr_expiry
    else:
        # Subtle feature: expiry_func() may return only quarterlies, rather than monthlies
        next_month_expiry = expiry_func(curr_expiry + pd.DateOffset(months=1))
        prev_month_expiry = expiry_func(curr_expiry - pd.DateOffset(months=1))
        if curr_expiry == next_month_expiry or curr_expiry == prev_month_expiry:
            months_backward *= 3  # Adjust n terms from months to quarters
        # Fast-rewind to appropriate month and run expiry_func()
        designated_month_first = date.replace(day=1) - pd.DateOffset(months=months_backward)
        designated_month_expiry = expiry_func(strip_to_date(designated_month_first))
        if expiry_time is not None:
            designated_month_expiry += timelike_to_timedelta(expiry_time)
        return designated_month_expiry


def next_treasury_futures_maturity(datelike, n_terms=1, tenor=10):
    """ Find designated CBOT Treasury futures maturity date, 0th or 7th business day preceding
        the last business day of the quarterly month
        NOTE: if input date is the maturity date, it will be returned as the "next"
              maturity, since maturation would technically happen at the end of that day
        NOTE: originally had separate algorithm from next_expiry, now a subset
    :param datelike: date-like representation of any day in the month
    :param n_terms: number of terms forward (1 or more)
    :param tenor: 2, 5, 10, or 30 for 2-, 5-, 10-, or 30-year Treasury note futures
    :return: pd.Timestamp
    """
    # Different tenors have different rules for maturity date
    if tenor in [2, 5]:
        return next_expiry(datelike, quarterly_only(last_of_month),
                           n_terms, expiry_time=TREASURY_FUTURES_MATURITY_TIME)
    elif tenor in [10, 30]:
        return next_expiry(datelike, quarterly_only(seventh_before_last_of_month),
                           n_terms, expiry_time=TREASURY_FUTURES_MATURITY_TIME)
    else:
        raise ValueError(f"Unrecognized tenor - {tenor}.")


def prev_treasury_futures_maturity(datelike, n_terms=1, tenor=10):
    """ Find designated CBOT Treasury futures maturity date, 0th or 7th business day preceding
        the last business day of the quarterly month
        NOTE: if input date is the maturity date, it will NOT be returned as the "previous"
              maturity, since maturation would technically happen at the end of that day
        NOTE: originally had separate algorithm from prev_expiry, now a subset
    :param datelike: date-like representation of any day in the month
    :param n_terms: number of terms backward (1 or more)
    :param tenor: 2, 5, 10, or 30 for 2-, 5-, 10-, or 30-year Treasury note futures
    :return: pd.Timestamp
    """
    if tenor in [2, 5]:
        return prev_expiry(datelike, quarterly_only(last_of_month),
                           n_terms, expiry_time=TREASURY_FUTURES_MATURITY_TIME)
    elif tenor in [10, 30]:
        return prev_expiry(datelike, quarterly_only(seventh_before_last_of_month),
                           n_terms, expiry_time=TREASURY_FUTURES_MATURITY_TIME)
    else:
        raise ValueError(f"Unrecognized tenor - {tenor}.")


def generate_expiries(start_datelike, end_datelike=None, n_terms=100,
                      specific_product=None, expiry_func=third_friday):
    """ Generate Series of product expiry dates
        NOTE: vix_maturities = generate_expiries('2004-01-02', pd.Timestamp('now'), specific_product='VIX')
    :param start_datelike: left bound (inclusive) on expiries to generate
    :param end_datelike: right bound (inclusive) on expiries to generate; set None to ues n_terms
    :param n_terms: instead of an end date, generate a number of expiries
    :param specific_product: override expiry_func argument with built-in selection;
                             recognizes: 'VIX', 'SPX', 'Treasury options', 'Treasury futures 2/5/10/30', 'iBoxx'
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :return: pd.Series of pd.Timestamp
    """
    start_date = datelike_to_timestamp(start_datelike)
    # Override with bespoke expiry_func for common products
    if isinstance(specific_product, str):
        specific_product = specific_product.lower()     # Normalize to lowercase
        if specific_product in ['vix', 'vix future', 'vix futures', 'vix option', 'vix options']:
            # Common request: Cboe VIX futures/options expiries (same for both)
            expiry_func = vix_thirty_days_before(third_friday)
        elif specific_product in ['spx', 'spx option', 'spx options']:
            # Common request: Cboe SPX options expiries
            expiry_func = third_friday
        elif specific_product in ['spx future', 'spx futures', 'e-mini', 'e-minis', 'spoos',
                                  'e-mini option', 'e-mini options']:
            # CME E-mini (SPX futures) and E-mini options, both of which are quarterly
            expiry_func = quarterly_only(third_friday)
        elif specific_product in ['treasury option', 'treasury options']:
            # CME Treasury options (all tenors)
            expiry_func = last_friday
        elif specific_product in ['treasury future 2', 'treasury futures 2',
                                  'treasury future 5', 'treasury futures 5']:
            # CME Treasury futures (2-year and 5-year tenors)
            expiry_func = quarterly_only(last_of_month)
        elif specific_product in ['treasury future 10', 'treasury futures 10',
                                  'treasury future 30', 'treasury futures 30']:
            # CME Treasury futures (10-year and 30-year tenors)
            expiry_func = quarterly_only(seventh_before_last_of_month)
        elif specific_product in ['iboxx', 'iboxx future', 'iboxx futures', 'ibhy', 'ibig',
                                  'ibhy future', 'ibhy futures', 'ibig future', 'ibig futures']:
            # iBoxx futures (IBHY and IBIG)
            expiry_func = first_of_month
        else:
            raise ValueError(f"Cannot recognize product \"{specific_product}\"")
    # Decide how many expiries to generate
    if end_datelike is not None:
        end_date = datelike_to_timestamp(end_datelike)
        # End date specified - no great way to do this, so iteratively generate
        prev_n_terms, curr_n_terms = 0, 100     # Base n_terms is configured to 100; may be changed
        mat_list = [next_expiry(start_date, expiry_func, n_terms=i) for i in range(prev_n_terms+1, curr_n_terms+1)]
        while mat_list[-1] < end_date:
            prev_n_terms, curr_n_terms = curr_n_terms, 2*curr_n_terms   # Exponential expansion of n_terms
            mat_list_extension = [next_expiry(start_date, expiry_func, n_terms=i)
                                  for i in range(prev_n_terms+1, curr_n_terms+1)]
            mat_list += mat_list_extension
        # Now cut superset to the end date (next_expiry() already takes care of start date)
        mat_ser = pd.Series(mat_list)
        return mat_ser[mat_ser <= end_date].copy()  # mat_ser[:mat_ser.searchsorted(end_date, 'right')-1] works too
    else:
        # Generate specified number of expiries - easier
        mat_list = [next_expiry(start_date, expiry_func, n_terms=i) for i in range(1, n_terms+1)]
        mat_ser = pd.Series(mat_list)
        return mat_ser


def get_maturity_status(datelike, specific_product=None, expiry_func=third_friday, use_busdays=True, side='right'):
    """ Derive current maturity period details - start, end, number of days, elapsed days
    :param datelike: date-like representation
    :param specific_product: override expiry_func argument with built-in selection;
                             recognizes: 'VIX', 'SPX', 'Treasury options', 'Treasury futures 2/5/10/30', 'iBoxx'
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :param use_busdays: set True to only count business days
    :param side: if date is maturity, 'left' shows it as "next", 'right' shows it as "prev";
                 does not matter if date is not maturity case; see numpy.searchsorted() for more
    :return: (previous maturity date, next maturity date,
              number of days in current maturity period, number of days since last maturity)
    """
    # Override with bespoke expiry_func for common products
    if isinstance(specific_product, str):
        specific_product = specific_product.lower()  # Normalize to lowercase
        if specific_product in ['vix', 'vix future', 'vix futures', 'vix option', 'vix options']:
            # Common request: Cboe VIX futures/options expiries (same for both)
            expiry_func = vix_thirty_days_before(third_friday)
        elif specific_product in ['spx', 'spx option', 'spx options']:
            # Common request: Cboe SPX options expiries
            expiry_func = third_friday
        elif specific_product in ['spx future', 'spx futures', 'e-mini', 'e-minis', 'spoos',
                                  'e-mini option', 'e-mini options']:
            # CME E-mini (SPX futures) and E-mini options, both of which are quarterly
            expiry_func = quarterly_only(third_friday)
        elif specific_product in ['treasury option', 'treasury options']:
            # CME Treasury options (all tenors)
            expiry_func = last_friday
        elif specific_product in ['treasury future 2', 'treasury futures 2',
                                  'treasury future 5', 'treasury futures 5']:
            # CME Treasury futures (2-year and 5-year tenors)
            expiry_func = quarterly_only(last_of_month)
        elif specific_product in ['treasury future 10', 'treasury futures 10',
                                  'treasury future 30', 'treasury futures 30']:
            # CME Treasury futures (10-year and 30-year tenors)
            expiry_func = quarterly_only(seventh_before_last_of_month)
        elif specific_product in ['iboxx', 'iboxx future', 'iboxx futures', 'ibhy', 'ibig',
                                  'ibhy future', 'ibhy futures', 'ibig future', 'ibig futures']:
            # iBoxx futures (IBHY and IBIG)
            expiry_func = first_of_month
        else:
            raise ValueError(f"Cannot recognize product \"{specific_product}\"")
    # When date is a maturity date, side='left' shows it as "next", side='right' shows it as "prev"
    # (side does not matter for dates that are not maturity dates)
    date = datelike_to_timestamp(datelike)
    if date == expiry_func(date) and side == 'right':
        # next_expiry() and prev_expiry() were designed for side='left', so need to get creative for 'right'
        prev_mat_date = date
        next_mat_date = next_expiry(date, expiry_func, n_terms=2)
    elif side not in ['left', 'right']:
        raise ValueError(f"Cannot recognize side \"{side}\"; must be 'right' or 'left'")
    else:
        prev_mat_date = prev_expiry(datelike, expiry_func)
        next_mat_date = next_expiry(datelike, expiry_func)
    days_in_mat_period = days_between(prev_mat_date, next_mat_date, use_busdays=use_busdays)
    days_since_mat = days_between(prev_mat_date, datelike, use_busdays=use_busdays)
    return prev_mat_date, next_mat_date, days_in_mat_period, days_since_mat


###############################################################################

if __name__ == '__main__':
    # Monthly expiration date functions
    print("last_of_month('2020-09-21 16:00:01'):\n{}"
          .format(last_of_month('2020-09-21 16:00:01')))
    print("seventh_before_last_of_month('2020-04-30'):\n{}"
          .format(seventh_before_last_of_month('2020-04-30')))
    print("quarterly_only(seventh_before_last_of_month)('2020-04-30'):\n{}"
          .format(quarterly_only(seventh_before_last_of_month)('2020-04-30')))
    print("quarterly_only(seventh_before_last_of_month)('2021-07-05'):\n{}"
          .format(quarterly_only(seventh_before_last_of_month)('2021-07-05')))
    print("first_of_month('2021-05-21'):\n{}"
          .format(first_of_month('2021-05-21')))
    print()

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
          .format(next_expiry('2019-04-19', n_terms=1, curr_as_first_term=True)))
    print("prev_expiry('2019-06-22', n_terms=3):\n{}"
          .format(prev_expiry('2019-06-22', n_terms=3)))
    print("prev_expiry('2019-05-17', n_terms=2, curr_month_as_first_term=True):\n{}"
          .format(prev_expiry('2019-05-17', n_terms=2, curr_as_first_term=True)))
    print("next_expiry('2016-03-09', last_friday, 1):\n{}"
          .format(next_expiry('2016-03-09', last_friday, 1)))
    print("next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12, expiry_time='14:00:00'):\n{}"
          .format(next_expiry('2016-03-09', expiry_func=last_friday, n_terms=12, expiry_time='14:00:00')))
    print("prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,\n"
          "            curr_month_as_first_term=True, expiry_time='14:00:00'):\n{}"
          .format(prev_expiry('2017-02-24', expiry_func=last_friday, n_terms=12,
                              curr_as_first_term=True, expiry_time='14:00:00')))
    print("next_expiry('2019-03-20 16:00:00', expiry_func=last_friday, expiry_time='11:59:59'):\n{}"
          .format(next_expiry('2019-03-20 16:00:00', expiry_func=last_friday, expiry_time='11:59:59')))
    print("next_expiry('2018-10-12', expiry_func=vix_thirty_days_before(third_friday), n_terms=3):\n{}"
          .format(next_expiry('2018-10-12', expiry_func=vix_thirty_days_before(third_friday), n_terms=3)))
    print("next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=1):\n{}"
          .format(next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=1)))
    print("next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=2):\n{}"
          .format(next_expiry('2019-05-01', expiry_func=vix_thirty_days_before(last_friday), n_terms=2)))
    print("next_expiry('2020-04-17', n_terms=1):\n{}"
          .format(next_expiry('2020-04-17', n_terms=1)))
    print("next_expiry('2020-04-17', n_terms=2):\n{}"
          .format(next_expiry('2020-04-17', n_terms=2)))
    print("next_expiry('2020-04-17 16:00:00', n_terms=1):\n{}"
          .format(next_expiry('2020-04-17 16:00:00', n_terms=1)))
    print("next_expiry('2020-04-17 16:00:00', n_terms=2):\n{}"
          .format(next_expiry('2020-04-17 16:00:00', n_terms=2)))
    print("next_expiry('2020-04-17 15:00:00', n_terms=1, expiry_time='16:00:00'):\n{}"
          .format(next_expiry('2020-04-17 15:00:00', n_terms=1, expiry_time='16:00:00')))
    print("next_expiry('2020-04-17 15:00:00', n_terms=2, expiry_time='16:00:00'):\n{}"
          .format(next_expiry('2020-04-17 15:00:00', n_terms=2, expiry_time='16:00:00')))
    print("next_expiry('2020-04-17 16:00:00', n_terms=1, expiry_time='16:00:00'):\n{}"
          .format(next_expiry('2020-04-17 16:00:00', n_terms=1, expiry_time='16:00:00')))
    print("next_expiry('2020-04-17 16:00:00', n_terms=2, expiry_time='16:00:00'):\n{}"
          .format(next_expiry('2020-04-17 16:00:00', n_terms=2, expiry_time='16:00:00')))
    print("prev_expiry('2020-04-17 16:00:00', n_terms=1, expiry_time='16:00:00'):\n{}"
          .format(prev_expiry('2020-04-17 16:00:00', n_terms=1, expiry_time='16:00:00')))
    print("prev_expiry('2020-04-17 16:00:00', n_terms=2, expiry_time='16:00:00'):\n{}"
          .format(prev_expiry('2020-04-17 16:00:00', n_terms=2, expiry_time='16:00:00')))
    print("prev_expiry('2020-04-17 16:00:00', n_terms=1):\n{}"
          .format(prev_expiry('2020-04-17 16:00:00', n_terms=1)))
    print()

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
    print("next_treasury_futures_maturity('2020-06-19 16:00:00', 1):\n{}"
          .format(next_treasury_futures_maturity('2020-06-19 16:00:00', 1)))
    print("next_treasury_futures_maturity('2020-06-19 15:59:59', 2):\n{}"
          .format(next_treasury_futures_maturity('2020-06-19 15:59:59', 2)))
    print("prev_treasury_futures_maturity('2020-09-21 16:00:00', 1):\n{}"
          .format(prev_treasury_futures_maturity('2020-09-21 16:00:00', 1)))
    print("prev_treasury_futures_maturity('2020-09-21 16:00:01', 2):\n{}"
          .format(prev_treasury_futures_maturity('2020-09-21 16:00:01', 2)))

""" Expected Output:
last_of_month('2020-09-21 16:00:01'):
2020-09-30 16:00:01
seventh_before_last_of_month('2020-04-30'):
2020-04-21 00:00:00
quarterly_only(seventh_before_last_of_month)('2020-04-30'):
2020-06-19 00:00:00
quarterly_only(seventh_before_last_of_month)('2021-07-05'):
2021-09-21 00:00:00
first_of_month('2021-05-21'):
2021-05-03 00:00:00

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
next_expiry('2020-04-17', n_terms=1):
2020-04-17 00:00:00
next_expiry('2020-04-17', n_terms=2):
2020-05-15 00:00:00
next_expiry('2020-04-17 16:00:00', n_terms=1):
2020-04-17 00:00:00
next_expiry('2020-04-17 16:00:00', n_terms=2):
2020-05-15 00:00:00
next_expiry('2020-04-17 15:00:00', n_terms=1, expiry_time='16:00:00'):
2020-04-17 16:00:00
next_expiry('2020-04-17 15:00:00', n_terms=2, expiry_time='16:00:00'):
2020-05-15 16:00:00
next_expiry('2020-04-17 16:00:00', n_terms=1, expiry_time='16:00:00'):
2020-05-15 16:00:00
next_expiry('2020-04-17 16:00:00', n_terms=2, expiry_time='16:00:00'):
2020-06-19 16:00:00
prev_expiry('2020-04-17 16:00:00', n_terms=1, expiry_time='16:00:00'):
2020-03-20 16:00:00
prev_expiry('2020-04-17 16:00:00', n_terms=2, expiry_time='16:00:00'):
2020-02-21 16:00:00
prev_expiry('2020-04-17 16:00:00', n_terms=1):
2020-03-20 00:00:00

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
next_treasury_futures_maturity('2020-06-19 16:00:00', 1):
2020-09-21 16:00:00
next_treasury_futures_maturity('2020-06-19 15:59:59', 2):
2020-09-21 16:00:00
prev_treasury_futures_maturity('2020-09-21 16:00:00', 1):
2020-06-19 16:00:00
prev_treasury_futures_maturity('2020-09-21 16:00:01', 2):
2020-06-19 16:00:00
"""
