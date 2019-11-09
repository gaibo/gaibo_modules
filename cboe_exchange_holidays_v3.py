# Adapted from code by Pierre Boutquin of Stack Overflow
# https://stackoverflow.com/questions/33094297/create-trading-holiday-calendar-with-pandas

import pandas as pd
from pandas.tseries.holiday import \
    AbstractHolidayCalendar, Holiday, MO, nearest_workday, sunday_to_monday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, USLaborDay, USThanksgivingDay, \
    get_calendar

# Additional holidays
NewYearsDay = Holiday('New Year\'s Day', month=1, day=1, observance=sunday_to_monday)
USIndependenceDay = Holiday('Independence Day', month=7, day=4, observance=nearest_workday)
USColumbusDay = Holiday('Columbus Day', month=10, day=1, offset=pd.DateOffset(weekday=MO(2)))
USVeteransDay = Holiday('Veterans Day', month=11, day=11, observance=sunday_to_monday)
ChristmasDay = Holiday('Christmas', month=12, day=25, observance=nearest_workday)
NYSEUSMartinLutherKingJr = Holiday('Dr. Martin Luther King Jr.',
                                   start_date=pd.Timestamp(1998, 1, 1), month=1, day=1,
                                   offset=pd.DateOffset(weekday=MO(3)))     # Nationally 1986, but NYSE 1998
# Presidential Days of Mourning
GHWBushDayofMourning = Holiday('George H. W. Bush Day of Mourning', year=2018, month=12, day=5)
FordDayofMourning = Holiday('Gerald Ford Day of Mourning', year=2007, month=1, day=2)
ReaganDayofMourning = Holiday('Ronald Reagan Day of Mourning', year=2004, month=6, day=11)
NixonDayofMourning = Holiday('Richard Nixon Day of Mourning', year=1994, month=4, day=27)
# Disasters
HurricaneSandyMonday = Holiday('Hurricane Sandy', year=2012, month=10, day=29)
HurricaneSandyTuesday = Holiday('Hurricane Sandy', year=2012, month=10, day=30)
NineElevenTuesday = Holiday('9/11', year=2001, month=9, day=11)
NineElevenWednesday = Holiday('9/11', year=2001, month=9, day=12)
NineElevenThursday = Holiday('9/11', year=2001, month=9, day=13)
NineElevenFriday = Holiday('9/11', year=2001, month=9, day=14)


class BaseTradingCalendar(AbstractHolidayCalendar):
    """ Base calendar containing US holidays that are universally recognized
    """
    def __init__(self):
        AbstractHolidayCalendar.__init__(self)
        self.rules = [
            NewYearsDay,
            USMartinLutherKingJr,
            USPresidentsDay,
            GoodFriday,
            USMemorialDay,
            USIndependenceDay,
            USLaborDay,
            USThanksgivingDay,
            ChristmasDay
        ]


class CboeTradingCalendar(BaseTradingCalendar):
    """ Cboe's trading calendar; should be the same as NYSE, confirmed back to 1990
        NOTE: if Jan 1 is on a Saturday, New Year's Day is technically not "observed"
              (source: http://cfe.cboe.com/about-cfe/holiday-calendar),
              though it is still listed in the holidays list generated by this
              calendar; it should never matter though, as it's already a weekend
    """
    def __init__(self):
        BaseTradingCalendar.__init__(self)
        self.rules.extend([GHWBushDayofMourning, FordDayofMourning, ReaganDayofMourning, NixonDayofMourning,
                           HurricaneSandyMonday, HurricaneSandyTuesday,
                           NineElevenTuesday, NineElevenWednesday, NineElevenThursday, NineElevenFriday])
        # NYSE/Cboe began observing MLK Day in 1998, not 1986 when it became nationally recognized
        self.rules.remove(USMartinLutherKingJr)
        self.rules.extend([NYSEUSMartinLutherKingJr])


class FICCGSDBusinessCalendar(BaseTradingCalendar):
    """ FICC's GSD business calendar, i.e. days on which Treasury notes can be delivered
        NOTE: observes federal holidays (Columbus and Veterans Day) AND Good Friday
        NOTE: empirically, we see it observes Bush Sr. Day of Mourning but NOT Ford's
              DoM (2007-01-02) or Hurricane Sandy (2012-10-29 and 30); may be other exceptions
    """
    def __init__(self):
        BaseTradingCalendar.__init__(self)
        self.rules.extend([USColumbusDay, USVeteransDay, GHWBushDayofMourning])


def datelike_to_timestamp(datelike):
    """ Utility: Convert date-like representations to pd.Timestamp, for consistency
    :param datelike: date-like representation, e.g. '2019-01-03', datetime object, etc.
    :return: pd.Timestamp version of date
    """
    if not isinstance(datelike, pd.Timestamp):
        return pd.to_datetime(str(datelike))
    else:
        return datelike


def _get_holidays_start_end(cal, start_datelike, end_datelike=None, fancy=False):
    """ Helper: Return list of holidays given start and (optionally) end date
    :param cal: calendar object that has holidays() method
    :param start_datelike: start date, as a date-like representation
    :param end_datelike: end date, as a date-like representation; default current date
    :param fancy: set True to get visually fancy output
    :return: collection of holidays
    """
    start_date = datelike_to_timestamp(start_datelike)
    if end_datelike is None:
        end_date = pd.Timestamp('now').strftime('%Y-%m-%d')     # Today
    else:
        end_date = datelike_to_timestamp(end_datelike)
    return cal.holidays(start_date, end_date, fancy)


def _get_holidays_year(cal, year, fancy=False):
    """ Helper: Return list of holidays in the given year
    :param cal: calendar object that has holidays() method
    :param year: year from which to get holidays
    :param fancy: set True to get visually fancy output
    :return: collection of holidays
    """
    start_date = pd.Timestamp(year, 1, 1)
    end_date = pd.Timestamp(year, 12, 31)
    return cal.holidays(start_date, end_date, fancy)


def get_cboe_holidays(year=None, start_datelike=None, end_datelike=None, fancy=False):
    """ Return list of Cboe exchange holidays
        NOTE: every parameter defaults to None in order to allow 3 configurations:
              1) only year
              2) only start date
              3) start date and end date
    :param year: year from which to get holidays
    :param start_datelike: start date, as a date-like representation
    :param end_datelike: end date, as a date-like representation; default current date
    :param fancy: set True to get visually fancy output
    :return: collection of holidays
    """
    cboe_cal = get_calendar('CboeTradingCalendar')
    if year is not None:
        return _get_holidays_year(cboe_cal, year, fancy)
    else:
        return _get_holidays_start_end(cboe_cal, start_datelike, end_datelike, fancy)


def get_ficcgsd_holidays(year=None, start_datelike=None, end_datelike=None, fancy=False):
    """ Return list of FICC's GSD business holidays
        NOTE: every parameter defaults to None in order to allow 3 configurations:
              1) only year
              2) only start date
              3) start date and end date
    :param year: year from which to get holidays
    :param start_datelike: start date, as a date-like representation
    :param end_datelike: end date, as a date-like representation; default current date
    :param fancy: set True to get visually fancy output
    :return: collection of holidays
    """
    ficcgsd_cal = get_calendar('FICCGSDBusinessCalendar')
    if year is not None:
        return _get_holidays_year(ficcgsd_cal, year, fancy)
    else:
        return _get_holidays_start_end(ficcgsd_cal, start_datelike, end_datelike, fancy)


###############################################################################

if __name__ == '__main__':
    print("\nCboe Year Holidays Check:")
    print("2011:\n{}".format(get_cboe_holidays(2011)))
    print("2016:\n{}".format(get_cboe_holidays(2016)))
    print("2019:\n{}".format(get_cboe_holidays(2019)))
    print("2022:\n{}".format(get_cboe_holidays(2022)))
    print("\nCboe Date Range Holidays Check:")
    print("get_cboe_holidays(start_datelike='2019-11-08', end_datelike='2019-12-31'):\n{}"
          .format(get_cboe_holidays(start_datelike='2019-11-08', end_datelike='2019-12-31')))
    print("get_cboe_holidays(start_datelike=2018):\n{}"
          .format(get_cboe_holidays(start_datelike=2018)))

""" Expected Output
Cboe Year Holidays Check:
2011:
DatetimeIndex(['2011-01-01', '2011-01-17', '2011-02-21', '2011-04-22',
               '2011-05-30', '2011-07-04', '2011-09-05', '2011-11-24',
               '2011-12-26'],
              dtype='datetime64[ns]', freq=None)
2016:
DatetimeIndex(['2016-01-01', '2016-01-18', '2016-02-15', '2016-03-25',
               '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24',
               '2016-12-26'],
              dtype='datetime64[ns]', freq=None)
2019:
DatetimeIndex(['2019-01-01', '2019-01-21', '2019-02-18', '2019-04-19',
               '2019-05-27', '2019-07-04', '2019-09-02', '2019-11-28',
               '2019-12-25'],
              dtype='datetime64[ns]', freq=None)
2022:
DatetimeIndex(['2022-01-01', '2022-01-17', '2022-02-21', '2022-04-15',
               '2022-05-30', '2022-07-04', '2022-09-05', '2022-11-24',
               '2022-12-26'],
              dtype='datetime64[ns]', freq=None)

Cboe Date Range Holidays Check:
get_cboe_holidays(start_datelike='2019-11-08', end_datelike='2019-12-31'):
DatetimeIndex(['2019-11-28', '2019-12-25'], dtype='datetime64[ns]', freq=None)
get_cboe_holidays(start_datelike=2018):
DatetimeIndex(['2018-01-01', '2018-01-15', '2018-02-19', '2018-03-30',
               '2018-05-28', '2018-07-04', '2018-09-03', '2018-11-22',
               '2018-12-05', '2018-12-25', '2019-01-01', '2019-01-21',
               '2019-02-18', '2019-04-19', '2019-05-27', '2019-07-04',
               '2019-09-02'],
              dtype='datetime64[ns]', freq=None)
"""
