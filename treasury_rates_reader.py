import pandas as pd
import numpy as np
import feedparser
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from metaballon.CleanSplines import ExLinearNaturalCubicSpline

RATES_FILEDIR = 'P:/PrdDevSharedDB/Treasury Rates/'
YIELDS_CSV_FILENAME = 'treasury_cmt_yields.csv'
YIELDS_XML_URL = 'https://data.treasury.gov/feed.svc/DailyTreasuryYieldCurveRateData'
YIELDS_FIELDS = ['1 Mo', '2 Mo', '3 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr', '7 Yr', '10 Yr', '20 Yr', '30 Yr']
MATURITY_NAME_TO_DAYS_DICT = {'1 Mo': 30, '2 Mo': 61, '3 Mo': 91, '6 Mo': 182,
                              '1 Yr': 365, '2 Yr': 730, '3 Yr': 1095, '5 Yr': 1825,
                              '7 Yr': 2555, '10 Yr': 3650, '20 Yr': 7300, '30 Yr': 10950}
RATE_TO_PERCENT = 100


def _parse_raw(raw):
    """ Helper: Clean XML entries for pull_treasury_rates """
    if raw.strip() == '':
        return None
    else:
        return round(float(raw), 2)


def pull_treasury_rates(file_dir=RATES_FILEDIR, file_name=YIELDS_CSV_FILENAME):
    """ Pull CMT Treasury yield rates from treasury.gov and write them to disk
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :return: pd.DataFrame with Treasury rates
    """
    # Pull XML feed, retrying until successful
    retry_count = 0
    while retry_count < 10:
        yields_feed = feedparser.parse(YIELDS_XML_URL)
        if len(yields_feed.entries) == 0:
            print("WARNING: Empty feed received.\n"
                  "Retrying...")
            retry_count += 1
        else:
            break
    else:
        raise RuntimeError("treasury.gov failed to return usable XML feed after 10 tries.")
    # Clean each entry and create DataFrame
    yields_dict = {}
    for entry in yields_feed.entries:
        raw_values = entry['m_properties'].split('\n')
        date = pd.Timestamp(raw_values[1])
        yields = [_parse_raw(raw) for raw in raw_values[2:-1]]
        yields_dict[date] = yields
    yields_df = pd.DataFrame(yields_dict, index=YIELDS_FIELDS).T.sort_index()
    yields_df.to_csv(file_dir + file_name)
    return yields_df


def load_treasury_rates(file_dir=RATES_FILEDIR, file_name=YIELDS_CSV_FILENAME):
    """ Read CMT Treasury yield rates from disk and load them into DataFrame
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :return: pd.DataFrame with Treasury rates
    """
    return pd.read_csv(file_dir + file_name, index_col='Date', parse_dates=True)


def continuous_to_apy(cc_rate):
    """ Convert rate that is continuously compounded to net one-year return (APY)
        Formula: APY = exp(CC) - 1
    :param cc_rate: continuously compounded rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: annualized percentage yield, also in percent
    """
    apy_rate = (np.exp(cc_rate/RATE_TO_PERCENT) - 1) * RATE_TO_PERCENT
    return apy_rate if apy_rate >= 0 else 0


def apy_to_continuous(apy_rate):
    """ Convert net one-year return (APY) to rate that is continuously compounded
        Formula: CC = log(1 + APY)
        NOTE: this function can prep miscellaneous rates to be used as if they are continuous
    :param apy_rate: annualized percentage yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: continuously compounded rate, also in percent
    """
    cc_rate = np.log(1 + apy_rate/RATE_TO_PERCENT) * RATE_TO_PERCENT
    return cc_rate if cc_rate >= 0 else 0


def bey_to_apy(bey_rate):
    """ Convert semiannual-coupon-based market rate (BEY) to net one-year return (APY)
        Formula: APY = (1 + BEY/2)^2 - 1
    :param bey_rate: bond equivalent yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: annualized percentage yield, also in percent
    """
    apy_rate = ((1 + bey_rate/RATE_TO_PERCENT/2)**2 - 1) * RATE_TO_PERCENT
    return apy_rate if apy_rate >= 0 else 0


def apy_to_bey(apy_rate):
    """ Convert net one-year return (APY) to semiannual-coupon-based market rate (BEY)
        Formula: BEY = ((1 + APY)^0.5 - 1) * 2
    :param apy_rate: annualized percentage yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: bond equivalent yield, also in percent
    """
    bey_rate = ((1 + apy_rate/RATE_TO_PERCENT)**0.5 - 1) * 2 * RATE_TO_PERCENT
    return bey_rate if bey_rate >= 0 else 0


# NOTE: - CMT Treasury yields are bond equivalent yields (BEYs)
#       - BEYs cannot be used directly - they must always be used in context of (1 + BEY/2)^2 being an APY
#       - APYs must be used in context of (1 + APY)^t, t being time in years:
#         e.g. an APY of 2.56% means that reinvesting at this rate for 3 years returns 1.0256^3
#       - BEYs are thus converted first to APYs, then into multi-year or fraction-of-year returns
#       - to complicate matters further, Cboe white paper formulas use exp(rt); we can substitute (1 + APY) for exp(r),
#         so we can also calculate an r = log(1 + APY) via a repurposing of the apy_to_continuous function
#       - the rates used for VIX currently do not account for treasury.gov CMT yields being BEY;
#         thus, to replicate VIX rates, we act as if APY = BEY and directly apply the apy_to_continuous function
RATE_TYPE_FUNCTION_DISPATCH = {'bey': lambda rate: rate,
                               'BEY': lambda rate: rate,
                               'treasury': lambda rate: rate,
                               'Treasury': lambda rate: rate,
                               'yield': lambda rate: rate,
                               'APY': bey_to_apy,
                               '1+APY': lambda rate: (1 + bey_to_apy(rate)/RATE_TO_PERCENT) * RATE_TO_PERCENT,
                               'rate_t': lambda rate, t:
                                   ((1 + bey_to_apy(rate)/RATE_TO_PERCENT)**t - 1) * RATE_TO_PERCENT,
                               'zero': lambda rate, t:
                                   ((1 + bey_to_apy(rate)/RATE_TO_PERCENT)**t - 1) * RATE_TO_PERCENT,
                               '1+rate_t': lambda rate, t: (1 + bey_to_apy(rate)/RATE_TO_PERCENT)**t * RATE_TO_PERCENT,
                               'log(1+APY)': lambda rate: apy_to_continuous(bey_to_apy(rate)),
                               'vix': apy_to_continuous,
                               'VIX': apy_to_continuous}


def natural_cubic_spline_interpolation(rates_time_to_maturity, rates_rates, time_to_maturity):
    """ Interpolate rate using natural cubic spline
    :param rates_time_to_maturity: days to maturity of the rates term structure
    :param rates_rates: rates of the rates term structure (corresponds to rates_time_to_maturity)
    :param time_to_maturity: desired days to maturity
    :return: interpolated rate for time_to_maturity
    """
    nat_cub_spl = ExLinearNaturalCubicSpline(rates_time_to_maturity, rates_rates)
    return nat_cub_spl.eval(time_to_maturity)


def linear_interpolation(rates_time_to_maturity, rates_rates, time_to_maturity):
    """ Interpolate rate linearly
    :param rates_time_to_maturity: days to maturity of the rates term structure
    :param rates_rates: rates of the rates term structure (corresponds to rates_time_to_maturity)
    :param time_to_maturity: desired days to maturity
    :return: interpolated rate for time_to_maturity
    """
    # Get the maturities just shorter and just longer than the desired time_to_maturity
    shorter_maturity_index = 0
    longer_maturity_index = len(rates_time_to_maturity)
    for index, days in enumerate(rates_time_to_maturity):
        if time_to_maturity >= days:
            # Move shorter maturity towards longer each loop
            shorter_maturity_index = index
        if time_to_maturity <= days:
            # If longer maturity is reached, both shorter and longer have been found
            longer_maturity_index = index
            break
    if shorter_maturity_index == longer_maturity_index:
        # Skip interpolation
        return rates_rates[shorter_maturity_index]
    else:
        # Interpolate between shorter and longer maturity
        shorter_rate = rates_rates[shorter_maturity_index]
        shorter_days = rates_time_to_maturity[shorter_maturity_index]
        longer_rate = rates_rates[longer_maturity_index]
        longer_days = rates_time_to_maturity[longer_maturity_index]
        shorter_proportion = (longer_days - time_to_maturity) / (longer_days - shorter_days)
        longer_proportion = 1 - shorter_proportion
        return shorter_proportion * shorter_rate + longer_proportion * longer_rate


INTERPOLATION_FUNCTION_DISPATCH = {'natural cubic spline': natural_cubic_spline_interpolation,
                                   'linear': linear_interpolation}


def get_rate(datelike, time_to_maturity, loaded_rates=None, time_in_years=False,
             interp_method='natural cubic spline', return_rate_type='log(1+APY)'):
    """ Get a forward-filled and interpolated rate (BEY, APY, zero, etc.)
        NOTE: if this function is to be used multiple times, please provide optional parameter loaded_rates
              with the source DataFrame loaded through load_treasury_rates for speed/efficiency purposes
        NOTE: 1 Mo started 2001-07-31; 2 Mo started 2018-10-16;
              20 Yr discontinued 1987-01-01 through 1993-09-30; 30 Yr discontinued 2002-02-19 through 2006-02-08
        NOTE: maximum forward-fill is restricted to 1 day, as that covers regular holidays
        NOTE: no vectorized input and output implemented
    :param datelike: date on which to obtain rate (can be string or object)
    :param time_to_maturity: number of days to maturity at which rate is interpolated
    :param loaded_rates: DataFrame pre-loaded through load_treasury_rates or pull_treasury_rates
    :param time_in_years: set True if time_to_maturity is in years instead of days
    :param interp_method: method of rates interpolation (e.g. 'natural cubic spline', 'linear')
    :param return_rate_type: type of rate to return; see RATE_TYPE_FUNCTION_DISPATCH for documentation
    :return: numerical rate in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    """
    date = datelike_to_timestamp(datelike)
    if time_in_years:
        time_to_maturity *= 365     # Convert to days
    # Get date's available rates by
    # 1) forward-filling from previous date and 2) dropping maturities that are not available
    if loaded_rates is None:
        loaded_rates = load_treasury_rates()    # Read from disk if not provided as parameter
        if date > loaded_rates.index[-1]:
            loaded_rates = pull_treasury_rates()    # Pull update from website if disk version seems outdated
            if date > loaded_rates.index[-1]:
                raise ValueError(f"{datelike} yields have not yet posted to treasury.gov.")
    else:
        if date > loaded_rates.index[-1]:
            raise ValueError(f"{datelike} rate not available in given loaded_rates.")
    day_rates = loaded_rates.loc[:date].fillna(method='ffill', limit=1).iloc[-1].dropna()
    day_rates_days = [MATURITY_NAME_TO_DAYS_DICT[name] for name in day_rates.index]
    day_rates_rates = day_rates.values
    # Interpolate CMT Treasury rates
    interp_func = INTERPOLATION_FUNCTION_DISPATCH[interp_method]
    treasury_rate = interp_func(day_rates_days, day_rates_rates, time_to_maturity)
    # Convert interpolated Treasury rate into desired format
    convert_func = RATE_TYPE_FUNCTION_DISPATCH[return_rate_type]
    if return_rate_type in ['rate_t', 'zero', '1+rate_t']:
        return convert_func(treasury_rate, time_to_maturity/365)
    else:
        return convert_func(treasury_rate)


###############################################################################

if __name__ == '__main__':
    disk_rates = load_treasury_rates()
    test_bey = get_rate('2019-12-07', 61, disk_rates, return_rate_type='BEY')
    test_apy = get_rate('2019-12-07', 61, disk_rates, return_rate_type='APY')
    test_zero = get_rate('2019-12-07', 61, disk_rates, return_rate_type='zero')
    test_1_plus_rate_t = get_rate('2019-12-07', 61, disk_rates, return_rate_type='1+rate_t')
    test_r = get_rate('2019-12-07', 61, disk_rates)
    if not np.isclose(np.exp(test_r/100 * 61/365), 1 + test_zero/100):
        print("****FAILED 1****")
    if not np.isclose((1 + test_zero/100) * 100, test_1_plus_rate_t):
        print("****FAILED 2****")
    if not np.isclose((1 + test_bey/100/2)**2, 1 + test_apy/100):
        print("****FAILED 3****")
    pass
