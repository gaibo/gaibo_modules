import pandas as pd
import numpy as np
import feedparser
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from metaballon.CleanSplines import ExLinearNaturalCubicSpline

RATES_FILEDIR = 'P:/PrdDevSharedDB/Treasury Rates/'
YIELDS_CSV_FILENAME = 'treasury_cmt_yields.csv'
YIELDS_XML_URL = 'https://data.treasury.gov/feed.svc/DailyTreasuryYieldCurveRateData'
YIELDS_FIELDS = ['1 Mo', '2 Mo', '3 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr', '7 Yr', '10 Yr', '20 Yr', '30 Yr']
MATURITY_NAME_TO_DAYS_DICT = {'1 Mo': 30, '2 Mo': 60, '3 Mo': 91, '6 Mo': 182,
                              '1 Yr': 365, '2 Yr': 730, '3 Yr': 1095, '5 Yr': 1825,
                              '7 Yr': 2555, '10 Yr': 3650, '20 Yr': 7300, '30 Yr': 10950}
RATE_TO_PERCENT = 100


def _parse_raw(raw):
    """ Helper: Clean XML entries for pull_treasury_rates """
    if raw.strip() == '':
        return None
    else:
        return round(float(raw), 2)


def pull_treasury_rates(file_dir=RATES_FILEDIR, file_name=YIELDS_CSV_FILENAME, drop_empty_dates=True):
    """ Pull CMT Treasury yield rates from treasury.gov and write them to disk
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param drop_empty_dates: set True to remove dates on which all rates are missing... AFTER writing
                             and BEFORE returning DataFrame; file written is true to treasury.gov
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
    yields_df.index.name = 'Date'
    yields_df.to_csv(file_dir + file_name)  # Export to disk
    if drop_empty_dates:
        yields_df = yields_df.dropna(how='all')
    return yields_df


def load_treasury_rates(file_dir=RATES_FILEDIR, file_name=YIELDS_CSV_FILENAME, drop_empty_dates=True):
    """ Read CMT Treasury yield rates from disk and load them into DataFrame
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :param drop_empty_dates: set True to remove dates on which all rates are missing
    :return: pd.DataFrame with Treasury rates
    """
    yields_df = pd.read_csv(file_dir + file_name, index_col='Date', parse_dates=True)
    if drop_empty_dates:
        yields_df = yields_df.dropna(how='all')
    return yields_df


def continuous_to_apy(cc_rate):
    """ Convert rate that is continuously compounded to net one-year return (APY)
        Formula: APY = exp(CC) - 1
    :param cc_rate: continuously compounded rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: annualized percentage yield, also in percent
    """
    apy_rate = (np.exp(cc_rate/RATE_TO_PERCENT) - 1) * RATE_TO_PERCENT
    return apy_rate


def apy_to_continuous(apy_rate):
    """ Convert net one-year return (APY) to rate that is continuously compounded
        Formula: CC = log(1 + APY)
        NOTE: this function can prep miscellaneous rates to be used as if they are continuous
    :param apy_rate: annualized percentage yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: continuously compounded rate, also in percent
    """
    cc_rate = np.log(1 + apy_rate/RATE_TO_PERCENT) * RATE_TO_PERCENT
    return cc_rate


def bey_to_apy(bey_rate):
    """ Convert semiannual-coupon-based market rate (BEY) to net one-year return (APY)
        Formula: APY = (1 + BEY/2)^2 - 1
    :param bey_rate: bond equivalent yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: annualized percentage yield, also in percent
    """
    apy_rate = ((1 + bey_rate/RATE_TO_PERCENT/2)**2 - 1) * RATE_TO_PERCENT
    return apy_rate


def apy_to_bey(apy_rate):
    """ Convert net one-year return (APY) to semiannual-coupon-based market rate (BEY)
        Formula: BEY = ((1 + APY)^0.5 - 1) * 2
    :param apy_rate: annualized percentage yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: bond equivalent yield, also in percent
    """
    bey_rate = ((1 + apy_rate/RATE_TO_PERCENT)**0.5 - 1) * 2 * RATE_TO_PERCENT
    return bey_rate


def identity(something):
    """ Identity function purely for decluttering RATE_TYPE_FUNCTION_DISPATCH """
    return something


def apy_to_return(apy_rate, time_to_maturity):
    """ Convert net one-year return (APY) to percent return for given time
    :param apy_rate: annualized percentage yield, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :param time_to_maturity: desired days to maturity
    :return: return for the given time, also in percent
    """
    t = time_to_maturity/365
    return ((1 + apy_rate/RATE_TO_PERCENT)**t - 1) * RATE_TO_PERCENT


def one_plus_rate(rate):
    """ Convert a rate in percent to a 1+rate format
    :param rate: rate in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: rate in 1+rate format (e.g. 1.0243 is 2.43%; 1.0017 is 0.17%)
    """
    return 1 + rate/RATE_TO_PERCENT


# NOTE: - CMT Treasury yields are bond equivalent yields (BEYs)
#       - BEYs cannot be used directly - they must always be used in context of (1 + BEY/2)^2 being an APY
#       - APYs must be used in context of (1 + APY)^t, t being time in years:
#         e.g. an APY of 2.56% means that reinvesting at this rate for 3 years returns 1.0256^3
#       - BEYs are thus converted first to APYs, then into multi-year or fraction-of-year returns
#       - to complicate matters further, Cboe white paper formulas use exp(rt); we can substitute (1 + APY) for exp(r),
#         so we can also calculate an r = log(1 + APY) via a repurposing of the apy_to_continuous function
#       - the rates used for VIX currently do not account for treasury.gov CMT yields being BEY;
#         thus, to replicate VIX rates, we act as if APY = BEY and directly apply the apy_to_continuous function
#       - the zero rates can be used directly as a multiplier, since they already account for days to maturity:
#         e.g. (1+rate_t)*zero_price_t = 100, zero_price_t being price of a zero-coupon bond with t years to maturity
RATE_TYPE_FUNCTION_DISPATCH = {'bey': identity,
                               'BEY': identity,
                               'treasury': identity,
                               'Treasury': identity,
                               'yield': identity,
                               'APY': bey_to_apy,
                               '1+APY': lambda rate: one_plus_rate(bey_to_apy(rate)),
                               'log(1+APY)': lambda rate: apy_to_continuous(bey_to_apy(rate)),
                               'vix': apy_to_continuous,
                               'VIX': apy_to_continuous,
                               'rate_t': lambda rate, days: apy_to_return(bey_to_apy(rate), days),
                               'zero': lambda rate, days: apy_to_return(bey_to_apy(rate), days),
                               '1+rate_t': lambda rate, days: one_plus_rate(apy_to_return(bey_to_apy(rate), days)),
                               '1+zero': lambda rate, days: one_plus_rate(apy_to_return(bey_to_apy(rate), days))}


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


def extrapolation_bounds(rates_time_to_maturity, rates_rates, time_to_maturity):
    """ Compute lower and upper bounds on cubic spline extrapolation period, i.e.
        period left of shortest available maturity (usually the 1-month maturity)
    :param rates_time_to_maturity: days to maturity of the rates term structure
    :param rates_rates: rates of the rates term structure (corresponds to rates_time_to_maturity)
    :param time_to_maturity: desired days to maturity; can be multi-element array
    :return: (bound_lower, bound_upper), where each bound's dimensions match time_to_maturity
    """
    if len(rates_time_to_maturity) < 2 or len(rates_rates) < 2:
        raise ValueError("rates_time_to_maturity and rates_rates must correspond and have length 2 or more.")
    rates_time_to_maturity = np.array(rates_time_to_maturity)
    rates_rates = np.array(rates_rates)
    time_to_maturity = np.array(time_to_maturity)
    # Find reference data points, and derive the slope-defined bounds:
    # 1) shortest maturity (t_1, cmt_1)
    t_1, cmt_1 = rates_time_to_maturity[0], rates_rates[0]
    next_rates_time_to_maturity = rates_time_to_maturity[1:]
    next_rates_rates = rates_rates[1:]
    # 2) next shortest maturity equal or above shortest (t_above, cmt_above)
    idxs_above = next_rates_rates >= cmt_1  # Can't use np.argmax() because equals sign
    cmts_above = next_rates_rates[idxs_above]
    if len(cmts_above) == 0:
        # No such point, i.e. term structure completely inverted (strictly decreasing)
        m_lower = 0     # Backup lower-bound slope of 0
    else:
        t_above, cmt_above = next_rates_time_to_maturity[idxs_above][0], cmts_above[0]
        m_lower = (cmt_above - cmt_1) / (t_above - t_1)  # >=0 slope
    b_lower = cmt_1 - m_lower*t_1
    bound_lower = m_lower*time_to_maturity + b_lower
    # 3) next shortest maturity equal or below shortest (t_below, cmt_below)
    idxs_below = next_rates_rates <= cmt_1  # Can't use np.argmax() because equals sign
    cmts_below = next_rates_rates[idxs_below]
    if len(cmts_below) == 0:
        # No such point, i.e. term structure has no inversions (strictly increasing)
        m_upper = 0     # Backup upper-bound slope of 0
    else:
        t_below, cmt_below = next_rates_time_to_maturity[idxs_below][0], cmts_below[0]
        m_upper = (cmt_below - cmt_1) / (t_below - t_1)  # <=0 slope
    b_upper = cmt_1 - m_upper*t_1
    bound_upper = m_upper*time_to_maturity + b_upper
    return bound_lower, bound_upper


def interpolation_bounds(rates_time_to_maturity, rates_rates, time_to_maturity):
    """ Compute lower and upper bounds on cubic spline interpolation period, i.e.
        period right of shortest available maturity (usually the 1-month maturity)
    :param rates_time_to_maturity: days to maturity of the rates term structure
    :param rates_rates: rates of the rates term structure (corresponds to rates_time_to_maturity)
    :param time_to_maturity: desired days to maturity; can be multi-element array
    :return: (bound_lower, bound_upper), where each bound's dimensions match time_to_maturity
    """
    if len(rates_time_to_maturity) < 2 or len(rates_rates) < 2:
        raise ValueError("rates_time_to_maturity and rates_rates must correspond and have length 2 or more.")
    rates_time_to_maturity = np.array(rates_time_to_maturity)
    rates_rates = np.array(rates_rates)
    # Example of how the following np.searchsorted() is used:
    # let rates_time_to_maturity = [30, 60, 91, 182]; time_to_maturity = [31, 32, ..., 59, 60, 61, ..., 90, 91, 92];
    # then idxs_right_bound = [1, 1, ..., 1, 1, 2, ..., 2, 2, 3]; idxs_left_bound = [0, 0, ..., 0, 0, 1, ..., 1, 1, 2]
    # i.e. the 60 and 91 exact maturity indexes are thus included in the idxs_right_bound
    idxs_right_bound = np.searchsorted(rates_time_to_maturity, time_to_maturity, side='left')   # Right-inclusive
    idxs_left_bound = idxs_right_bound - 1
    if (idxs_left_bound < 0).any():
        raise ValueError("time_to_maturity contains extrapolation period day(s); "
                         "day(s) must be greater than shortest maturity in rates_time_to_maturity.")
    bound_lower = np.minimum(rates_rates[idxs_left_bound], rates_rates[idxs_right_bound])
    bound_upper = np.maximum(rates_rates[idxs_left_bound], rates_rates[idxs_right_bound])
    # Match bounds for time_to_maturity dates that have exact maturities and do not need bounding
    # Recall from earlier example that exact maturity indexes are included in idxs_right_bound
    idxs_right_bound_alt = np.searchsorted(rates_time_to_maturity, time_to_maturity, side='right')  # Left-inclusive
    idxs_exact_maturity = idxs_right_bound != idxs_right_bound_alt  # Take advantage of mismatch
    exact_maturity_rates = rates_rates[idxs_right_bound][idxs_exact_maturity]
    bound_lower[idxs_exact_maturity] = exact_maturity_rates
    bound_upper[idxs_exact_maturity] = exact_maturity_rates
    return bound_lower, bound_upper


def lower_upper_bounds(rates_time_to_maturity, rates_rates, time_to_maturity):
    """ Compute lower and upper bounds on cubic spline interpolation for any period
        NOTE: generalized version combining extrapolation_bounds() and interpolation_bounds()
    :param rates_time_to_maturity: days to maturity of the rates term structure
    :param rates_rates: rates of the rates term structure (corresponds to rates_time_to_maturity)
    :param time_to_maturity: desired days to maturity; can be multi-element array
    :return: (bound_lower, bound_upper), where each bound's dimensions match time_to_maturity
    """
    time_to_maturity = np.array(time_to_maturity)
    # Compute extrapolation and interpolation periods separately
    idxs_extrap = time_to_maturity <= rates_time_to_maturity[0]
    idxs_interp = time_to_maturity > rates_time_to_maturity[0]
    extrap_lower, extrap_upper = extrapolation_bounds(rates_time_to_maturity, rates_rates,
                                                      time_to_maturity[idxs_extrap])
    interp_lower, interp_upper = interpolation_bounds(rates_time_to_maturity, rates_rates,
                                                      time_to_maturity[idxs_interp])
    # Stitch together separate periods' results
    bound_lower = np.full_like(time_to_maturity, np.NaN, dtype=float)
    bound_lower[idxs_extrap] = extrap_lower
    bound_lower[idxs_interp] = interp_lower
    bound_upper = np.full_like(time_to_maturity, np.NaN, dtype=float)
    bound_upper[idxs_extrap] = extrap_upper
    bound_upper[idxs_interp] = interp_upper
    return bound_lower, bound_upper


def get_rate(datelike, time_to_maturity, loaded_rates=None, time_in_years=False,
             interp_method='natural cubic spline', return_rate_type='log(1+APY)',
             drop_2_mo=False, ffill_by_1=False, use_spline_bounds=True):
    """ Get a forward-filled (optional) and interpolated rate (BEY, APY, zero, 1+APY, etc.)
        NOTE: if this function is to be used multiple times, please provide optional parameter loaded_rates
              with the source DataFrame loaded through load_treasury_rates for speed/efficiency purposes
        NOTE: 1 Mo started 2001-07-31; 2 Mo started 2018-10-16;
              20 Yr discontinued 1987-01-01 through 1993-09-30; 30 Yr discontinued 2002-02-19 through 2006-02-08
        NOTE: Columbus Day 2010 (2010-10-11) is inexplicably included on treasury.gov as all NaNs;
              this is an oddity of the dataset as no other federal holiday is included - it must be dropped
        NOTE: maximum forward-fill is restricted to 1 day, as that covers regular holidays
        NOTE: no vectorized input and output implemented
    :param datelike: date on which to obtain rate (can be string or object)
    :param time_to_maturity: number of days to maturity at which rate is interpolated
    :param loaded_rates: DataFrame pre-loaded through load_treasury_rates or pull_treasury_rates
    :param time_in_years: set True if time_to_maturity is in years instead of days
    :param interp_method: method of rates interpolation (e.g. 'natural cubic spline', 'linear')
    :param return_rate_type: type of rate to return; see RATE_TYPE_FUNCTION_DISPATCH for documentation
    :param drop_2_mo: set True to NOT use the 2 Mo maturity, for legacy purposes
    :param ffill_by_1: set True to forward-fill NaN rates from previous day (limit one day prior);
                       not crucial but increases consistency of number of maturities used in spline,
                       paving over inexplicable days on which some but not all of the maturities’ yields are missing
    :param use_spline_bounds: set True to use specially defined linear upper and lower bounds to control spline behavior
    :return: numerical rate in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%), 1+rate format (e.g. 1.0243 is 2.43%;
             1.0017 is 0.17%), or other based on return_rate_type
    """
    date = datelike_to_timestamp(datelike)
    if time_in_years:
        time_to_maturity *= 365     # Convert to days
    # Load CMT Treasury yields dataset
    if loaded_rates is None:
        loaded_rates = load_treasury_rates()    # Read from disk if not provided as parameter
        if date > loaded_rates.index[-1]:
            loaded_rates = pull_treasury_rates()    # Pull update from website if disk version seems outdated
            if date > loaded_rates.index[-1]:
                raise ValueError(f"{datelike} yields have not yet posted to treasury.gov.")
    else:
        if date > loaded_rates.index[-1]:
            raise ValueError(f"{datelike} rate not available in given loaded_rates.")
    loaded_rates = loaded_rates.dropna(how='all')   # Remove inconsistent all-NaN dates such as 2010-10-11
    # Get date's available CMT Treasury yields by
    # 1) forward-filling from previous date (optional)
    # 2) dropping maturities that are not available (optional force-drop 2 Mo)
    if ffill_by_1:
        day_yields = loaded_rates.loc[:date].fillna(method='ffill', limit=1).iloc[-1]
    else:
        day_yields = loaded_rates.loc[:date].iloc[-1]
    if drop_2_mo:
        day_yields = day_yields.drop('2 Mo').dropna()
    else:
        day_yields = day_yields.dropna()
    day_yields_days = [MATURITY_NAME_TO_DAYS_DICT[name] for name in day_yields.index]
    day_yields_yields = day_yields.values
    # Interpolate CMT Treasury yields to get yield for date
    interp_func = INTERPOLATION_FUNCTION_DISPATCH[interp_method]
    treasury_yield = interp_func(day_yields_days, day_yields_yields, time_to_maturity)
    if use_spline_bounds:
        # Ensure interpolated yield satisfies spline-control upper and lower bounds
        lower_bound, upper_bound = lower_upper_bounds(day_yields_days, day_yields_yields, time_to_maturity)
        treasury_yield = max(min(treasury_yield, upper_bound), lower_bound)     # Clamp
    # Convert interpolated Treasury yield into desired format
    convert_func = RATE_TYPE_FUNCTION_DISPATCH[return_rate_type]
    if return_rate_type in ['rate_t', 'zero', '1+rate_t', '1+zero']:
        converted_rate = convert_func(treasury_yield, time_to_maturity)
    else:
        converted_rate = convert_func(treasury_yield)
    return converted_rate


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
    if not np.isclose(1 + test_zero/100, test_1_plus_rate_t):
        print("****FAILED 2****")
    if not np.isclose((1 + test_bey/100/2)**2, 1 + test_apy/100):
        print("****FAILED 3****")
    pass
