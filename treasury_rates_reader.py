import pandas as pd
import numpy as np
from metaballon.CleanSplines import ExLinearNaturalCubicSpline

RATES_CSV_FILEDIR = 'P:/PrdDevSharedDB/Treasury Rates/'
RATES_CSV_FILENAME = 'treasury_rates.csv'
MATURITY_NAME_TO_DAYS_DICT = {'1 Mo': 30, '2 Mo': 61, '3 Mo': 91, '6 Mo': 182,
                              '1 Yr': 365, '2 Yr': 730, '3 Yr': 1095, '5 Yr': 1825,
                              '7 Yr': 2555, '10 Yr': 3650, '20 Yr': 7300, '30 Yr': 10950}
RATE_TO_PERCENT = 100


def load_treasury_rates(file_dir=None, file_name=None):
    """ Read CMT Treasury yield rates from disk and load them into a DataFrame
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :return: pd.DataFrame with Treasury rates
    """
    # Use default directory and file name
    if file_dir is None:
        file_dir = RATES_CSV_FILEDIR
    if file_name is None:
        file_name = RATES_CSV_FILENAME
    # Load designated CSV containing rates
    try:
        return pd.read_csv(file_dir + file_name, index_col='Date', parse_dates=True)
    except FileNotFoundError:
        print("ERROR: '{}' Treasury rates file not found.".format(file_dir+file_name))
        return None


def continuous_apy_to_nominal_bey(continuous_rate):
    """ Convert continuously compounded APY rate to nominal annual rate (BEY)
        Formula: continuous_rate = exp(nominal_rate) - 1
    :param continuous_rate: continuously compounded rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: nominal rate, also in percent
    """
    nominal_rate = np.log(1 + (continuous_rate/RATE_TO_PERCENT)) * RATE_TO_PERCENT
    nonneg_nominal_rate = nominal_rate if nominal_rate >= 0 else 0
    return nonneg_nominal_rate


def nominal_bey_to_continuous_apy(nominal_rate):
    """ Convert nominal annual rate (BEY) to continuously compounded APY rate
        Formula: continuous_rate = exp(nominal_rate) - 1
    :param nominal_rate: nominal rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: continuously compounded rate, also in percent
    """
    continuous_rate = (np.exp(nominal_rate/RATE_TO_PERCENT) - 1) * RATE_TO_PERCENT
    nonneg_continuous_rate = continuous_rate if continuous_rate >= 0 else 0
    return nonneg_continuous_rate


def semiannual_apy_to_nominal_bey(semiannual_rate):
    """ Convert semiannually compounded APY rate to nominal annual rate (BEY)
        Formula: semiannual_rate = (1 + nominal_rate/2)^2 - 1
    :param semiannual_rate: semiannually compounded rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: nominal rate, also in percent
    """
    nominal_rate = (np.sqrt(semiannual_rate/RATE_TO_PERCENT + 1) - 1) * 2 * RATE_TO_PERCENT
    nonneg_nominal_rate = nominal_rate if nominal_rate >= 0 else 0
    return nonneg_nominal_rate


def nominal_bey_to_semiannual_apy(nominal_rate):
    """ Convert nominal annual rate (BEY) to semiannually compounded APY rate
        Formula: semiannual_rate = (1 + nominal_rate/2)^2 - 1
    :param nominal_rate: nominal rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: semiannually compounded rate, also in percent
    """
    semiannual_rate = ((1 + nominal_rate/RATE_TO_PERCENT/2)**2 - 1) * RATE_TO_PERCENT
    nonneg_semiannual_rate = semiannual_rate if semiannual_rate >= 0 else 0
    return nonneg_semiannual_rate


# NOTE: - Treasury rates are bond equivalent yields (BEYs) and nominal annual rates
#       - continuously and semiannually compounded rates must be used as r^t, not exp(r*t)
#       - Cboe officially uses something which I describe as log(annual); you use exp(r*t) to achieve
#         r^t where r is 1+nominal (i.e. an annually compounded rate)
#       - I think that log(semiannual) is the most useful and accurate rate; you use exp(r*t) to achieve
#         r^t where r is the semiannually compounded rate, i.e. the APY of the BEY
RATE_TYPE_FUNCTION_DISPATCH = {'bey': lambda rate: rate,
                               'BEY': lambda rate: rate,
                               'nominal': lambda rate: rate,
                               'treasury': lambda rate: rate,
                               'continuous': nominal_bey_to_continuous_apy,
                               'continuously compounded': nominal_bey_to_continuous_apy,
                               'semiannual': nominal_bey_to_semiannual_apy,
                               'semiannually compounded': nominal_bey_to_semiannual_apy,
                               'vix': continuous_apy_to_nominal_bey,
                               'VIX': continuous_apy_to_nominal_bey,
                               'log(annual)': continuous_apy_to_nominal_bey,
                               'log(semiannual)': lambda rate:
                                   continuous_apy_to_nominal_bey(nominal_bey_to_semiannual_apy(rate))}


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


def get_rate(loaded_rates, date, time_to_maturity, time_in_years=False,
             interp_method='natural cubic spline', return_rate_type='BEY'):
    """ Get a forward-filled and interpolated risk-free rate from DataFrame
        NOTE: no vectorized input and output implemented
    :param loaded_rates: DataFrame loaded through load_treasury_rates
    :param date: date on which to obtain rate (can be string or object)
    :param time_to_maturity: number of days to maturity at which rate is interpolated
    :param time_in_years: set True if time_to_maturity is in years instead of days
    :param interp_method: method of rates interpolation (e.g. 'natural cubic spline', 'linear')
    :param return_rate_type: type of rate to return; see RATE_TYPE_FUNCTION_DISPATCH for documentation
    :return: numerical risk-free rate in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    """
    if not isinstance(date, pd.Timestamp):
        date = pd.Timestamp(date)   # Ensure date is in pandas Timestamp format
    if time_in_years:
        time_to_maturity *= 365     # Convert to days
    # Get date's available rates by
    # 1) forward-filling if date not available and 2) dropping maturities that are not available
    day_rates = loaded_rates.loc[:date].fillna(method='ffill').iloc[-1].dropna()
    day_rates_days = [MATURITY_NAME_TO_DAYS_DICT[name] for name in day_rates.index]
    day_rates_rates = day_rates.values
    # Interpolate CMT Treasury rates
    interp_func = INTERPOLATION_FUNCTION_DISPATCH[interp_method]
    treasury_rate = interp_func(day_rates_days, day_rates_rates, time_to_maturity)
    # Convert interpolated Treasury rate into desired format
    convert_func = RATE_TYPE_FUNCTION_DISPATCH[return_rate_type]
    return convert_func(treasury_rate)
