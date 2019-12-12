import pandas as pd
import numpy as np

RATES_CSV_FILEDIR = 'P:/PrdDevSharedDB/Treasury Rates/'
RATES_CSV_FILENAME = 'treasury_rates.csv'
MATURITY_NAME_TO_DAYS_DICT = {'1 Mo': 30, '2 Mo': 61, '3 Mo': 91, '6 Mo': 182,
                              '1 Yr': 365, '2 Yr': 730, '3 Yr': 1095, '5 Yr': 1825,
                              '7 Yr': 2555, '10 Yr': 3650, '20 Yr': 7300, '30 Yr': 10950}


def load_treasury_rates(file_dir=None, file_name=None):
    """ Read Treasury rates from disk and load them into a DataFrame
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


def convert_to_nominal_rate(compounded_rate):
    """ Convert continuously compounded rate (return of continuous compounding) to nominal annual rate
        Formula: compounded_rate = exp(nominal_rate) - 1
        NOTE: output of this function can be used in exp(rate) calculations
    :param compounded_rate: continuously compounded rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: nominal rate, also in percent
    """
    nominal_rate = np.log(1 + (compounded_rate/100)) * 100
    legit_nominal_rate = nominal_rate if nominal_rate >= 0 else 0
    return legit_nominal_rate


def convert_to_compounded_rate(nominal_rate):
    """ Convert nominal annual rate to continuously compounded rate (return of continuous compounding)
        Formula: compounded_rate = exp(nominal_rate) - 1
    :param nominal_rate: nominal rate, in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    :return: continuously compounded rate, also in percent
    """
    compounded_rate = (np.exp(nominal_rate/100) - 1) * 100
    legit_compounded_rate = compounded_rate if compounded_rate >= 0 else 0
    return legit_compounded_rate


def cubic_spline_interpolation():
    pass


def get_treasury_rate(loaded_rates, date, time_to_maturity, time_in_years=False):
    """ Get a forward-filled and interpolated Treasury rate from DataFrame
        NOTE: no vectorized input and output implemented
    :param loaded_rates: DataFrame loaded through load_treasury_rates
    :param date: date on which to obtain rate (can be string or object)
    :param time_to_maturity: number of days to maturity at which rate is interpolated
    :param time_in_years: set True if time_to_maturity is in years instead of days
    :return: numerical Treasury rate in percent (e.g. 2.43 is 2.43%; 0.17 is 0.17%)
    """
    if not isinstance(date, pd.Timestamp):
        date = pd.Timestamp(date)   # Ensure date is in pandas Timestamp format
    if time_in_years:
        time_to_maturity *= 365     # Convert to days
    # Get date's available rates by
    # 1) forward-filling if date not available and 2) dropping maturities that are not available
    day_rates = loaded_rates.loc[:date].fillna(method='ffill').iloc[-1].dropna()
    day_available_maturity_days_dict = {MATURITY_NAME_TO_DAYS_DICT[name]: name
                                        for name in day_rates.index}
    day_available_maturity_names = list(day_available_maturity_days_dict.values())
    # Get the maturity just shorter and just longer than the desired time_to_maturity
    shorter_maturity_name = day_available_maturity_names[0]
    longer_maturity_name = day_available_maturity_names[-1]
    for days, name in day_available_maturity_days_dict.items():
        if time_to_maturity >= days:
            # Move shorter maturity towards longer each loop
            shorter_maturity_name = name
        if time_to_maturity <= days:
            # If longer maturity is found, convergence has been found
            longer_maturity_name = name
            break
    if shorter_maturity_name == longer_maturity_name:
        return day_rates[shorter_maturity_name]
    else:
        # Interpolate between shorter and longer maturity
        shorter_rate = day_rates[shorter_maturity_name]
        shorter_days = MATURITY_NAME_TO_DAYS_DICT[shorter_maturity_name]
        longer_rate = day_rates[longer_maturity_name]
        longer_days = MATURITY_NAME_TO_DAYS_DICT[longer_maturity_name]
        shorter_proportion = (longer_days - time_to_maturity) / (longer_days - shorter_days)
        longer_proportion = 1 - shorter_proportion
        return shorter_proportion*shorter_rate + longer_proportion*longer_rate
