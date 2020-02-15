import pandas as pd
import pdblp
from cboe_exchange_holidays_v3 import datelike_to_timestamp

BLOOMBERG_PULLS_FILEDIR = 'P:/PrdDevSharedDB/BBG Pull Scripts/'
TREASURY_FUT_CSV_FILENAME = 'treasury_futures_pull.csv'
TENOR_TO_CODE_DICT = {2: 'TU', 5: 'FV', 10: 'TY', 30: 'US'}
EXPMONTH_CODE_DICT = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                      7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
CODE_EXPMONTH_DICT = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                      'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
QUARTER_CODE_LIST = ['H', 'M', 'U', 'Z']


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


def fut_ticker(tenor, expiry_monthlike, expiry_type='options', use_single_digit_year=False):
    """ Derive Bloomberg Treasury futures ticker from expiry of options or futures
    :param tenor: 2, 5, 10, 30, etc. (-year Treasury futures)
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month)
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param use_single_digit_year: set True to return single-digit year, used when querying for current year
    :return: string Bloomberg ticker; e.g. 'TYM18 Comdty' for 10-year June futures in 2018
    """
    tenor_code = TENOR_TO_CODE_DICT[tenor]
    expiry_month = datelike_to_timestamp(expiry_monthlike)
    if expiry_type == 'options':
        next_month_and_year = expiry_month + pd.DateOffset(months=1)
        contract_year = next_month_and_year.year
        contract_month = next_month_and_year.month
    elif expiry_type == 'futures':
        contract_year = expiry_month.year
        contract_month = expiry_month.month
    else:
        raise ValueError("expiry_type must be either 'options' or 'futures'.")
    quarter_code = EXPMONTH_CODE_DICT[undl_fut_quarter_month(contract_month)]
    if use_single_digit_year:
        year_code = f'{contract_year%10}'   # One digit only, useful for current year queries
    else:
        year_code = f'{contract_year%100:02d}'  # Two digits, useful for past years
    ticker = tenor_code + quarter_code + year_code + ' Comdty'
    return ticker


def create_bloomberg_connection(debug=False, port=8194, timeout=25000):
    """ Create pdblp/blpapi connection to the Bloomberg Terminal on this computer
    :param debug: set True for verbose details on everything that comes through connection
    :param port: network port; does not need to be changed
    :param timeout: milliseconds for which function will attempt connection before timing out
    :return:
    """
    # Start pdblp connection to Bloomberg
    # NOTE: start with debugging using flag "debug=True", or set con.debug=True later on
    con = pdblp.BCon(debug=debug, port=port, timeout=timeout)
    con.start()
    return con


def pull_fut_prices(start_datelike, end_datelike, bloomberg_con=None,
                    file_dir=BLOOMBERG_PULLS_FILEDIR, file_name=TREASURY_FUT_CSV_FILENAME):
    """ Pull Treasury futures prices from Bloomberg Terminal and write them to disk
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date
    :param bloomberg_con: active pdblp Bloomberg connection; if None, runs create_bloomberg_connection()
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :return: pd.DataFrame with all Treasury futures prices between start and end dates
    """
    start_date = datelike_to_timestamp(start_datelike)
    end_date = datelike_to_timestamp(end_datelike)
    # Create list of all Treasury futures Bloomberg tickers in use between start and end dates
    ticker_list = []
    for tenor_code in TENOR_TO_CODE_DICT.values():
        for year in range(start_date.year, end_date.year):
            # For all years up to but not including current year of end_date
            for quarter_code in QUARTER_CODE_LIST:
                ticker = tenor_code + quarter_code + f'{year:02d}' + ' Comdty'
                ticker_list.append(ticker)
        # For current year of end_date
        for quarter_code in QUARTER_CODE_LIST:
            ticker = tenor_code + quarter_code + f'{end_date.year%10}' + ' Comdty'
            ticker_list.append(ticker)
    # Get last price time-series of each ticker
    bbg_start_dt = start_date.strftime('%Y%m%d')
    bbg_end_dt = end_date.strftime('%Y%m%d')
    if bloomberg_con is None:
        bloomberg_con = create_bloomberg_connection()
    fut_price_df = bloomberg_con.bdh(ticker_list, 'PX_LAST', start_date=bbg_start_dt, end_date=bbg_end_dt)
    # Export
    fut_price_df.to_csv(file_dir + file_name)
    return fut_price_df


def load_fut_prices(file_dir=BLOOMBERG_PULLS_FILEDIR, file_name=TREASURY_FUT_CSV_FILENAME):
    """ Read Treasury futures prices from disk and load them into DataFrame
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :return: pd.DataFrame with Treasury futures prices
    """
    return pd.read_csv(file_dir + file_name, index_col=0, parse_dates=True, header=[0, 1])


def get_fut_price(trade_date, tenor, expiry_monthlike, expiry_type='options', data=None,
                  contract_year=None, contract_month=None):
    """ Retrieve Treasury futures price from Bloomberg-exported data
        NOTE: contract_year and contract_month can be supplied together as an alternative to expiry_monthlike
    :param trade_date: trade date on which to get price
    :param tenor: 2, 5, 10, 30, etc. (-year Treasury futures)
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month);
                             set None to use contract_year and contract_month
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param data: Bloomberg-formatted dataset loaded via load_fut_prices()
    :param contract_year: options/futures contract year
    :param contract_month: options/futures contract month
    :return: numerical price
    """
    if expiry_monthlike is None:
        expiry_monthlike = f'{contract_year}-{contract_month:02d}'
    if data is None:
        data = load_fut_prices()
    try:
        ticker = fut_ticker(tenor, expiry_monthlike, expiry_type, use_single_digit_year=False)
        timeseries = data[ticker]['PX_LAST'].dropna()
    except KeyError:
        # Maybe case of ticker being in current year - use single digit year in ticker
        ticker = fut_ticker(tenor, expiry_monthlike, expiry_type, use_single_digit_year=True)
        timeseries = data[ticker]['PX_LAST'].dropna()
    return timeseries.loc[trade_date]
