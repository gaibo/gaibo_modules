import pandas as pd
import numpy as np
import pdblp
from options_futures_expirations_v3 import datelike_to_timestamp

BLOOMBERG_PULLS_FILEDIR = 'P:/PrdDevSharedDB/BBG Pull Scripts/'
TREASURY_FUT_CSV_FILENAME = 'treasury_futures_pull.csv'
SOFR_1_MONTH_FUT_CSV_FILENAME = 'sofr_1_month_futures_pull.csv'
SOFR_3_MONTH_FUT_CSV_FILENAME = 'sofr_3_month_futures_pull.csv'
EXPMONTH_CODE_DICT = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                      7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
CODE_EXPMONTH_DICT = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                      'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
QUARTER_CODE_LIST = ['H', 'M', 'U', 'Z']
MONTHLY_CODE_LIST = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


def fut_ticker(fut_code, expiry_monthlike, expiry_type='futures', use_single_digit_year=False, product_type=None):
    """ Derive Bloomberg Treasury futures ticker from expiry of options or futures
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month)
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param use_single_digit_year: set True to return single-digit year, used when querying for current year
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.;
                         set None to just omit the product keyword
    :return: string Bloomberg ticker; e.g. 'TYM18 Comdty' for 10-year June futures in 2018
    """
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
    month_code = EXPMONTH_CODE_DICT[contract_month]
    if use_single_digit_year:
        year_code = f'{contract_year%10}'   # One digit only, useful for current year queries
    else:
        year_code = f'{contract_year%100:02d}'  # Two digits, useful for past years
    ticker = fut_code + month_code + year_code
    if product_type is not None:
        ticker += f' {product_type}'
    return ticker


def reverse_fut_ticker(ticker, decade_helper=None, is_single_digit_year=False, has_product_type=True):
    """ Reverse fut_ticker - derive futures code and expiry year-month from Bloomberg futures ticker
    :param ticker: string Bloomberg ticker; e.g. 'TYM18 Comdty', 'FVZ9'
    :param decade_helper: ensure accuracy of expiry year by providing the correct decade;
                          this is ideal as Bloomberg tickers only provide one or two digits for year
    :param is_single_digit_year: set True if ticker only includes one digit for year (False indicates two)
    :param has_product_type: set False if ticker omits ' Comdty', etc. product keyword
    :return: (string futures code, string expiry year-month); e.g. ('TY', '2020-03')
    """
    if has_product_type:
        ticker = ticker.split()[0]  # Omit ' Comdty' part of ticker
    n_year_digits = 1 if is_single_digit_year else 2
    expiry_year_num = int(ticker[-n_year_digits:])
    expiry_month_idx = -n_year_digits - 1
    expiry_month_num = CODE_EXPMONTH_DICT[ticker[expiry_month_idx]]
    fut_code = ticker[:expiry_month_idx]
    if decade_helper is None:
        decade_helper = pd.Timestamp('now').year    # Use current year as reference; not ideal
    if is_single_digit_year:
        expiry_year_num += decade_helper - (decade_helper % 10)
    else:
        expiry_year_num += decade_helper - (decade_helper % 100)
    return fut_code, f'{expiry_year_num}-{expiry_month_num:02d}'


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


def pull_fut_prices(fut_code, start_datelike, end_datelike=None, end_year_current=True,
                    contract_cycle='quarterly', product_type='Comdty',
                    bloomberg_con=None, file_dir='', file_name='temp_bbg_fut_prices.csv'):
    """ Pull generic futures prices from Bloomberg Terminal and write them to disk
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date; set None for present day
    :param end_year_current: set True to treat end date's year as current year; important because Bloomberg
                             futures tickers have single-digit year format for current year (rather than double)
    :param contract_cycle: 'quarterly' or 'monthly'
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.
    :param bloomberg_con: active pdblp Bloomberg connection; if None, runs create_bloomberg_connection()
    :param file_dir: directory to write data file; set None for current directory
    :param file_name: file name to write to file_dir
    :return: pd.DataFrame with all futures prices between start and end dates, stored in matrix
    """
    # Determine start and end dates for price pull
    start_date = datelike_to_timestamp(start_datelike)
    if end_datelike is None:
        end_date = pd.Timestamp('now').normalize()
    else:
        end_date = datelike_to_timestamp(end_datelike)

    # Create list of all futures Bloomberg tickers in use between start and end dates
    ticker_list = []    # Master list
    # 1) Determine set of months in the cycle
    # (e.g. Treasury futures are only listed quarterly; 1-month SOFR is listed monthly)
    if contract_cycle == 'quarterly':
        month_code_list = QUARTER_CODE_LIST
    elif contract_cycle == 'monthly':
        month_code_list = MONTHLY_CODE_LIST
    else:
        raise ValueError(f"contract_cycle must be 'quarterly' or 'monthly'")
    # 2) Determine cycle months in first and last year (probably won't have complete years)
    # NOTE: cutting off at the end month is not crucial, since futures usually extend forward many months
    start_month_code = EXPMONTH_CODE_DICT[start_date.month]
    start_month_idx = np.searchsorted(month_code_list, start_month_code)
    end_month_code = EXPMONTH_CODE_DICT[end_date.month]
    end_month_idx = np.searchsorted(month_code_list, end_month_code) + 1    # +1 to be inclusive of end month
    # 3) Generate tickers as appropriate to very specific situation
    product_code = f' {product_type}'   # Product type is static
    if start_date.year == end_date.year:
        # Simple case: only pulling futures within a year
        if end_year_current:
            year_code = f'{end_date.year%10}'   # Alter year code to single digit
        else:
            year_code = f'{end_date.year%100:02d}'  # Use double digit year code as with historical years
        for month_code in month_code_list[start_month_idx:end_month_idx]:
            ticker = fut_code + month_code + year_code + product_code
            ticker_list.append(ticker)
    else:
        # Complex case: pulling futures across multiple years
        # First year: cycle months limited by start date
        year_code = f'{start_date.year%100:02d}'
        for month_code in month_code_list[start_month_idx:]:
            ticker = fut_code + month_code + year_code + product_code
            ticker_list.append(ticker)
        # Middle years (if any): all cycle months
        for year in range(start_date.year+1, end_date.year):
            year_code = f'{year%100:02d}'
            for month_code in month_code_list:
                ticker = fut_code + month_code + year_code + product_code
                ticker_list.append(ticker)
        # Final year: year code potentially single digit, cycle months limited by end date
        if end_year_current:
            year_code = f'{end_date.year%10}'   # Alter year code to single digit
        else:
            year_code = f'{end_date.year%100:02d}'  # Use double digit year code as with historical years
        for month_code in month_code_list[:end_month_idx]:
            ticker = fut_code + month_code + year_code + product_code
            ticker_list.append(ticker)

    # Get last price time-series of every ticker
    bbg_start_dt = start_date.strftime('%Y%m%d')
    bbg_end_dt = end_date.strftime('%Y%m%d')
    if bloomberg_con is None:
        bloomberg_con = create_bloomberg_connection()
        must_close_con = True
    else:
        must_close_con = False
    fut_price_df = bloomberg_con.bdh(ticker_list, 'PX_LAST', start_date=bbg_start_dt, end_date=bbg_end_dt)
    if must_close_con:
        bloomberg_con.stop()    # Close connection iff it was specifically made for this

    # Export and return results matrix
    fut_price_df.to_csv(file_dir + file_name)
    return fut_price_df


def load_fut_prices(file_dir='', file_name='temp_bbg_fut_prices.csv'):
    """ Read Bloomberg futures prices matrix from disk and load them into DataFrame
    :param file_dir: directory to search for data file
    :param file_name: file name to load from file_dir
    :return: pd.DataFrame with Treasury futures prices
    """
    return pd.read_csv(file_dir + file_name, index_col=0, parse_dates=True, header=[0, 1])


def get_fut_price(trade_date, fut_code, expiry_monthlike, expiry_type='futures', product_type='Comdty',
                  data=None, contract_year=None, contract_month=None):
    """ Retrieve futures price from Bloomberg-exported data
        NOTE: contract_year and contract_month can be supplied together as an alternative to expiry_monthlike
    :param trade_date: trade date on which to get price
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month);
                             set None to use contract_year and contract_month
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.
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
        ticker = fut_ticker(fut_code, expiry_monthlike, expiry_type, product_type=product_type,
                            use_single_digit_year=False)
        timeseries = data[ticker]['PX_LAST'].dropna()
    except KeyError:
        # Maybe case of ticker being in current year - use single digit year in ticker
        ticker = fut_ticker(fut_code, expiry_monthlike, expiry_type, product_type=product_type,
                            use_single_digit_year=True)
        timeseries = data[ticker]['PX_LAST'].dropna()
    return timeseries.loc[trade_date]
