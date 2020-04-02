import pandas as pd
import pdblp
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from options_futures_expirations_v3 import undl_fut_quarter_month

BLOOMBERG_PULLS_FILEDIR = 'P:/PrdDevSharedDB/BBG Pull Scripts/'
TREASURY_FUT_CSV_FILENAME = 'treasury_futures_pull.csv'
TENOR_CODE_DICT = {2: 'TU', 5: 'FV', 10: 'TY', 30: 'US'}
CODE_TENOR_DICT = {'TU': 2, 'FV': 5, 'TY': 10, 'US': 30}
EXPMONTH_CODE_DICT = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                      7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
CODE_EXPMONTH_DICT = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                      'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
QUARTER_CODE_LIST = ['H', 'M', 'U', 'Z']


def fut_ticker(tenor, expiry_monthlike, expiry_type='options', use_single_digit_year=False, no_comdty=False):
    """ Derive Bloomberg Treasury futures ticker from expiry of options or futures
    :param tenor: 2, 5, 10, 30, etc. (-year Treasury futures)
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month)
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param use_single_digit_year: set True to return single-digit year, used when querying for current year
    :param no_comdty: set True to omit the Bloomberg-specific ' Comdty' from the ticker
    :return: string Bloomberg ticker; e.g. 'TYM18 Comdty' for 10-year June futures in 2018
    """
    tenor_code = TENOR_CODE_DICT[tenor]
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
    ticker = tenor_code + quarter_code + year_code
    if not no_comdty:
        ticker += ' Comdty'
    return ticker


def reverse_fut_ticker(ticker, decade_helper=None, is_single_digit_year=False, no_comdty=False):
    """ Reverse fut_ticker - derive tenor and expiry year-month from Bloomberg futures ticker
    :param ticker: string Bloomberg ticker; e.g. 'TYM18 Comdty', 'FVZ9'
    :param decade_helper: ensure accuracy of expiry year by providing the correct decade;
                          this is ideal as Bloomberg tickers only provide one or two digits for year
    :param is_single_digit_year: set True if ticker only includes one digit for year (False indicates two)
    :param no_comdty: set True if ticker omits ' Comdty'
    :return: (numerical tenor, string expiry year-month); e.g. (10, '2020-03')
    """
    if not no_comdty:
        ticker = ticker.split()[0]  # Omit ' Comdty' part of ticker
    tenor = CODE_TENOR_DICT[ticker[:2]]
    expiry_month_num = CODE_EXPMONTH_DICT[ticker[2]]
    expiry_year_num = int(ticker[3:])
    if decade_helper is None:
        decade_helper = pd.Timestamp('now').year    # Use current year as reference; not ideal
    if is_single_digit_year:
        expiry_year_num += decade_helper - (decade_helper % 10)
    else:
        expiry_year_num += decade_helper - (decade_helper % 100)
    return tenor, f'{expiry_year_num}-{expiry_month_num:02d}'


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


def reformat_pdblp_ticker_field_value(tfv_format, ticker_index=None):
    """ Reformat pdblp real-time output DataFrame format into something more readable
    :param tfv_format: DataFrame with numerical index (unindexed) and 'ticker',
                       'field', and 'value' columns
    :param ticker_index: desired ordering of tickers in the output, reformatted DataFrame
    :return: pd.DataFrame with 'ticker' index and columns named after fields
    """
    if tfv_format.empty:
        print("WARNING: reformat_pdblp_ticker_field_value() input empty!")
        return tfv_format.copy()
    fields = tfv_format['field'].unique()
    fields_dict = {}
    for field in fields:
        fields_dict[field] = (tfv_format[tfv_format['field'] == field]
                              .drop('field', axis=1).set_index('ticker')['value'])
    result_df = pd.DataFrame(fields_dict, index=ticker_index)
    result_df.index.name = 'ticker'     # Important when exporting to CSV
    return result_df


def reformat_pdblp_bdh(bdh_format, ticker_index=None, squeeze=False):
    """ Reformat pdblp historical output DataFrame format into something more readable
    :param bdh_format: DataFrame with index of 'date', wherein each date contains a DataFrame
                       that is comparable to the tfv_format of reformat_pdblp_ticker_field_value()
    :param ticker_index: desired ordering of tickers in the output, reformatted DataFrame
    :param squeeze: set True to drop 'date' index in the output, reformatted DataFrame if
                    and only if input DataFrame contains only one date's worth of data
    :return: pd.DataFrame with 'date' index, wherein each date contains a DataFrame with
             'ticker' index and columns named after fields
    """
    if bdh_format.empty:
        print("WARNING: reformat_pdblp_bdh() input empty!")
        return bdh_format.copy()
    bdh_days = bdh_format.index
    bdh_days_list = []
    for bdh_day in bdh_days:
        # Model each date's data into "tfv_format" to use reformat_pdblp_ticker_field_value()
        tfv_format = bdh_format.loc[bdh_day].reset_index().rename({bdh_day: 'value'}, axis=1)
        bdh_day_format = (reformat_pdblp_ticker_field_value(tfv_format, ticker_index=ticker_index)
                          .reset_index().assign(date=bdh_day).set_index(['date', 'ticker']))
        bdh_days_list.append(bdh_day_format)
    result_df = pd.concat(bdh_days_list)
    if len(bdh_days) == 1 and squeeze:
        return result_df.reset_index('date', drop=True)
    else:
        return result_df


def reformat_pdblp(pdblp_result, ticker_index=None, is_bdh=False, squeeze=False):
    """ Reformat pdblp output into something more readable - combination of
        reformat_pdblp_ticker_field_value() and reformat_pdblp_bdh()
    :param pdblp_result: resulting DataFrame of a pdblp Bloomberg query
    :param ticker_index: desired ordering of tickers in the output, reformatted DataFrame
    :param is_bdh: set True to indicate that pdblp data is BDH (historical daily data; con.bdh);
                   set False to indicate that it is BDP (real-time snapshot data; con.ref)
    :param squeeze: only applicable if pdblp data is BDH (i.e. is_bdh=True);
                    set True to drop 'date' index in the output, reformatted DataFrame if
                    and only if input DataFrame contains only one date's worth of data
    :return: pd.DataFrame with 'date' index (if BDH), wherein each date contains a DataFrame
             with 'ticker' index and columns named after fields
    """
    if pdblp_result.empty:
        print("WARNING: reformat_pdblp() input empty!")
        return pdblp_result.copy()
    if is_bdh:
        result_df = reformat_pdblp_bdh(pdblp_result, ticker_index=ticker_index, squeeze=squeeze)
    else:
        result_df = reformat_pdblp_ticker_field_value(pdblp_result, ticker_index=ticker_index)
    return result_df


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
    for tenor_code in TENOR_CODE_DICT.values():
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
