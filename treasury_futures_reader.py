from futures_reader import BLOOMBERG_PULLS_FILEDIR, TREASURY_FUT_CSV_FILENAME
import futures_reader

TENOR_CODE_DICT = {2: 'TU', 5: 'FV', 10: 'TY', 30: 'US'}
CODE_TENOR_DICT = {'TU': 2, 'FV': 5, 'TY': 10, 'US': 30}


def fut_ticker(tenor, expiry_monthlike, expiry_type='options', use_single_digit_year=False, no_comdty=False):
    """ Derive Bloomberg Treasury futures ticker from expiry of options or futures
    :param tenor: 2, 5, 10, 30, etc. (-year Treasury futures)
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month)
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param use_single_digit_year: set True to return single-digit year, used when querying for current year
    :param no_comdty: set True to omit the Bloomberg-specific ' Comdty' from the ticker
    :return: string Bloomberg ticker; e.g. 'TYM18 Comdty' for 10-year June futures in 2018
    """
    return futures_reader.fut_ticker(fut_code=TENOR_CODE_DICT[tenor],
                                     expiry_monthlike=expiry_monthlike,
                                     expiry_type=expiry_type,
                                     use_single_digit_year=use_single_digit_year,
                                     product_type=(None if no_comdty else 'Comdty'))


def reverse_fut_ticker(ticker, decade_helper=None, is_single_digit_year=False, no_comdty=False):
    """ Reverse fut_ticker - derive tenor and expiry year-month from Bloomberg futures ticker
    :param ticker: string Bloomberg ticker; e.g. 'TYM18 Comdty', 'FVZ9'
    :param decade_helper: ensure accuracy of expiry year by providing the correct decade;
                          this is ideal as Bloomberg tickers only provide one or two digits for year
    :param is_single_digit_year: set True if ticker only includes one digit for year (False indicates two)
    :param no_comdty: set True if ticker omits ' Comdty'
    :return: (numerical tenor, string expiry year-month); e.g. (10, '2020-03')
    """
    futcode_exp_result = futures_reader.reverse_fut_ticker(ticker=ticker,
                                                           decade_helper=decade_helper,
                                                           is_single_digit_year=is_single_digit_year,
                                                           has_product_type=(False if no_comdty else True))
    return CODE_TENOR_DICT[futcode_exp_result[0]], futcode_exp_result[1]


def pull_fut_prices(start_datelike, end_datelike=None, end_year_current=True, n_maturities_past_end=3,
                    file_dir=BLOOMBERG_PULLS_FILEDIR, file_name=TREASURY_FUT_CSV_FILENAME,
                    bloomberg_con=None, verbose=True):
    """ Pull Treasury futures prices from Bloomberg Terminal and write them to disk
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date
    :param end_year_current: set True to treat end date's year as current year; important because Bloomberg
                             futures tickers have single-digit year format for current year (rather than double)
    :param n_maturities_past_end: number of current maturities (after price end date) to query for
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param bloomberg_con: active pdblp Bloomberg connection; if None, runs create_bloomberg_connection()
    :param verbose: set True for explicit print statements
    :return: pd.DataFrame with all Treasury futures prices between start and end dates
    """
    return futures_reader.pull_fut_prices(
               fut_codes=TENOR_CODE_DICT.values(), start_datelike=start_datelike, end_datelike=end_datelike,
               end_year_current=end_year_current, n_maturities_past_end=n_maturities_past_end,
               contract_cycle='quarterly', product_type='Comdty', file_dir=file_dir, file_name=file_name,
               bloomberg_con=bloomberg_con, verbose=verbose)


def load_fut_prices(file_dir=BLOOMBERG_PULLS_FILEDIR, file_name=TREASURY_FUT_CSV_FILENAME):
    """ Read Treasury futures prices from disk and load them into DataFrame
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :return: pd.DataFrame with Treasury futures prices
    """
    return futures_reader.load_fut_prices(file_dir, file_name)


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
    return futures_reader.get_fut_price(trade_date, fut_code=TENOR_CODE_DICT[tenor], expiry_monthlike=expiry_monthlike,
                                        expiry_type=expiry_type, product_type='Comdty', data=data,
                                        contract_year=contract_year, contract_month=contract_month)
