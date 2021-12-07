import pandas as pd
import numpy as np
from collections.abc import Iterable
import pdblp
from options_futures_expirations_v3 import datelike_to_timestamp, next_month_first_day, \
                                           next_quarterly_month, undl_fut_quarter_month, \
                                           generate_expiries, BUSDAY_OFFSET, get_maturity_status, third_friday

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


def fut_ticker(fut_code, expiry_monthlike, expiry_type='futures', contract_cycle='monthly',
               use_single_digit_year=False, product_type=None, verbose=True):
    """ Derive Bloomberg Treasury futures ticker from expiry of options or futures
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month)
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param contract_cycle: only relevant when expiry_type='options'; options are almost certainly available monthly,
                           while underlying futures may not be, e.g. Treasury options deliver quarterly futures;
                           for this very specific use case, expiry_monthlike may be set to options expiry, with
                           expiry_type='options' and contract_cycle='quarterly', to return quarterly futures ticker
    :param use_single_digit_year: set True to return single-digit year, used when querying for current year
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.;
                         set None to just omit the product keyword
    :param verbose: set True for explicit print statements
    :return: string Bloomberg ticker; e.g. 'TYM18 Comdty' for 10-year June futures in 2018
    """
    expiry_month = datelike_to_timestamp(expiry_monthlike)
    if expiry_type == 'options':
        # Options on futures assumed to expire in month before futures maturity
        next_month_and_year = expiry_month + pd.DateOffset(months=1)
        contract_year = next_month_and_year.year
        contract_month = next_month_and_year.month
    elif expiry_type == 'futures':
        contract_year = expiry_month.year
        contract_month = expiry_month.month
    else:
        raise ValueError("expiry_type must be either 'options' or 'futures'")
    if contract_cycle == 'monthly':
        month_code = EXPMONTH_CODE_DICT[contract_month]
    elif contract_cycle == 'quarterly':
        if verbose and expiry_type == 'futures':
            print("WARNING: 'futures' and contract_cycle specified; this is redundant as futures maturity is given"
                  "         in expiry_monthlike, so please verify this is intentional and not a misunderstanding")
        month_code = EXPMONTH_CODE_DICT[undl_fut_quarter_month(contract_month)]     # Only quarterly months!
    else:
        raise ValueError("contract_cycle must be either 'monthly' or 'quarterly'")
    if use_single_digit_year:
        year_code = f'{contract_year%10}'   # One digit only, useful for current year queries
    else:
        year_code = f'{contract_year%100:02d}'  # Two digits, useful for past years
    ticker = fut_code + month_code + year_code
    if product_type is not None:
        ticker += f' {product_type}'
    return ticker


def reverse_fut_ticker(ticker, decade_helper=None, is_single_digit_year=None, has_product_type=True):
    """ Reverse fut_ticker - derive futures code and expiry year-month from Bloomberg futures ticker
    :param ticker: string Bloomberg ticker; e.g. 'TYM18 Comdty', 'FVZ9'
    :param decade_helper: ensure accuracy of expiry year by providing the correct decade;
                          this is ideal as Bloomberg tickers only provide one or two digits for year
    :param is_single_digit_year: set True if ticker only includes one digit for year (False indicates two);
                                 set None to automatically try 2 and do 1 iff 2 fails
    :param has_product_type: set False if ticker omits ' Comdty', etc. product keyword
    :return: (string futures code, string expiry year-month); e.g. ('TY', '2020-03')
    """
    if has_product_type:
        ticker = ticker.split()[0]  # Omit ' Comdty' part of ticker
    if is_single_digit_year is None:
        # Try 2 digits, it will intelligently default to 1 digit if it doesn't work
        n_year_digits, is_single_digit_year = 2, False
    else:
        n_year_digits = 1 if is_single_digit_year else 2
    try:
        expiry_year_num = int(ticker[-n_year_digits:])
    except ValueError as e:
        print(f"Reading {n_year_digits} digit year didn't work; retrying assuming 1 digit year...\n"
              f"\t(Exception that was caught: <{e}>)")
        # "Invalid literal for int()" likely means we tried 2 digits and it's actually 1
        n_year_digits, is_single_digit_year = 1, True
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


def _create_futures_ticker_list_single_fut_code(
        fut_code, start_datelike, end_datelike=None, end_year_current=True,
        n_maturities_past_end=3, contract_cycle='quarterly', product_type='Comdty', verbose=True):
    """ Generate list of relevant futures tickers for use with Bloomberg
        NOTE: single-element version; fut_code cannot be an iterable
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date; set None for present day
    :param end_year_current: set True to treat end date's year as current year; important because Bloomberg
                             futures tickers have single-digit year format for current year (rather than double)
    :param n_maturities_past_end: number of current maturities (after price end date) to query for
    :param contract_cycle: 'quarterly' or 'monthly'
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.
    :param verbose: set True for explicit print statements
    :return: list of futures tickers
    """
    # Determine start and end dates for price pull
    start_date = datelike_to_timestamp(start_datelike)
    if end_datelike is None:
        end_date = pd.Timestamp('now').normalize()
        if verbose:
            print(f"End date inferred to be {end_date.strftime('%Y-%m-%d')}")
    else:
        end_date = datelike_to_timestamp(end_datelike)

    # Create list of all futures Bloomberg tickers in use between start and end dates
    ticker_list = []  # Master list
    # 1) Determine set of months in the cycle
    # (e.g. Treasury futures are only listed quarterly; 1-month SOFR is listed monthly)
    if contract_cycle == 'quarterly':
        month_code_list = QUARTER_CODE_LIST
    elif contract_cycle == 'monthly':
        month_code_list = MONTHLY_CODE_LIST
    else:
        raise ValueError(f"contract_cycle must be 'quarterly' or 'monthly'")
    if verbose:
        print(f"'{contract_cycle}' cycle containing letters {month_code_list} will be used")
    # 2) Determine cycle months in first and last year (probably won't have complete years)
    # NOTE: cutting off at the end month is not crucial, since futures usually extend forward many months
    start_month_code = EXPMONTH_CODE_DICT[start_date.month]
    start_month_idx = np.searchsorted(month_code_list, start_month_code)
    end_month_code = EXPMONTH_CODE_DICT[end_date.month]
    end_month_idx = np.searchsorted(month_code_list, end_month_code) + 1  # +1 to be inclusive of end month
    # 3) Generate tickers as appropriate to very specific situation
    product_code = f' {product_type}'  # Product type is static
    if start_date.year == end_date.year:
        # Simple case: only pulling futures within a year
        if end_year_current:
            year_code = f'{end_date.year % 10}'  # Alter year code to single digit
        else:
            year_code = f'{end_date.year % 100:02d}'  # Use double digit year code as with historical years
        for month_code in month_code_list[start_month_idx:end_month_idx]:
            ticker = fut_code + month_code + year_code + product_code
            ticker_list.append(ticker)
        if verbose:
            print(f"Simple base case: all price dates within one year;\n\t{len(ticker_list)} tickers: {ticker_list}")
    else:
        # Complex case: pulling futures across multiple years
        # First year: cycle months limited by start date
        year_code = f'{start_date.year % 100:02d}'
        for month_code in month_code_list[start_month_idx:]:
            ticker = fut_code + month_code + year_code + product_code
            ticker_list.append(ticker)
        # Middle years (if any): all cycle months
        for year in range(start_date.year + 1, end_date.year):
            year_code = f'{year % 100:02d}'
            for month_code in month_code_list:
                ticker = fut_code + month_code + year_code + product_code
                ticker_list.append(ticker)
        # Final year: year code potentially single digit, cycle months limited by end date
        if end_year_current:
            year_code = f'{end_date.year % 10}'  # Alter year code to single digit
        else:
            year_code = f'{end_date.year % 100:02d}'  # Use double digit year code as with historical years
        for month_code in month_code_list[:end_month_idx]:
            ticker = fut_code + month_code + year_code + product_code
            ticker_list.append(ticker)
        if verbose:
            print(f"Complex base case: price dates span multiple years;\n\t{len(ticker_list)} tickers: {ticker_list}")

    # Add additional "current" maturities to the list
    if n_maturities_past_end > 0:
        additional_months = []
        if contract_cycle == 'quarterly':
            # Obtain last quarterly month already included, then go further
            most_recent_included = next_quarterly_month(end_date, quarter_return_self=True)
            upcoming_not_included = next_quarterly_month(most_recent_included)
            for additional_mat in range(0, n_maturities_past_end):
                additional_months.append(upcoming_not_included)
                upcoming_not_included = next_quarterly_month(upcoming_not_included)
        elif contract_cycle == 'monthly':
            upcoming_not_included = next_month_first_day(end_date)
            for additional_mat in range(0, n_maturities_past_end):
                additional_months.append(upcoming_not_included)
                upcoming_not_included = next_month_first_day(upcoming_not_included)
        additional_ticker_list = []
        for additional in additional_months:
            year_code = f'{additional.year % 10}'  # Single digit year code for futures with maturity past the present
            month_code = EXPMONTH_CODE_DICT[additional.month]
            ticker = fut_code + month_code + year_code + product_code
            additional_ticker_list.append(ticker)
        ticker_list += additional_ticker_list
        if verbose:
            print(f"{n_maturities_past_end} additional tickers past end date:\n\t{additional_ticker_list}")

    return ticker_list


def create_futures_ticker_list(fut_codes, start_datelike, end_datelike=None,
                               end_year_current=True, n_maturities_past_end=3, contract_cycle='quarterly',
                               product_type='Comdty', verbose=True):
    """ Generate list of relevant futures tickers for use with Bloomberg
        NOTE: only difference from single-element version is fut_codes can be iterable
    :param fut_codes: code(s) for the futures; e.g. 'TY', ['FV', 'SER'], ('SFR', 'IBY', 'IHB')
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date; set None for present day
    :param end_year_current: set True to treat end date's year as current year; important because Bloomberg
                             futures tickers have single-digit year format for current year (rather than double)
    :param n_maturities_past_end: number of current maturities (after price end date) to query for
    :param contract_cycle: 'quarterly' or 'monthly'
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.
    :param verbose: set True for explicit print statements
    :return: list of futures tickers
    """
    if not isinstance(fut_codes, str) and isinstance(fut_codes, Iterable):
        # Note the special handling of string - it is Iterable, but we want it as single element, not many chars
        list_of_ticker_lists = \
            [_create_futures_ticker_list_single_fut_code(fut_code, start_datelike, end_datelike,
                                                         end_year_current, n_maturities_past_end, contract_cycle,
                                                         product_type, verbose)
             for fut_code in fut_codes]
        return [ticker for ticker_list in list_of_ticker_lists for ticker in ticker_list]   # Flatten
    else:
        return _create_futures_ticker_list_single_fut_code(fut_codes, start_datelike, end_datelike,
                                                           end_year_current, n_maturities_past_end, contract_cycle,
                                                           product_type, verbose)


def pull_fut_prices(fut_codes, start_datelike, end_datelike=None, end_year_current=True,
                    n_maturities_past_end=3, contract_cycle='quarterly', product_type='Comdty',
                    bbg_flds_list=None, ticker_list=None,
                    file_dir='', file_name='temp_bbg_fut_prices.csv', bloomberg_con=None, verbose=True):
    """ Pull generic futures prices from Bloomberg Terminal and write them to disk
    :param fut_codes: code(s) for the futures; e.g. 'TY', ['FV', 'SER'], ('SFR', 'IBY', 'IHB')
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date; set None for present day
    :param end_year_current: set True to treat end date's year as current year; important because Bloomberg
                             futures tickers have single-digit year format for current year (rather than double)
    :param n_maturities_past_end: number of current maturities (after price end date) to query for
    :param contract_cycle: 'quarterly' or 'monthly'
    :param product_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.
    :param bbg_flds_list: explicit list of Bloomberg FLDS to query; not None overrides ['PX_LAST']
    :param ticker_list: explicit list of Bloomberg tickers to query; not None essentially overrides previous 4 arguments
    :param file_dir: directory to write data file; set None for current directory
    :param file_name: file name to write to file_dir
    :param bloomberg_con: active pdblp Bloomberg connection; if None, runs create_bloomberg_connection()
    :param verbose: set True for explicit print statements
    :return: pd.DataFrame with all futures prices between start and end dates, stored in matrix
    """
    # Determine start and end dates for price pull
    start_date = datelike_to_timestamp(start_datelike)
    if end_datelike is None:
        end_date = pd.Timestamp('now').normalize()
        if verbose:
            print(f"End date inferred to be {end_date.strftime('%Y-%m-%d')}")
    else:
        end_date = datelike_to_timestamp(end_datelike)

    # Determine fields to query (default is just last/settle price)
    if bbg_flds_list is None:
        bbg_flds_list = ['PX_LAST']

    # Create list of futures tickers
    if ticker_list is None:
        ticker_list = create_futures_ticker_list(fut_codes, start_date, end_date,
                                                 end_year_current, n_maturities_past_end, contract_cycle,
                                                 product_type, verbose=verbose)

    # Get last price time-series of every ticker
    bbg_start_dt = start_date.strftime('%Y%m%d')
    bbg_end_dt = end_date.strftime('%Y%m%d')
    if bloomberg_con is None:
        bloomberg_con = create_bloomberg_connection()
        must_close_con = True
        if verbose:
            print(f"New Bloomberg connection created")
    else:
        must_close_con = False
        if verbose:
            print(f"Existing Bloomberg connection given")
    try:
        fut_price_df = bloomberg_con.bdh(ticker_list, bbg_flds_list, start_date=bbg_start_dt, end_date=bbg_end_dt)
    except ValueError:
        raise ValueError(f"pull unsuccessful. here is list of tickers attempted:\n{ticker_list}")
    if must_close_con:
        bloomberg_con.stop()    # Close connection iff it was specifically made for this
        if verbose:
            print(f"New Bloomberg connection closed")

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


def get_fut_price(trade_date, fut_code, expiry_monthlike, expiry_type='futures', contract_cycle='monthly',
                  product_type='Comdty', data=None, contract_year=None, contract_month=None):
    """ Retrieve futures price from Bloomberg-exported data
        NOTE: contract_year and contract_month can be supplied together as an alternative to expiry_monthlike
    :param trade_date: trade date on which to get price
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param expiry_monthlike: date-like representation of expiration month (precision only needed to month);
                             set None to use contract_year and contract_month
    :param expiry_type: specify whether the expiry is 'options' or 'futures'
    :param contract_cycle: only relevant when expiry_type='options'; options are almost certainly available monthly,
                           while underlying futures may not be, e.g. Treasury options deliver quarterly futures;
                           for this very specific use case, expiry_monthlike may be set to options expiry, with
                           expiry_type='options' and contract_cycle='quarterly', to return quarterly futures ticker
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
        ticker = fut_ticker(fut_code, expiry_monthlike, expiry_type, contract_cycle=contract_cycle,
                            product_type=product_type, use_single_digit_year=False)
        timeseries = data[ticker]['PX_LAST'].dropna()
    except KeyError:
        # Maybe case of ticker being in current year - use single digit year in ticker
        ticker = fut_ticker(fut_code, expiry_monthlike, expiry_type, contract_cycle=contract_cycle,
                            product_type=product_type, use_single_digit_year=True)
        timeseries = data[ticker]['PX_LAST'].dropna()
    return timeseries.loc[trade_date]


def create_maturities_roll_helper_df(roll_n_before_expiry=3, maturities=None,
                                     start_datelike=None, **generate_expiries_kwargs):
    """ Generate helper DataFrame for use in rolling futures
        NOTE: function is an extension of options_futures_expirations_v3.generate_expiries();
              see its documentation, but the key is to configure the future's maturity dates
    :param roll_n_before_expiry: number of days before maturity date to roll
    :param maturities: array of future's maturity dates; set as None to use generate_expiries() functionality
    :param start_datelike: relevant iff maturities is None; mandatory start date to pass to generate_expiries()
    :param generate_expiries_kwargs:
               generate_expiries(start_datelike, end_datelike=None, n_terms=100,
                                 specific_product=None, expiry_func=third_friday)
    :return: pd.DataFrame with index 'Maturity' and columns 'Selected Roll Date',
             'Post-Roll Return Date' (after rolling to next contract, you'll want to stitch in new returns),
             'Bloomberg Stitch Date' (Bloomberg's default generic 1st and 2nd term futures roll day after maturity)
    """
    if maturities is None:
        if start_datelike is None:
            raise ValueError("Incorrect usage: must either pass in 1) maturities array or "
                             "2) start date + kwargs for use with generate_expiries()")
        else:
            maturities = generate_expiries(start_datelike, **generate_expiries_kwargs)
    maturities_df = (pd.DataFrame({'Maturity': maturities,
                                   'Selected Roll Date': maturities - roll_n_before_expiry*BUSDAY_OFFSET,
                                   'Post-Roll Return Date': maturities - (roll_n_before_expiry-1)*BUSDAY_OFFSET,
                                   'Bloomberg Stitch Date': maturities + BUSDAY_OFFSET})
                     .set_index('Maturity'))
    return maturities_df


def stitch_bloomberg_futures(gen1, gen2, maturities_df=None, specific_product=None, expiry_func=third_friday,
                             roll_n_before_expiry=3):
    """ Go through trade date history, performing rolls and Bloomberg data stitches
        Goal is to end up with:
          - a percent return history with considerate stitching
          - a scaled price history (starting at 100) for total return
          - cumulative roll cost (selling near term buying next term on each roll date)
        NOTE: as with any function dealing with maturities, several options are available for specifying them:
                -> pass in pre-created, pre-formatted maturities info (maturities_df)
                OR
                -> create formatted maturities info in this function:
                  -> pass in recognized product name (specific_product)
                  OR
                  -> pass in function coded to find maturity date (expiry_func)
    :param gen1: Bloomberg generic 1st term futures price
    :param gen2: Bloomberg generic 2nd term futures price
    :param maturities_df: helper DataFrame from create_maturities_roll_helper_df();
                          set None to create from scratch with specific_product or expiry_func
    :param specific_product: override expiry_func argument with built-in selection;
                             recognizes: 'VIX', 'SPX', 'Treasury options', 'Treasury futures 2/5/10/30', 'iBoxx', etc.
    :param expiry_func: monthly expiry function (returns expiration date given day in month)
    :param roll_n_before_expiry: number of days before maturity date to roll
    :return: pd.DataFrame detailing stitching method and 'Stitched Change', 'Scaled Price', 'Cumulative Roll Cost'
    """
    # Initialize roll_df with Bloomberg timeseries
    roll_df = pd.DataFrame({'Bloomberg 1st': gen1, 'Bloomberg 2nd': gen2})
    roll_df.index.name = 'Trade Date'
    roll_df = roll_df.dropna(how='all')  # If NaN for both terms, chances are date is not legit
    if roll_df.empty:
        raise ValueError("No usable data in input Bloomberg prices")

    if maturities_df is None:
        # Generate futures maturities and surrounding dates relevant to roll
        # NOTE: consider subtle edge case of last data date being right before a maturity - need to know that next
        #       maturity to know whether final dates in data require roll stitching
        oldest_data_date, latest_data_date = roll_df.first_valid_index(), roll_df.last_valid_index()
        _, next_relevant_expiry, _, _ = \
            get_maturity_status(latest_data_date, specific_product=specific_product, expiry_func=expiry_func,
                                side='left')    # side='left' because if latest_data_date is maturity, don't go further
        maturities_df = \
            create_maturities_roll_helper_df(start_datelike=oldest_data_date, end_datelike=next_relevant_expiry,
                                             specific_product=specific_product, expiry_func=expiry_func,
                                             roll_n_before_expiry=roll_n_before_expiry)

    # Play through each roll date and perform roll-related tasks around it and record in roll_df
    # NOTE: 1 NaN price causes 2 consecutive NaN changes - day of and day after;
    #       should never worry a perfect dataset, but beware data is never perfect
    # NOTE: subtle edge case: when data starts or ends in the middle of a roll period, loop should not throw error
    roll_df['1st Change'] = roll_df['Bloomberg 1st'].pct_change(fill_method=None)
    roll_df['2nd Change'] = roll_df['Bloomberg 2nd'].pct_change(fill_method=None)
    for maturity_date in maturities_df.index:
        task_dates = maturities_df.loc[maturity_date]
        if task_dates['Selected Roll Date'] in roll_df.index:
            # 1) Get roll "cost" - per-contract cost of buying 2nd term, selling 1st term
            roll_cost = (roll_df.loc[task_dates['Selected Roll Date'], 'Bloomberg 2nd']
                         - roll_df.loc[task_dates['Selected Roll Date'], 'Bloomberg 1st'])
            roll_df.loc[task_dates['Selected Roll Date'], 'Roll Cost'] = roll_cost  # Record to DataFrame
        # 2) Use Bloomberg 2nd term returns until reassignment of 1st and 2nd term
        post_roll_pre_stitch_returns = \
            roll_df.loc[task_dates['Post-Roll Return Date']:maturity_date, '2nd Change']
        roll_df.loc[task_dates['Post-Roll Return Date']:maturity_date, 'Stitched Change from 2nd'] = \
            post_roll_pre_stitch_returns.values     # Record to DataFrame
        if task_dates['Bloomberg Stitch Date'] in roll_df.index:
            # 3) Create and use special stitched return to account for reassignment of 1st and 2nd term
            stitch_date_return = \
                (roll_df.loc[task_dates['Bloomberg Stitch Date'], 'Bloomberg 1st']
                 - roll_df.loc[maturity_date, 'Bloomberg 2nd']) / roll_df.loc[maturity_date, 'Bloomberg 2nd']
            roll_df.loc[task_dates['Bloomberg Stitch Date'], 'Stitched Change from (1st-2nd)/2nd'] = \
                stitch_date_return  # Record to DataFrame
    no_stitch_returns_idx = (roll_df['Stitched Change from 2nd'].isna()
                             & roll_df['Stitched Change from (1st-2nd)/2nd'].isna())
    roll_df.loc[no_stitch_returns_idx, 'Stitched Change from 1st'] = \
        roll_df.loc[no_stitch_returns_idx, '1st Change'].values

    # Combine purposefully separated 3 components to create stitched percent returns
    # NOTE: overwrite order does not matter because 'Stitched Change from 1st' defined to fill gaps
    roll_df['Stitched Change'] = \
        (roll_df['Stitched Change from 2nd']
         .combine_first(roll_df['Stitched Change from (1st-2nd)/2nd'])
         .combine_first(roll_df['Stitched Change from 1st']))

    # Run stitched returns on 100 to get scaled price history
    roll_df['Scaled Price'] = (roll_df['Stitched Change'] + 1).cumprod() * 100
    roll_df.loc[roll_df.index[0], 'Scaled Price'] = 100

    # Sum roll costs
    roll_df['Cumulative Roll Cost'] = roll_df['Roll Cost'].cumsum().ffill()

    # Enforce column order - edge cases make 'Roll Cost' column move around
    roll_df_cols = ['Bloomberg 1st', 'Bloomberg 2nd', '1st Change', '2nd Change',
                    'Roll Cost', 'Stitched Change from 2nd', 'Stitched Change from (1st-2nd)/2nd',
                    'Stitched Change from 1st', 'Stitched Change', 'Scaled Price', 'Cumulative Roll Cost']
    roll_df = roll_df[roll_df_cols]

    return roll_df
