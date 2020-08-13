import pandas as pd
import numpy as np
from options_futures_expirations_v3 import datelike_to_timestamp
from treasury_futures_reader import create_bloomberg_connection, EXPMONTH_CODE_DICT, QUARTER_CODE_LIST

MONTHLY_CODE_LIST = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


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
