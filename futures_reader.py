import pandas as pd
from options_futures_expirations_v3 import datelike_to_timestamp
from treasury_futures_reader import create_bloomberg_connection, QUARTER_CODE_LIST

MONTHLY_CODE_LIST = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


def pull_fut_prices(fut_code, start_datelike, end_datelike=None,
                    contract_cycle='quarterly', bloomberg_type='Comdty',
                    bloomberg_con=None, file_dir='', file_name='temp_bbg_fut_prices.csv'):
    """ Pull generic futures prices from Bloomberg Terminal and write them to disk
    :param fut_code: code for the futures; e.g. 'TY', 'FV', 'SER', 'SFR', 'IBY', 'IHB'
    :param start_datelike: date-like representation of start date
    :param end_datelike: date-like representation of end date; set None for present day
    :param contract_cycle: 'quarterly' or 'monthly'
    :param bloomberg_type: Bloomberg futures are usually 'Comdty', but sometimes 'Index', etc.
    :param bloomberg_con: active pdblp Bloomberg connection; if None, runs create_bloomberg_connection()
    :param file_dir: directory to write data file; set None for current directory
    :param file_name: exact file name to write to file_dir
    :return: pd.DataFrame with all futures prices between start and end dates, stored in matrix
    """
    start_date = datelike_to_timestamp(start_datelike)
    if end_datelike is None:
        end_date = pd.Timestamp('now').normalize()
    else:
        end_date = datelike_to_timestamp(end_datelike)
    # Create list of all futures Bloomberg tickers in use between start and end dates
    ticker_list = []    # Master list to add to
    if contract_cycle == 'quarterly':
        cycle_code_list = QUARTER_CODE_LIST
    elif contract_cycle == 'monthly':
        cycle_code_list = MONTHLY_CODE_LIST
    else:
        raise ValueError(f"contract_cycle must be 'quarterly' or 'monthly'")
    for year in range(start_date.year, end_date.year):
        # For all years up to but not including current year of end_date
        for cycle_code in cycle_code_list:
            ticker = fut_code + cycle_code + f'{year%100:02d}' + f' {bloomberg_type}'
            ticker_list.append(ticker)
    # For current year of end_date - need single (rather than double) digit year
    for cycle_code in cycle_code_list:
        ticker = fut_code + cycle_code + f'{end_date.year%10}' + f' {bloomberg_type}'
        ticker_list.append(ticker)
    # Get last price time-series of each ticker
    bbg_start_dt = start_date.strftime('%Y%m%d')
    bbg_end_dt = end_date.strftime('%Y%m%d')
    if bloomberg_con is None:
        bloomberg_con = create_bloomberg_connection()
        must_close_con = True
    else:
        must_close_con = False
    while True:
        try:
            fut_price_df = bloomberg_con.bdh(ticker_list, 'PX_LAST', start_date=bbg_start_dt, end_date=bbg_end_dt)
        except ValueError:
            popped_ticker = ticker_list.pop(0)
            print(f"Price pull failed; removing '{popped_ticker}' and trying again...")
            continue
        break  # Pull was successful
    if must_close_con:
        bloomberg_con.stop()    # Close connection iff it was specifically made for this
    # Export
    fut_price_df.to_csv(file_dir + file_name)
    return fut_price_df
