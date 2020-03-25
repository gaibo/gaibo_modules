import pandas as pd
import re
from treasury_futures_reader import CODE_EXPMONTH_DICT
from cboe_exchange_holidays_v3 import datelike_to_timestamp

XTP_FILEDIR_TEMPLATE = 'P:/PrdDevSharedDB/CME Data/{}y/XTP_Sourced_Data/'
XTP_FILENAME_TEMPLATE = 'OZN_settlement_{}.txt'


def read_xtp_file(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None):
    """ Read XTP EOD Treasury options prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all snapshots; set False to return only latest snap for each price
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :return: unindexed pd.DataFrame with labeled columns
    """
    if file_dir is None:
        file_dir = XTP_FILEDIR_TEMPLATE.format(tenor)
    if file_name is None:
        trade_date = datelike_to_timestamp(trade_datelike)
        file_name = XTP_FILENAME_TEMPLATE.format(trade_date.strftime('%y%m%d'))
    file = pd.read_csv(f'{file_dir}{file_name}', header=None, squeeze=True)
    file_split = file.apply(str.split)  # Split space-delimited lines into lists
    # Parse XTP-captured file to extract a sequence of price snapshots for each options series
    full = pd.DataFrame(
        {'Last Trade Date': pd.to_datetime(
            file_split.map(lambda fldlst: f'20{fldlst[0][4:6]}-{CODE_EXPMONTH_DICT[fldlst[0][3]]}-{fldlst[0][7:9]}')),
         'Put/Call': file_split.map(lambda fldlst: fldlst[0][9]),
         'Strike Price': file_split.map(lambda fldlst: fldlst[0][11:]).astype(float),
         'Snapshot Time': pd.to_datetime(
             file_split.map(lambda fldlst: re.split('[_.]', file_name)[2]+' '+fldlst[3]), yearfirst=True),
         'SettlPriceType (Bitmap)': file_split.map(lambda fldlst: fldlst[-1]).astype(int),
         'Settlement': file_split.map(lambda fldlst: fldlst[11]).astype(float)}
    ).sort_values(['Last Trade Date', 'Put/Call', 'Strike Price', 'Snapshot Time', 'SettlPriceType (Bitmap)']) \
     .reset_index(drop=True)     # Reset numerical index after sorting
    if return_full:
        return full
    else:
        # Create version with only latest price snapshot for each options series
        latest = full.groupby(['Last Trade Date', 'Put/Call', 'Strike Price']).tail(1) \
                 .reset_index(drop=True)    # Reset numerical index
        return latest


###############################################################################

if __name__ == '__main__':
    # Check for changes between snapshots, which may prove to be problematic
    test_full = read_xtp_file(10, '2020-03-03', return_full=True)
    changesum = test_full.groupby(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement'].diff().abs().sum()
    if changesum != 0:
        print(f"WARNING: SETTLEMENT PRICE(S) CHANGED BETWEEN SNAPSHOTS: {changesum}.")
