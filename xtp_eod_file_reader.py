import pandas as pd
from pandas.errors import EmptyDataError
import re
from treasury_futures_reader import CODE_EXPMONTH_DICT
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from options_futures_expirations_v3 import BUSDAY_OFFSET

XTP_FILEDIR_TEMPLATE = 'P:/PrdDevSharedDB/CME Data/{}y/XTP_Sourced_Data/'
XTP_FILENAME_TEMPLATE = 'OZN_settlement_{}.txt'
NOTATION_ADJUSTMENT_ZERO = 0.001
XTP_OUTPUT_FILEDIR = 'P:/PrdDevSharedDB/CME Data/10y/XTP/'


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
    # Adjust for known notation differences from CME data
    full.loc[full['Settlement'] == NOTATION_ADJUSTMENT_ZERO, 'Settlement'] = 0
    if return_full:
        return full
    else:
        # Create version with only latest price snapshot for each options series
        latest = full.groupby(['Last Trade Date', 'Put/Call', 'Strike Price']).tail(1) \
                 .reset_index(drop=True)    # Reset numerical index
        return latest


###############################################################################

if __name__ == '__main__':
    # Example script for evaluating XTP files
    USABLE_DATA_DATES = []
    PRICE_CHANGE_DATES = []  # Subset of USABLE_DATA_DATES, around half
    EMPTY_DATA_DATES = []
    MISSING_DATA_DATES = []
    trading_days = pd.date_range(start='2019-10-28', end='2020-03-25', freq=BUSDAY_OFFSET)
    for date_str in trading_days.strftime('%Y-%m-%d'):
        try:
            xtp = read_xtp_file(10, date_str, return_full=False)
            xtp_full = read_xtp_file(10, date_str, return_full=True)
            USABLE_DATA_DATES.append(date_str)
        except EmptyDataError:
            EMPTY_DATA_DATES.append(date_str)
            continue
        except FileNotFoundError:
            MISSING_DATA_DATES.append(date_str)
            continue
        # Perform operations on USABLE dates
        # # 1) Export as CME-styled data
        # xtp_indexed = xtp.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])
        # xtp_indexed.to_csv(f"{XTP_OUTPUT_FILEDIR}10y_{date_str}_EOD_latest.csv")
        # 2) Record date if settlement prices changed over snapshots
        changesum = xtp_full.groupby(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement'].diff().abs().sum()
        if changesum != 0:
            print(f"WARNING: {date_str} settlement price(s) "
                  f"changed between snapshots; sum of changes is {changesum}.")
            PRICE_CHANGE_DATES.append(date_str)
    print(f"Total trading dates: {len(trading_days)}")
    print(f"Usable data dates: {len(USABLE_DATA_DATES)}")
    print(f"Usable data dates with settlement price changes: {len(PRICE_CHANGE_DATES)}")
    print(f"File found but data empty dates ({len(EMPTY_DATA_DATES)}): {EMPTY_DATA_DATES}")
    print(f"File not found dates ({len(MISSING_DATA_DATES)}): {MISSING_DATA_DATES}")

""" Expected Output:
WARNING: 2019-10-28 settlement price(s) changed between snapshots; sum of changes is 0.15625.
WARNING: 2019-10-29 settlement price(s) changed between snapshots; sum of changes is 0.46875.
WARNING: 2019-11-04 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2019-12-06 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2019-12-12 settlement price(s) changed between snapshots; sum of changes is 1.53125.
WARNING: 2019-12-13 settlement price(s) changed between snapshots; sum of changes is 0.0625.
WARNING: 2019-12-20 settlement price(s) changed between snapshots; sum of changes is 0.09375.
WARNING: 2019-12-23 settlement price(s) changed between snapshots; sum of changes is 0.078125.
WARNING: 2019-12-27 settlement price(s) changed between snapshots; sum of changes is 0.09375.
WARNING: 2020-01-06 settlement price(s) changed between snapshots; sum of changes is 0.09375.
WARNING: 2020-01-07 settlement price(s) changed between snapshots; sum of changes is 0.09375.
WARNING: 2020-01-08 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2020-01-09 settlement price(s) changed between snapshots; sum of changes is 0.328125.
WARNING: 2020-01-10 settlement price(s) changed between snapshots; sum of changes is 0.125.
WARNING: 2020-01-16 settlement price(s) changed between snapshots; sum of changes is 0.28125.
WARNING: 2020-01-21 settlement price(s) changed between snapshots; sum of changes is 0.0625.
WARNING: 2020-01-27 settlement price(s) changed between snapshots; sum of changes is 0.21875.
WARNING: 2020-01-28 settlement price(s) changed between snapshots; sum of changes is 0.28125.
WARNING: 2020-01-29 settlement price(s) changed between snapshots; sum of changes is 0.015625.
WARNING: 2020-01-31 settlement price(s) changed between snapshots; sum of changes is 0.5.
WARNING: 2020-02-03 settlement price(s) changed between snapshots; sum of changes is 0.21875.
WARNING: 2020-02-04 settlement price(s) changed between snapshots; sum of changes is 0.421875.
WARNING: 2020-02-05 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2020-02-06 settlement price(s) changed between snapshots; sum of changes is 0.125.
WARNING: 2020-02-07 settlement price(s) changed between snapshots; sum of changes is 0.09375.
WARNING: 2020-02-13 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2020-02-14 settlement price(s) changed between snapshots; sum of changes is 0.1875.
WARNING: 2020-02-18 settlement price(s) changed between snapshots; sum of changes is 0.0625.
WARNING: 2020-02-28 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2020-03-02 settlement price(s) changed between snapshots; sum of changes is 0.0625.
WARNING: 2020-03-03 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2020-03-10 settlement price(s) changed between snapshots; sum of changes is 0.03125.
WARNING: 2020-03-12 settlement price(s) changed between snapshots; sum of changes is 1.234375.
WARNING: 2020-03-16 settlement price(s) changed between snapshots; sum of changes is 2.75.
WARNING: 2020-03-17 settlement price(s) changed between snapshots; sum of changes is 1.9375.
WARNING: 2020-03-19 settlement price(s) changed between snapshots; sum of changes is 32.328125.
WARNING: 2020-03-20 settlement price(s) changed between snapshots; sum of changes is 4.15625.
WARNING: 2020-03-23 settlement price(s) changed between snapshots; sum of changes is 161.796875.
WARNING: 2020-03-25 settlement price(s) changed between snapshots; sum of changes is 212.46875.
Total trading dates: 103
Usable data dates: 78
Usable data dates with settlement price changes: 39
File found but data empty dates (3): ['2020-01-17', '2020-02-24', '2020-03-06']
File not found dates (22): ['2019-11-01', '2019-11-05', '2019-11-06', '2019-11-07', '2019-11-08', '2019-11-11', '2019-11-12', '2019-11-13', '2019-11-14', '2019-11-15', '2019-11-18', '2019-11-19', '2019-11-20', '2019-11-21', '2019-11-22', '2019-11-25', '2019-11-26', '2019-11-27', '2019-11-29', '2019-12-02', '2019-12-03', '2019-12-04']
"""
