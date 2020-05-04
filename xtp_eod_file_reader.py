import pandas as pd
from pandas.errors import EmptyDataError
import numpy as np
import re
from treasury_futures_reader import CODE_EXPMONTH_DICT
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from options_futures_expirations_v3 import BUSDAY_OFFSET
from cme_eod_file_reader import read_cme_file

XTP_FILEDIR_TEMPLATE = 'P:/PrdDevSharedDB/CME Data/JERRY INTERIM XTP/{}y/'
XTP_FILENAME_TEMPLATE = 'OZN_settlement_{}.txt'
XTP_OUTPUT_FILEDIR = 'P:/PrdDevSharedDB/CME Data/JERRY INTERIM XTP/10y/Formatted/'
ADJUST_PRICE_TO_NAN = 0.001     # Notational adjustment


def read_xtp_file(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None, verbose=True):
    """ Read XTP EOD Treasury options prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all snapshots; set False to return only latest snap for each price
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param verbose: set True to print name of file read
    :return: unindexed pd.DataFrame with labeled columns
    """
    if file_dir is None:
        file_dir = XTP_FILEDIR_TEMPLATE.format(tenor)
    if file_name is None:
        trade_date = datelike_to_timestamp(trade_datelike)
        file_name = XTP_FILENAME_TEMPLATE.format(trade_date.strftime('%y%m%d'))
    file = pd.read_csv(f'{file_dir}{file_name}', header=None, squeeze=True)
    if verbose:
        print(file_name + " read.")
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
    full.loc[full['Settlement'] == ADJUST_PRICE_TO_NAN, 'Settlement'] = np.NaN
    if return_full:
        return full
    else:
        # Create version with only latest price snapshot for each options series
        latest = full.groupby(['Last Trade Date', 'Put/Call', 'Strike Price']).tail(1) \
                 .reset_index(drop=True)    # Reset numerical index
        return latest


###############################################################################

if __name__ == '__main__':
    READABLE_DATA_DATES = []    # Trading days on which there is usable data
    EMPTY_DATA_DATES = []       # Trading days on which data files are empty
    MISSING_DATA_DATES = []     # Trading days on which there are no data files
    TRULY_DIFFERENT_DATES = []  # READABLE_DATA_DATES on which XTP and CME final prices do not match exactly
    PRICE_CHANGE_DATES = []     # READABLE_DATA_DATES on which settlement prices change over course of snapshots
    trading_days = pd.date_range(start='2019-10-28', end='2020-01-31', freq=BUSDAY_OFFSET)
    for date_str in trading_days.strftime('%Y-%m-%d'):
        try:
            xtp = read_xtp_file(10, date_str, return_full=False)
            xtp_full = read_xtp_file(10, date_str, return_full=True)
            READABLE_DATA_DATES.append(date_str)
        except EmptyDataError:
            EMPTY_DATA_DATES.append(date_str)
            continue
        except FileNotFoundError:
            MISSING_DATA_DATES.append(date_str)
            continue
        # Perform operations on READABLE dates
        # 1) Load comparable XTP and CME prices and check/record differences
        xtp_prices = xtp.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement']
        cme_e = read_cme_file(10, date_str, 'e')
        cme_e_prices = cme_e.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement']
        diff_bool = xtp_prices.ne(cme_e_prices)
        diff_idx = diff_bool[diff_bool]     # Remove False rows for aesthetic
        n_diffs = diff_idx.sum()
        if n_diffs != 0:
            print(f"\n****** {date_str} BAD - NO MATCH {n_diffs} DIFFS ******")
            print(f"CME Official:\n{cme_e_prices[diff_idx.index]}")
            print(f"XTP Capture:\n{xtp_prices[diff_idx.index]}")
            print(f"***********************************************\n")
            TRULY_DIFFERENT_DATES.append(date_str)
        # 2) Check/record if settlement prices changed over snapshots
        changesum = xtp_full.groupby(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement'].diff().abs().sum()
        if changesum != 0:
            print(f"WARNING: {date_str} settlement price(s) "
                  f"changed between snapshots; sum of changes is {changesum}.")
            PRICE_CHANGE_DATES.append(date_str)
        # # 3) Export as CME-formatted data files
        # xtp_indexed = xtp.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])
        # xtp_indexed.to_csv(f"{XTP_OUTPUT_FILEDIR}10y_{date_str}_EOD_latest.csv")
    print(f"\nTotal trading dates: {len(trading_days)}")
    print(f"Readable data dates: {len(READABLE_DATA_DATES)}")
    print(f"Readable data dates with differences to purchased CME data: {len(TRULY_DIFFERENT_DATES)}")
    print(f"Readable data dates with settlement price changes: {len(PRICE_CHANGE_DATES)}")
    print(f"File not found dates ({len(MISSING_DATA_DATES)}): {MISSING_DATA_DATES}")
    print(f"File found but data empty dates ({len(EMPTY_DATA_DATES)}): {EMPTY_DATA_DATES}\n")

""" Expected Output:
10y_2019-10-28_EOD_raw_e.csv read.
WARNING: 2019-10-28 settlement price(s) changed between snapshots; sum of changes is 0.15625.
10y_2019-10-29_EOD_raw_e.csv read.
WARNING: 2019-10-29 settlement price(s) changed between snapshots; sum of changes is 0.46875.
10y_2019-10-30_EOD_raw_e.csv read.
10y_2019-10-31_EOD_raw_e.csv read.
10y_2019-11-04_EOD_raw_e.csv read.
WARNING: 2019-11-04 settlement price(s) changed between snapshots; sum of changes is 0.03125.
10y_2019-12-05_EOD_raw_e.csv read.
10y_2019-12-06_EOD_raw_e.csv read.
WARNING: 2019-12-06 settlement price(s) changed between snapshots; sum of changes is 0.03125.
10y_2019-12-09_EOD_raw_e.csv read.
10y_2019-12-10_EOD_raw_e.csv read.
10y_2019-12-11_EOD_raw_e.csv read.
10y_2019-12-12_EOD_raw_e.csv read.
WARNING: 2019-12-12 settlement price(s) changed between snapshots; sum of changes is 1.53125.
10y_2019-12-13_EOD_raw_e.csv read.
WARNING: 2019-12-13 settlement price(s) changed between snapshots; sum of changes is 0.0625.
10y_2019-12-16_EOD_raw_e.csv read.
10y_2019-12-17_EOD_raw_e.csv read.
10y_2019-12-18_EOD_raw_e.csv read.
10y_2019-12-19_EOD_raw_e.csv read.
10y_2019-12-20_EOD_raw_e.csv read.
WARNING: 2019-12-20 settlement price(s) changed between snapshots; sum of changes is 0.09375.
10y_2019-12-23_EOD_raw_e.csv read.
WARNING: 2019-12-23 settlement price(s) changed between snapshots; sum of changes is 0.078125.
10y_2019-12-24_EOD_raw_e.csv read.
10y_2019-12-26_EOD_raw_e.csv read.
10y_2019-12-27_EOD_raw_e.csv read.

****** 2019-12-27 BAD - NO MATCH 2 DIFFS ******
CME Official:
Last Trade Date  Put/Call  Strike Price
2019-12-27       C         118.0           10.65625
                 P         106.0            0.00000
Name: Settlement, dtype: float64
XTP Capture:
Last Trade Date  Put/Call  Strike Price
2019-12-27       C         118.0          NaN
                 P         106.0          NaN
Name: Settlement, dtype: float64
***********************************************

WARNING: 2019-12-27 settlement price(s) changed between snapshots; sum of changes is 0.09375.
10y_2019-12-30_EOD_raw_e.csv read.
10y_2019-12-31_EOD_raw_e.csv read.
10y_2020-01-02_EOD_raw_e.csv read.
10y_2020-01-03_EOD_raw_e.csv read.
10y_2020-01-06_EOD_raw_e.csv read.
WARNING: 2020-01-06 settlement price(s) changed between snapshots; sum of changes is 0.09375.
10y_2020-01-07_EOD_raw_e.csv read.
WARNING: 2020-01-07 settlement price(s) changed between snapshots; sum of changes is 0.09375.
10y_2020-01-08_EOD_raw_e.csv read.
WARNING: 2020-01-08 settlement price(s) changed between snapshots; sum of changes is 0.03125.
10y_2020-01-09_EOD_raw_e.csv read.
WARNING: 2020-01-09 settlement price(s) changed between snapshots; sum of changes is 0.328125.
10y_2020-01-10_EOD_raw_e.csv read.
WARNING: 2020-01-10 settlement price(s) changed between snapshots; sum of changes is 0.125.
10y_2020-01-13_EOD_raw_e.csv read.
10y_2020-01-14_EOD_raw_e.csv read.
10y_2020-01-15_EOD_raw_e.csv read.
10y_2020-01-16_EOD_raw_e.csv read.
WARNING: 2020-01-16 settlement price(s) changed between snapshots; sum of changes is 0.28125.
10y_2020-01-21_EOD_raw_e.csv read.
WARNING: 2020-01-21 settlement price(s) changed between snapshots; sum of changes is 0.0625.
10y_2020-01-22_EOD_raw_e.csv read.
10y_2020-01-23_EOD_raw_e.csv read.
10y_2020-01-24_EOD_raw_e.csv read.

****** 2020-01-24 BAD - NO MATCH 1 DIFFS ******
CME Official:
Last Trade Date  Put/Call  Strike Price
2020-01-24       C         104.25          26.03125
Name: Settlement, dtype: float64
XTP Capture:
Last Trade Date  Put/Call  Strike Price
2020-01-24       C         104.25         NaN
Name: Settlement, dtype: float64
***********************************************

10y_2020-01-27_EOD_raw_e.csv read.
WARNING: 2020-01-27 settlement price(s) changed between snapshots; sum of changes is 0.21875.
10y_2020-01-28_EOD_raw_e.csv read.
WARNING: 2020-01-28 settlement price(s) changed between snapshots; sum of changes is 0.28125.
10y_2020-01-29_EOD_raw_e.csv read.
WARNING: 2020-01-29 settlement price(s) changed between snapshots; sum of changes is 0.015625.
10y_2020-01-30_EOD_raw_e.csv read.
10y_2020-01-31_EOD_raw_e.csv read.
WARNING: 2020-01-31 settlement price(s) changed between snapshots; sum of changes is 0.5.

Total trading dates: 66
Readable data dates: 43
Readable data dates with differences to purchased CME data: 2
Readable data dates with settlement price changes: 20
File not found dates (22): ['2019-11-01', '2019-11-05', '2019-11-06', '2019-11-07', '2019-11-08',
                            '2019-11-11', '2019-11-12', '2019-11-13', '2019-11-14', '2019-11-15',
                            '2019-11-18', '2019-11-19', '2019-11-20', '2019-11-21', '2019-11-22',
                            '2019-11-25', '2019-11-26', '2019-11-27', '2019-11-29', '2019-12-02',
                            '2019-12-03', '2019-12-04']
File found but data empty dates (1): ['2020-01-17']
"""
