import pandas as pd
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from options_futures_expirations_v3 import BUSDAY_OFFSET
from cme_eod_file_reader import read_cme_file, FIRST_E_DATE
from xtp_eod_file_reader import read_xtp_file

CME_TO_HANWECK_HANDOFF_DATE = pd.Timestamp('2020-01-31')    # Date of last purchased CME EOD
# Futures Configurations
HANWECK_FILEDIR_FUT = 'P:/PrdDevSharedDB/CME Data/Hanweck/Futures/Unzipped/'
HANWECK_FILENAME_FUT_TEMPLATE = 'Hanweck_CME_Settlement_FUT_{}.csv'     # Fill {} with 20200121, etc.
HANWECK_OUTPUT_FILEDIR_FUT = 'P:/PrdDevSharedDB/CME Data/Hanweck/Futures/Unzipped/Formatted/'
FUTSYMBOL_TENOR_DICT = {'ZT': 2, 'ZF': 5, 'ZN': 10, 'ZB': 30}
HANWECK_FUT_FIELDS = \
    ['tickerElec', 'tickerExch', 'matMY',
     'matDate', 'SettlePrice',
     'desc_', 'mult', 'tickSize', 'PrevDayVol', 'PrevDayOI',
     'contractID']
HANWECK_FUT_DATE_FIELDS = ['matDate']
FUT_FIELDS_RENAME = \
    ['Symbol', 'Ticker', 'Contract YearMonth',
     'Maturity Date', 'Settlement',
     'Description', 'Multiplier', 'Tick Size', 'Previous Day Volume', 'Previous Day OI',
     'Contract ID']
FUT_FIELDS_OUTPUT = \
    ['Tenor', 'Last Trade Date', 'Settlement',
     'Description', 'Multiplier', 'Tick Size', 'Previous Day Volume', 'Previous Day OI',
     'Contract Year-Month', 'Symbol', 'Ticker', 'Contract ID']
# Options Configurations
HANWECK_FILEDIR_OPT = 'P:/PrdDevSharedDB/CME Data/Hanweck/Options/Unzipped/'
HANWECK_FILENAME_OPT_TEMPLATE = 'Hanweck_CME_Settlement_OOF_{}.csv'     # Fill {} with 20200121, etc.
HANWECK_OUTPUT_FILEDIR_OPT = 'P:/PrdDevSharedDB/CME Data/Hanweck/Options/Unzipped/Formatted/'
OPTSYMBOL_TENOR_DICT = {'OZT': 2, 'OZF': 5, 'OZN': 10, 'OZB': 30}
HANWECK_OPT_FIELDS = \
    ['tickerElec', 'tickerExch', 'matMY',
     'expDate', 'putCall', 'Strike', 'SettlePrice',
     'desc_', 'mult', 'tickSize', 'PrevDayVol', 'PrevDayOI',
     'SettleDelta', 'undlyId']
HANWECK_OPT_DATE_FIELDS = ['expDate']
OPT_FIELDS_RENAME = \
    ['Symbol', 'Ticker', 'Contract YearMonth',
     'Expiry Date', 'Put/Call', 'Strike Price', 'Settlement',
     'Description', 'Multiplier', 'Tick Size', 'Previous Day Volume', 'Previous Day OI',
     'Delta', 'Underlying Contract ID']
OPT_FIELDS_OUTPUT = \
    ['Tenor', 'Last Trade Date', 'Put/Call', 'Strike Price', 'Settlement',
     'Description', 'Multiplier', 'Tick Size', 'Previous Day Volume', 'Previous Day OI',
     'Delta', 'Contract Year-Month', 'Symbol', 'Ticker', 'Underlying Contract ID']


def read_hanweck_futures(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None, verbose=True):
    """ Read Hanweck EOD Treasury futures prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury futures)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all tenors in Hanweck file; default return specified tenor
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param verbose: set True to print name of file read
    :return: unindexed pd.DataFrame with labeled columns
    """
    # Load specified data
    if file_dir is None:
        file_dir = HANWECK_FILEDIR_FUT
    if file_name is None:
        trade_date = datelike_to_timestamp(trade_datelike)
        file_name = HANWECK_FILENAME_FUT_TEMPLATE.format(trade_date.strftime('%Y%m%d'))
    data = pd.read_csv(f'{file_dir}{file_name}', usecols=HANWECK_FUT_FIELDS,
                       parse_dates=HANWECK_FUT_DATE_FIELDS)[HANWECK_FUT_FIELDS]     # Enforce order
    if verbose:
        print(file_name + " read.")
    if data.empty:
        raise ValueError(f"Empty data file on {trade_datelike}, though columns exist.")
    data.columns = FUT_FIELDS_RENAME    # Rename fields to be consistent with CME style
    # Create additional useful fields
    data['Tenor'] = data['Symbol'].map(lambda s: FUTSYMBOL_TENOR_DICT[s])
    data['Contract Year-Month'] = (data['Contract YearMonth'].astype(str)
                                   .map(lambda s: s[:4] + '-' + s[4:]))
    # Perform final aesthetic touch-up
    data = data.rename({'Maturity Date': 'Last Trade Date'}, axis=1)  # Consistency with CME style
    data = data[FUT_FIELDS_OUTPUT]
    data = (data.sort_values(['Tenor', 'Last Trade Date'])
            .reset_index(drop=True))
    # Adjust for known notation differences from CME data
    data['Settlement'] = data['Settlement'].fillna(0)   # Convert Hanweck's NaN to CME's 0
    # Return only specified tenor or full data
    if return_full:
        return data
    else:
        return data.set_index('Tenor').loc[tenor].reset_index(drop=True)


def read_hanweck_options(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None, verbose=True):
    """ Read Hanweck EOD Treasury options prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all tenors in Hanweck file; default return specified tenor
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param verbose: set True to print name of file read
    :return: unindexed pd.DataFrame with labeled columns
    """
    # Load specified data
    if file_dir is None:
        file_dir = HANWECK_FILEDIR_OPT
    if file_name is None:
        trade_date = datelike_to_timestamp(trade_datelike)
        file_name = HANWECK_FILENAME_OPT_TEMPLATE.format(trade_date.strftime('%Y%m%d'))
    data = pd.read_csv(f'{file_dir}{file_name}', usecols=HANWECK_OPT_FIELDS,
                       parse_dates=HANWECK_OPT_DATE_FIELDS)[HANWECK_OPT_FIELDS]     # Enforce order
    if verbose:
        print(file_name + " read.")
    if data.empty:
        raise ValueError(f"Empty data file on {trade_datelike}, though columns exist.")
    data.columns = OPT_FIELDS_RENAME    # Rename fields to be consistent with CME style
    # Create additional useful fields
    data['Tenor'] = data['Symbol'].map(lambda s: OPTSYMBOL_TENOR_DICT[s])
    data['Contract Year-Month'] = (data['Contract YearMonth'].astype(str)
                                   .map(lambda s: s[:4] + '-' + s[4:]))
    # Perform final aesthetic touch-up
    data = data.rename({'Expiry Date': 'Last Trade Date'}, axis=1)  # Consistency with CME style
    data = data[OPT_FIELDS_OUTPUT]
    data = (data.sort_values(['Tenor', 'Last Trade Date', 'Put/Call', 'Strike Price'])
            .reset_index(drop=True))
    # Adjust for known notation differences from CME data
    data['Settlement'] = data['Settlement'].fillna(0)   # Convert Hanweck's NaN to CME's 0
    # Return only specified tenor or full data
    if return_full:
        return data
    else:
        return data.set_index('Tenor').loc[tenor].reset_index(drop=True)


def read_hanweck_file(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None, verbose=True,
                      futures_or_options='options'):
    """ Read Hanweck EOD Treasury prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury maturity derivatives)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all tenors in Hanweck file; default return specified tenor
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param verbose: set True to print name of file read
    :param futures_or_options: set 'options' to use read_hanweck_options(), 'futures' to use read_hanweck_futures()
    :return: unindexed pd.DataFrame with labeled columns
    """
    if futures_or_options == 'options':
        return read_hanweck_options(tenor, trade_datelike, return_full, file_dir, file_name, verbose)
    elif futures_or_options == 'futures':
        return read_hanweck_futures(tenor, trade_datelike, return_full, file_dir, file_name, verbose)
    else:
        raise ValueError(f"futures_or_options must be 'options' or 'futures', not '{futures_or_options}'.")


def read_cme_or_hanweck_file(tenor, trade_datelike, file_dir=None, file_name=None, verbose=True):
    """ Read either CME 'e' or Hanweck depending on date, automatically transitioning between the two
        NOTE: CME 'e' files did not exist prior to 2016-02-25
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param verbose: set True to print name of file read
    :return: unindexed pd.DataFrame with labeled columns
    """
    trade_date = datelike_to_timestamp(trade_datelike)
    if trade_date > CME_TO_HANWECK_HANDOFF_DATE:
        return read_hanweck_file(tenor, trade_date,
                                 file_dir=file_dir, file_name=file_name, verbose=verbose)
    elif trade_date < FIRST_E_DATE:
        raise ValueError("CME did not produce 'e' files until 2016-02-25.")
    else:
        return read_cme_file(tenor, trade_date,
                             file_dir=file_dir, file_name=file_name, verbose=verbose)


###############################################################################

if __name__ == '__main__':
    # Test equality of CME, XTP, and Hanweck data sources for Treasury options settlement
    trading_dates = pd.date_range(start='2020-01-20', end='2020-01-31', freq=BUSDAY_OFFSET)
    for date in trading_dates:
        # Load
        cme = read_cme_file(10, date)  # Default to 'e' file, which contains complete prices
        xtp = read_xtp_file(10, date)
        hanweck = read_hanweck_file(10, date)
        cme_price = cme.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement']
        xtp_price = xtp.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement']
        hanweck_price = hanweck.set_index(['Last Trade Date', 'Put/Call', 'Strike Price'])['Settlement']
        # Check equality
        date_str = date.strftime('%Y-%m-%d')
        if cme_price.equals(hanweck_price) and (hanweck_price.equals(xtp_price) or date_str == '2020-01-24'):
            print(f"{date_str}: PASS")  # 2020-01-24 is 1 of 2 known problematic XTP dates
        else:
            print(f"\n****{date_str}: FAIL****\n")

""" Expected Output:
10y_2020-01-21_EOD_raw_e.csv read.
OZN_settlement_200121.txt read.
Hanweck_CME_Settlement_OOF_20200121.csv read.
2020-01-21: PASS
10y_2020-01-22_EOD_raw_e.csv read.
OZN_settlement_200122.txt read.
Hanweck_CME_Settlement_OOF_20200122.csv read.
2020-01-22: PASS
10y_2020-01-23_EOD_raw_e.csv read.
OZN_settlement_200123.txt read.
Hanweck_CME_Settlement_OOF_20200123.csv read.
2020-01-23: PASS
10y_2020-01-24_EOD_raw_e.csv read.
OZN_settlement_200124.txt read.
Hanweck_CME_Settlement_OOF_20200124.csv read.
2020-01-24: PASS
10y_2020-01-27_EOD_raw_e.csv read.
OZN_settlement_200127.txt read.
Hanweck_CME_Settlement_OOF_20200127.csv read.
2020-01-27: PASS
10y_2020-01-28_EOD_raw_e.csv read.
OZN_settlement_200128.txt read.
Hanweck_CME_Settlement_OOF_20200128.csv read.
2020-01-28: PASS
10y_2020-01-29_EOD_raw_e.csv read.
OZN_settlement_200129.txt read.
Hanweck_CME_Settlement_OOF_20200129.csv read.
2020-01-29: PASS
10y_2020-01-30_EOD_raw_e.csv read.
OZN_settlement_200130.txt read.
Hanweck_CME_Settlement_OOF_20200130.csv read.
2020-01-30: PASS
10y_2020-01-31_EOD_raw_e.csv read.
OZN_settlement_200131.txt read.
Hanweck_CME_Settlement_OOF_20200131.csv read.
2020-01-31: PASS
"""
