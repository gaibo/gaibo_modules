import pandas as pd
from cboe_exchange_holidays_v3 import datelike_to_timestamp

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


def read_hanweck_futures(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None):
    """ Read Hanweck EOD Treasury futures prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury futures)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all tenors in Hanweck file; default return specified tenor
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
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
    # Return only specified tenor or full data
    if return_full:
        return data
    else:
        return data.set_index('Tenor').loc[tenor].reset_index(drop=True)


def read_hanweck_options(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None):
    """ Read Hanweck EOD Treasury options prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all tenors in Hanweck file; default return specified tenor
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
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
    # Return only specified tenor or full data
    if return_full:
        return data
    else:
        return data.set_index('Tenor').loc[tenor].reset_index(drop=True)


def read_hanweck_file(tenor, trade_datelike, return_full=False, file_dir=None, file_name=None,
                      futures_or_options='options'):
    """ Read Hanweck EOD Treasury prices from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury maturity derivatives)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param return_full: set True to return all tenors in Hanweck file; default return specified tenor
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param futures_or_options: set 'options' to use read_hanweck_options(), 'futures' to use read_hanweck_futures()
    :return: unindexed pd.DataFrame with labeled columns
    """
    if futures_or_options == 'options':
        return read_hanweck_options(tenor, trade_datelike, return_full, file_dir, file_name)
    elif futures_or_options == 'futures':
        return read_hanweck_futures(tenor, trade_datelike, return_full, file_dir, file_name)
    else:
        raise ValueError(f"futures_or_options must be 'options' or 'futures', not '{futures_or_options}'.")
