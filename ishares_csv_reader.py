import pandas as pd
import numpy as np
from options_futures_expirations_v3 import BUSDAY_OFFSET, datelike_to_timestamp, TREASURY_BUSDAY_OFFSET
from bonds_analytics import create_coupon_schedule
from web_tools import download_file
import os
import warnings
from pandas.errors import EmptyDataError, PerformanceWarning

ETF_NAMES = ['SHY', 'IEI', 'IEF', 'TLH', 'TLT', 'MBB']
# SHY: 1-3 year Treasury bond ETF
# IEI: 3-7 year Treasury bond ETF
# IEF: 7-10 year Treasury bond ETF
# TLH: 10-20 year Treasury bond ETF
# TLT: 20+ year Treasury bond ETF
ETF_FILE_URL_DICT = {
    'SHY': {
        'XLS': ('https://www.ishares.com/us/products/'
                '239452/ishares-13-year-treasury-bond-etf/1521942788811.ajax?'
                'dataType=fund&fileType=xls&fileName=iShares-1-3-Year-Treasury-Bond-ETF_fund'),
        'Holdings': ('https://www.ishares.com/us/products/'
                     '239452/ishares-13-year-treasury-bond-etf/1467271812596.ajax?'
                     'dataType=fund&fileType=csv&fileName=SHY_holdings'),
        'Preliminary': 'https://www.ishares.com/us/literature/holdings/isht1-3-etf-early-holdings.csv',
        'Cash Flows': 'https://www.ishares.com/us/literature/cashflows/isht1-3-etf-cash-flows.csv'
    },
    'IEI': {
        'XLS': ('https://www.ishares.com/us/products/'
                '239455/ishares-37-year-treasury-bond-etf/1521942788811.ajax?'
                'dataType=fund&fileType=xls&fileName=iShares-3-7-Year-Treasury-Bond-ETF_fund'),
        'Holdings': ('https://www.ishares.com/us/products/'
                     '239455/ishares-37-year-treasury-bond-etf/1467271812596.ajax?'
                     'dataType=fund&fileType=csv&fileName=IEI_holdings'),
        'Preliminary': 'https://www.ishares.com/us/literature/holdings/isht3-7-etf-early-holdings.csv',
        'Cash Flows': 'https://www.ishares.com/us/literature/cashflows/isht3-7-etf-cash-flows.csv'
    },
    'IEF': {
        'XLS': ('https://www.ishares.com/us/products/'
                '239456/ishares-710-year-treasury-bond-etf/1521942788811.ajax?'
                'dataType=fund&fileType=xls&fileName=iShares-7-10-Year-Treasury-Bond-ETF_fund'),
        'Holdings': ('https://www.ishares.com/us/products/'
                     '239456/ishares-710-year-treasury-bond-etf/1467271812596.ajax?'
                     'dataType=fund&fileType=csv&fileName=IEF_holdings'),
        'Preliminary': 'https://www.ishares.com/us/literature/holdings/isht7-10-etf-early-holdings.csv',
        'Cash Flows': 'https://www.ishares.com/us/literature/cashflows/isht7-10-etf-cash-flows.csv'
    },
    'TLH': {
        'XLS': ('https://www.ishares.com/us/products/'
                '239453/ishares-1020-year-treasury-bond-etf/1521942788811.ajax?'
                'dataType=fund&fileType=xls&fileName=iShares-10-20-Year-Treasury-Bond-ETF_fund'),
        'Holdings': ('https://www.ishares.com/us/products/'
                     '239453/ishares-1020-year-treasury-bond-etf/1467271812596.ajax?'
                     'dataType=fund&fileType=csv&fileName=TLH_holdings'),
        'Preliminary': 'https://www.ishares.com/us/literature/holdings/isht10-20-etf-early-holdings.csv',
        'Cash Flows': 'https://www.ishares.com/us/literature/cashflows/isht10-20-etf-cash-flows.csv'
    },
    'TLT': {
        'XLS': ('https://www.ishares.com/us/products/'
                '239454/ishares-20-year-treasury-bond-etf/1521942788811.ajax?'
                'dataType=fund&fileType=xls&fileName=iShares-20-Year-Treasury-Bond-ETF_fund'),
        'Holdings': ('https://www.ishares.com/us/products/'
                     '239454/ishares-20-year-treasury-bond-etf/1467271812596.ajax?'
                     'dataType=fund&fileType=csv&fileName=TLT_holdings'),
        'Preliminary': 'https://www.ishares.com/us/literature/holdings/isht20-etf-early-holdings.csv',
        'Cash Flows': 'https://www.ishares.com/us/literature/cashflows/isht20-etf-cash-flows.csv'
    },
    'MBB': {
        'XLS': ('https://www.ishares.com/us/products/'
                '239465/ishares-mbs-etf/1521942788811.ajax?'
                'dataType=fund&fileType=xls&fileName=iShares-MBS-ETF_fund'),
        'Holdings': ('https://www.ishares.com/us/products/'
                     '239465/ishares-mbs-etf/1467271812596.ajax?'
                     'dataType=fund&fileType=csv&fileName=MBB_holdings'),
        'Preliminary': 'https://www.ishares.com/us/literature/holdings/ishmbs-etf-early-holdings.csv',
        'Cash Flows': 'https://www.ishares.com/us/literature/cashflows/ishmbs-etf-cash-flows.csv'
    }
}
URL_ASOFDATE_API_FORMAT = '&asOfDate={}'    # ...&asOfDate=20200623
ETF_FILEDIR = '//bats.com/projects/ProductDevelopment/Database/Production/ETF_Tsy_VIX/ETF Holdings/'
# Hard-code defective data dates ("as of" dates)
PAR_VALUE_1000_DATES = pd.to_datetime(['2014-12-31', '2015-01-30', '2015-02-27', '2015-03-31', '2015-04-30'])
VALUE_HALVE_DATES = pd.to_datetime(['2018-03-14'])  # NOTE: no longer an issue after the July 2020 holdings reformat!
VALUE_HALVE_FIELDS = ['Weight (%)', 'Market Value', 'Notional Value', 'Par Value']
# Hard-code helpful info for reading XLS files
HISTORICAL_SHEET_START = (
    '<ss:Worksheet ss:Name="Historical">\n'
    '<ss:Table>\n'
    '<ss:Row>\n'
    '<ss:Cell ss:StyleID="headerstyle">\n<ss:Data ss:Type="String">As Of</ss:Data>\n</ss:Cell>\n'
    '<ss:Cell ss:StyleID="headerstyle">\n<ss:Data ss:Type="String">Index Level</ss:Data>\n</ss:Cell>\n'
    '<ss:Cell ss:StyleID="headerstyle">\n<ss:Data ss:Type="String">NAV per Share</ss:Data>\n</ss:Cell>\n'
    '<ss:Cell ss:StyleID="headerstyle">\n<ss:Data ss:Type="String">Ex-Dividends</ss:Data>\n</ss:Cell>\n'
    '<ss:Cell ss:StyleID="headerstyle">\n<ss:Data ss:Type="String">Shares Outstanding</ss:Data>\n</ss:Cell>\n'
    '</ss:Row>\n'
)
AGNOSTIC_FIELD_START = '<ss:Data ss:Type='
NUM_FIELD_START = '<ss:Data ss:Type="Number">'
STR_FIELD_START = '<ss:Data ss:Type="String">'
FIELD_END = '</ss:Data>'
LEN_FIELD_START = 26    # Take advantage of len(NUM_FIELD_START) == len(STR_FIELD_START)


def load_holdings_csv(etf_name='TLT', asof_datelike=None,
                      file_dir=None, file_name=None, verbose=True):
    """ Read iShares ETF holdings file from disk
    :param etf_name: 'TLT', 'IEF', etc.
    :param asof_datelike: desired "as of" date of information; set None to get latest file
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :param verbose: set True for explicit print statements
    :return: (holdings DataFrame, extra info DataFrame)
    """
    # Derive local filename of specified file
    if file_dir is None:
        file_dir = ETF_FILEDIR
    if file_name is None:
        if asof_datelike is not None:
            # Most common case: craft filename from given "as of" date
            asof_date = datelike_to_timestamp(asof_datelike)
            asof_date_str = asof_date.strftime('%Y-%m-%d')
            file_name = f'{asof_date_str}_{etf_name}_holdings.csv'
        else:
            # Nothing is given: prepare latest holdings file available in file_dir
            file_name = sorted([f for f in os.listdir(file_dir)
                                if f.endswith(f'_{etf_name}_holdings.csv')])[-1]
    full_local_name = f'{file_dir}{file_name}'
    if verbose:
        print(f"Local file to be read: {full_local_name}")
    # Read regularly-formatted section (skipping first 9 rows)
    # NOTE: '\xa0' (at end of holdings CSV) is a non-breaking space in Latin1 (ISO 8859-1) (value 160)
    # NOTE: files frustratingly give coupon rates imprecisely - that is fixed here
    # NOTE: at start of 2020-07, iShares reformatted columns; try-except has been added to patch code
    try:
        holdings = pd.read_csv(full_local_name,
                               skiprows=range(9),
                               thousands=',',
                               na_values=['-', '\xa0'],
                               parse_dates=['Maturity']).dropna(how='all')
    except EmptyDataError:
        # Completely empty file - perhaps date is not a business date
        if verbose:
            print(f"WARNING: {full_local_name} appears to be completely empty.")
        return None, None
    except ValueError:
        # No "Maturity" column
        holdings = pd.read_csv(full_local_name,
                               skiprows=range(9),
                               thousands=',',
                               na_values=['-', '\xa0']).dropna(how='all')
        if verbose:
            print("WARNING: Holdings section has no \"Maturity\" column.")
    try:
        holdings.loc[(round(holdings['Coupon (%)'] % 1, 2) == 0.13)
                     | (round(holdings['Coupon (%)'] % 1, 2) == 0.38)
                     | (round(holdings['Coupon (%)'] % 1, 2) == 0.63)
                     | (round(holdings['Coupon (%)'] % 1, 2) == 0.88), 'Coupon (%)'] -= 0.005
    except KeyError:
        # No "Coupon (%) column
        if verbose:
            print("WARNING: Holdings section has no \"Coupon (%)\" column.")
    if verbose:
        print("Holdings section successfully formatted.")
    # Read irregularly-formatted section (first 8 rows, 7 if not counting header)
    extra_info = pd.read_csv(full_local_name, nrows=7, na_values=['-']).T
    date_fields = ['Fund Holdings as of', 'Inception Date']
    for date_field in date_fields:
        # No vectorized way to modify multiple columns' dtypes
        extra_info[date_field] = pd.to_datetime(extra_info[date_field])
    try:
        extra_info['Shares Outstanding'] = float(extra_info['Shares Outstanding'].squeeze().replace(',', ''))
    except AttributeError:
        if verbose:
            print("WARNING: Extra section has no \"Shares Outstanding\" info; likely has no info at all.")
    percent_fields = ['Stock', 'Bond', 'Cash', 'Other']     # NaN in recent files
    extra_info[percent_fields] = extra_info[percent_fields].astype(float)
    if verbose:
        print("Extra info section successfully formatted.")
        print(f"{file_name} read.")
    # Check for known defective data dates
    try:
        asof_date = pd.to_datetime(file_name[:10])
        if etf_name == 'TLT' and asof_date in PAR_VALUE_1000_DATES:
            holdings.loc[holdings['Name'] != 'BLK CSH FND TREASURY SL AGENCY', 'Par Value'] *= 1000
        # if etf_name == 'TLT' and asof_date in VALUE_HALVE_DATES:
        #     holdings[VALUE_HALVE_FIELDS] /= 2
    except ValueError:
        if verbose:
            print("WARNING: Cannot check for known defective data dates because custom file_name was given.")
    return holdings, extra_info


def create_temp_file_name(etf_name='TLT', identifier='holdings'):
    """ Create current-date-distinguishable placeholder filename for use with temporary downloads
    :param etf_name: 'TLT', 'IEF', etc.
    :param identifier: string uniquely identifying file, e.g. 'holdings', 'cashflows'
    :return: string filename (no directory)
    """
    today = pd.Timestamp('now').normalize()
    temp_asof_date = today - BUSDAY_OFFSET  # Guess that file has been updated to the latest available today
    temp_asof_date_str = temp_asof_date.strftime('%Y-%m-%d')
    temp_file_name = f'temp_{temp_asof_date_str}_{etf_name}_{identifier}_temp.csv'
    return temp_file_name


def _handle_download_to_temp(etf_name, identifier, file_query_url, file_dir):
    """ Helper: Handle downloading from URL to temporary file
    :param etf_name: 'TLT', 'IEF', etc.
    :param identifier: string uniquely identifying file, e.g. 'holdings', 'cashflows'
    :param file_query_url: URL to download from
    :param file_dir: directory to write data file
    :return: name of temporary file (not including file directory path, since that is given)
    """
    temp_file_name = create_temp_file_name(etf_name, identifier)
    temp_full_local_name = f'{file_dir}{temp_file_name}'
    download_success = download_file(file_query_url, temp_full_local_name, no_overwrite=False)  # Overwrite to ensure
    if not download_success:
        raise RuntimeError(f"Download failed.\n"
                           f"\tURL: {file_query_url}\n"
                           f"\tLocal save name: {temp_full_local_name}")
    return temp_file_name


def _handle_no_overwrite_temp_extraction(etf_name, identifier, file_query_url,
                                         file_dir, load_func, verbose=True):
    """ Helper: Handle situation of extracting data from temporary file then deleting file
    :param etf_name: 'TLT', 'IEF', etc.
    :param identifier: string uniquely identifying file, e.g. 'holdings', 'cashflows'
    :param file_query_url: URL to download from
    :param file_dir: directory to write data file
    :param load_func: function to load downloaded temporary file by filename
    :param verbose: set True for explicit print statements
    :return: (holdings DataFrame, extra info DataFrame) (same as load_holdings_csv)
    """
    if verbose:
        print("Initial download failed, likely because filename already exists.\n"
              "Will try downloading to temporary filename...")
    # Download to temporary file (overwriting allowed to ensure success)
    temp_file_name = _handle_download_to_temp(etf_name, identifier, file_query_url, file_dir)
    # Extract info from freshly downloaded temporary file
    extracted = \
        load_func(etf_name, file_dir=file_dir, file_name=temp_file_name, verbose=False)
    # Delete freshly downloaded temporary file
    temp_full_local_name = f'{file_dir}{temp_file_name}'
    os.remove(temp_full_local_name)
    if verbose:
        print(f"no_overwriting was set to True, so existing file was not touched.\n"
              f"Temporary download file {temp_file_name} has been used and deleted.")
    return extracted


def pull_current_holdings_csv(etf_name, file_dir=None, file_name=None, no_overwrite=True, verbose=True):
    """ Download current iShares ETF holdings file from website and write to disk
        NOTE: this function always returns freshly downloaded holdings/extra info,
              even if nothing is written to disk due to no_overwrite setting
    :param etf_name: 'TLT', 'IEF', etc.
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param no_overwrite: set True to never overwrite existing file; instead, a temporary file
                         will be created and destroyed to retrieve fresh information if needed
    :param verbose: set True for explicit print statements
    :return: (holdings DataFrame, extra info DataFrame) (same as load_holdings_csv)
    """
    file_query_url = ETF_FILE_URL_DICT[etf_name]['Holdings']
    if file_dir is None:
        file_dir = ETF_FILEDIR
    if file_name is None:
        # Execute clever plan:
        #   1) Download file to temporary name
        #   2) Load file and obtain true "as of" date
        #   3) Rename file properly using true "as of" date
        # Download file and give it placeholder name
        temp_file_name = _handle_download_to_temp(etf_name, 'holdings', file_query_url, file_dir)
        temp_full_local_name = f'{file_dir}{temp_file_name}'
        # Open freshly downloaded file to obtain true "as of" date
        holdings, extra_info = load_holdings_csv(etf_name, file_dir=file_dir, file_name=temp_file_name, verbose=False)
        if extra_info is None:
            if verbose:
                print(f"WARNING: Downloaded holdings file {temp_full_local_name} appears to be empty.\n"
                      f"         Temporary file will not be renamed. Please dispose of it manually.")
            return holdings, extra_info
        asof_date = extra_info['Fund Holdings as of'].squeeze()
        asof_date_str = asof_date.strftime('%Y-%m-%d')
        # Rename downloaded file properly
        file_name = f'{asof_date_str}_{etf_name}_holdings.csv'
        full_local_name = f'{file_dir}{file_name}'
        try:
            os.rename(temp_full_local_name, full_local_name)
            if verbose:
                print(f"Renamed {temp_full_local_name} to {full_local_name}.")
        except FileExistsError:
            # File with proper name already exists; check overwriting protocol
            if no_overwrite:
                os.remove(temp_full_local_name)
                if verbose:
                    print("Smart rename failed; file with \"as of\" date already exists.\n"
                          "Download has been deleted - directory is back to state prior to function call.")
            else:
                os.remove(full_local_name)  # Delete existing file so its name can be taken
                os.rename(temp_full_local_name, full_local_name)
                if verbose:
                    print(f"Overwrote {temp_full_local_name} to {full_local_name}.")
    else:
        # Rare but simple case: filename to save as is given
        full_local_name = f'{file_dir}{file_name}'
        # Download using overwriting protocol
        download_success = download_file(file_query_url, full_local_name, no_overwrite=no_overwrite)
        if not download_success:
            # Try download to temporary file, extract data, then delete file
            holdings, extra_info = \
                _handle_no_overwrite_temp_extraction(etf_name, 'holdings', file_query_url,
                                                     file_dir, load_holdings_csv, verbose=verbose)
        else:
            # Open freshly downloaded file
            holdings, extra_info = load_holdings_csv(etf_name, file_dir=file_dir, file_name=file_name, verbose=False)
            if verbose:
                print(f"Wrote (or overwrote) file {full_local_name}.")
    return holdings, extra_info


def pull_historical_holdings_csv(etf_name, asof_datelike,
                                 file_dir=None, file_name=None, no_overwrite=True, verbose=True):
    """ Download historical iShares ETF holdings file from website and write to disk
    :param etf_name: 'TLT', 'IEF', etc.
    :param asof_datelike: desired "as of" date of information
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param no_overwrite: set True to never overwrite existing file; instead, a temporary file
                         will be created and destroyed to retrieve fresh information if needed
    :param verbose: set True for explicit print statements
    :return: (holdings DataFrame, extra info DataFrame) (same as load_holdings_csv)
    """
    # Construct URL to query for specific historical "as of" date
    file_query_url = ETF_FILE_URL_DICT[etf_name]['Holdings']
    asof_date = datelike_to_timestamp(asof_datelike)
    file_query_url += URL_ASOFDATE_API_FORMAT.format(asof_date.strftime('%Y%m%d'))
    # Construct filename to save to (no auto-renaming needed since "as of" date is known)
    if file_dir is None:
        file_dir = ETF_FILEDIR
    if file_name is None:
        file_name = f"{asof_date.strftime('%Y-%m-%d')}_{etf_name}_holdings.csv"
    full_local_name = f'{file_dir}{file_name}'
    # Download using overwriting protocol
    download_success = download_file(file_query_url, full_local_name, no_overwrite=no_overwrite)
    if not download_success:
        # Try download to temporary file, extract data, then delete file
        holdings, extra_info = \
            _handle_no_overwrite_temp_extraction(etf_name, 'holdings', file_query_url,
                                                 file_dir, load_holdings_csv, verbose=verbose)
    else:
        # Open freshly downloaded file
        holdings, extra_info = load_holdings_csv(etf_name, file_dir=file_dir, file_name=file_name, verbose=False)
        if verbose:
            print(f"Wrote (or overwrote) file {full_local_name}.")
    return holdings, extra_info


def pull_holdings_csv(etf_name='TLT', asof_datelike=None,
                      file_dir=None, file_name=None, no_overwrite=True, verbose=True):
    """ Download iShares ETF holdings file from website and write to disk
        NOTE: distinguishes between current and historical downloads through asof_datelike field
    :param etf_name: 'TLT', 'IEF', etc.
    :param asof_datelike: desired "as of" date of information; set None to get current file
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param no_overwrite: set True to never overwrite existing file; instead, a temporary file
                         will be created and destroyed to retrieve fresh information if needed
    :param verbose: set True for explicit print statements
    :return: (holdings DataFrame, extra info DataFrame) (same as load_holdings_csv)
    """
    if asof_datelike is None:
        # No "as of" date specified means current file is desired
        return pull_current_holdings_csv(etf_name,
                                         file_dir=file_dir, file_name=file_name,
                                         no_overwrite=no_overwrite, verbose=verbose)
    else:
        return pull_historical_holdings_csv(etf_name, asof_datelike,
                                            file_dir=file_dir, file_name=file_name,
                                            no_overwrite=no_overwrite, verbose=verbose)


def get_historical_xls_info(etf_name, asof_datelike,
                            file_dir=None, file_name=None, verbose=True):
    """ Read historical information from latest iShares XLS file from disk
        NOTE: iShares XLS files are created in XML that is close to an awful early-Excel format
              called XML Spreadsheet 2003 (they are misnamed as .xls), so we parse them manually
    :param etf_name: 'TLT', 'IEF', etc.
    :param asof_datelike: desired "as of" date of information
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :param verbose: set True for explicit print statements
    :return: (index level, NAV per share, ex-dividends, shares outstanding)
    """
    asof_date = datelike_to_timestamp(asof_datelike)
    # Derive local filename of relevant XLS file
    if file_dir is None:
        file_dir = ETF_FILEDIR
    if file_name is None:
        # Use latest XLS file available in file_dir (does not depend on "as of" date)
        file_name = sorted([f for f in os.listdir(file_dir)
                            if f.endswith(f'_{etf_name}.xls')])[-1]
    # Open XLS file and parse by raw string
    full_local_name = f'{file_dir}{file_name}'
    with open(full_local_name) as f:
        f_text = f.read()  # Extract all contents of file to string
        hist_sheet_loc = f_text.find(HISTORICAL_SHEET_START)  # Find Historical sheet for starting point
        asof_date_str = asof_date.strftime('%b %d, %Y')
        asof_date_loc = f_text.find(asof_date_str, hist_sheet_loc, -1)  # Find date in Historical sheet
        if asof_date_loc == -1:
            raise ValueError(f"\"as of\" date '{asof_date_str}' could not be found in {file_name}")
        # Extract index level (1st field; Number)
        index_start = f_text.find(NUM_FIELD_START, asof_date_loc, -1) + LEN_FIELD_START
        index_end = f_text.find(FIELD_END, index_start, -1)
        index = float(f_text[index_start:index_end])
        # Extract NAV per share (2nd field; Number)
        nav_start = f_text.find(NUM_FIELD_START, index_end, -1) + LEN_FIELD_START
        nav_end = f_text.find(FIELD_END, nav_start, -1)
        nav = float(f_text[nav_start:nav_end])
        # Extract ex-dividends (3rd field; String '--' if none, Number if exists)
        div_agnostic_field_start = f_text.find(AGNOSTIC_FIELD_START, nav_end, -1)
        div_start = div_agnostic_field_start + LEN_FIELD_START  # Assume field is either string or number
        div_field_start = f_text[div_agnostic_field_start:div_start]
        if div_field_start == STR_FIELD_START:
            div = 0.0   # Don't bother reading the '--'
        elif div_field_start == NUM_FIELD_START:
            div_end = f_text.find(FIELD_END, div_start, -1)
            div = float(f_text[div_start:div_end])
        else:
            raise ValueError(f"{asof_date_str} div_field_start indicates neither "
                             f"String nor Number: '{div_field_start}'")
        # Extract shares outstanding (4th field; Number)
        shares_start = f_text.find(NUM_FIELD_START, div_start, -1) + LEN_FIELD_START
        shares_end = f_text.find(FIELD_END, shares_start, -1)
        shares = float(f_text[shares_start:shares_end])
    if verbose:
        print(f"{file_name} read.")
    return index, nav, div, shares


def load_cashflows_csv(etf_name='TLT', asof_datelike=None,
                       file_dir=None, file_name=None, verbose=True):
    """ Read iShares ETF cash flows file from disk
    :param etf_name: 'TLT', 'IEF', etc.
    :param asof_datelike: desired "as of" date of information; set None to get latest file
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :param verbose: set True for explicit print statements
    :return: pd.DataFrame
    """
    # Derive local filename of specified file
    if file_dir is None:
        file_dir = ETF_FILEDIR
    if file_name is None:
        if asof_datelike is not None:
            # Most common case: craft filename from given "as of" date
            asof_date = datelike_to_timestamp(asof_datelike)
            asof_date_str = asof_date.strftime('%Y-%m-%d')
            file_name = f'{asof_date_str}_{etf_name}_cashflows.csv'
        else:
            # Nothing is given: prepare latest holdings file available in file_dir
            file_name = sorted([f for f in os.listdir(file_dir)
                                if f.endswith(f'_{etf_name}_cashflows.csv')])[-1]
    full_local_name = f'{file_dir}{file_name}'
    # Read file
    cashflows = pd.read_csv(full_local_name, parse_dates=['ASOF_DATE', 'CASHFLOW_DATE'])
    if verbose:
        print(f"{file_name} read.")
    return cashflows


def pull_cashflows_csv(etf_name='TLT', file_dir=None, file_name=None, no_overwrite=True, verbose=True):
    """ Download current iShares ETF cash flows file from website and write to disk
        NOTE: this function always returns freshly downloaded info,
              even if nothing is written to disk due to no_overwrite setting
    :param etf_name: 'TLT', 'IEF', etc.
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param no_overwrite: set True to never overwrite existing file; instead, a temporary file
                         will be created and destroyed to retrieve fresh information if needed
    :param verbose: set True for explicit print statements
    :return: pd.DataFrame (same as load_cashflows_csv)
    """
    file_query_url = ETF_FILE_URL_DICT[etf_name]['Cash Flows']
    if file_dir is None:
        file_dir = ETF_FILEDIR
    if file_name is None:
        # Execute clever plan:
        #   1) Download file to temporary name
        #   2) Load file and obtain true "as of" date
        #   3) Rename file properly using true "as of" date
        # Download file and give it placeholder name
        temp_file_name = _handle_download_to_temp(etf_name, 'cashflows', file_query_url, file_dir)
        temp_full_local_name = f'{file_dir}{temp_file_name}'
        # Open freshly downloaded file to obtain true "as of" date
        cashflows = load_cashflows_csv(etf_name, file_dir=file_dir, file_name=temp_file_name, verbose=False)
        if cashflows.empty:
            if verbose:
                print(f"WARNING: Downloaded cashflows file {temp_full_local_name} appears to be empty.\n"
                      f"         Temporary file will not be renamed. Please dispose of it manually.")
            return cashflows
        asof_date = cashflows['ASOF_DATE'].iloc[0]
        asof_date_str = asof_date.strftime('%Y-%m-%d')
        # Rename downloaded file properly
        file_name = f'{asof_date_str}_{etf_name}_cashflows.csv'
        full_local_name = f'{file_dir}{file_name}'
        try:
            os.rename(temp_full_local_name, full_local_name)
            if verbose:
                print(f"Renamed {temp_full_local_name} to {full_local_name}.")
        except FileExistsError:
            # File with proper name already exists; check overwriting protocol
            if no_overwrite:
                os.remove(temp_full_local_name)
                if verbose:
                    print("Smart rename failed; file with \"as of\" date already exists.\n"
                          "Download has been deleted - directory is back to state prior to function call.")
            else:
                os.remove(full_local_name)  # Delete existing file so its name can be taken
                os.rename(temp_full_local_name, full_local_name)
                if verbose:
                    print(f"Overwrote {temp_full_local_name} to {full_local_name}.")
    else:
        # Rare but simple case: filename to save as is given
        full_local_name = f'{file_dir}{file_name}'
        # Download using overwriting protocol
        download_success = download_file(file_query_url, full_local_name, no_overwrite=no_overwrite)
        if not download_success:
            # Try download to temporary file, extract data, then delete file
            cashflows = \
                _handle_no_overwrite_temp_extraction(etf_name, 'cashflows', file_query_url,
                                                     file_dir, load_cashflows_csv, verbose=verbose)
        else:
            # Open freshly downloaded file
            cashflows = load_cashflows_csv(etf_name, file_dir=file_dir, file_name=file_name, verbose=False)
            if verbose:
                print(f"Wrote (or overwrote) file {full_local_name}.")
    return cashflows


def to_per_million_shares(value, shares_outstanding):
    """ Scale a value to per one million shares """
    return value / shares_outstanding * 1000000


def coupon_payment_from_holding(row, shares_outstanding):
    """ Helper: Calculate scaled (per million shares) coupon payment from row of iShares holdings file
        NOTE: written with DataFrame.apply() in mind
    :param row: holding (note/bond) information; in particular, 'Coupon (%)' and 'Par Value'
    :param shares_outstanding: number of shares outstanding; used to scale row info
    :return: scaled value
    """
    coupon_portion = row['Coupon (%)'] / 100 / 2
    coupon_payment = coupon_portion * row['Par Value']
    return to_per_million_shares(coupon_payment, shares_outstanding)


def face_payment_from_holding(row, shares_outstanding):
    """ Helper: Calculate scaled (per million shares) face payment from row of iShares holdings file
        NOTE: written with DataFrame.apply() in mind
    :param row: holding (note/bond) information; in particular, 'Par Value'
    :param shares_outstanding: number of shares outstanding; used to scale row info
    :return: scaled value
    """
    face_payment = row['Par Value']
    return to_per_million_shares(face_payment, shares_outstanding)


def get_cashflows_from_holdings(etf_name='TLT', asof_datelike=None, file_dir=None, file_name=None,
                                live_calc=False, shift_shares=False, verbose=True):
    """ Create aggregated cash flows (ACF) information (in style of iShares ETF cash flows file)
        from local holdings information (Shares ETF holdings file)
        NOTE: the process in this function is highly visual - coupon_flow_df and face_flow_df
              may be useful for visualizing the cash flows contributions of individual notes/bonds
    :param etf_name: 'TLT', 'IEF', etc.
    :param asof_datelike: desired "as of" date of information; set None to get latest file
    :param file_dir: directory to search for data file (overrides default directory)
    :param file_name: exact file name to load from file_dir (overrides default file name)
    :param live_calc: set True if conversion is being performed during trade date just after "as of" date;
                      necessary to trigger alternative method for obtaining ex-dividend date distributions
    :param shift_shares: set True to perform idiosyncratic shift of shares outstanding to day before;
                         useful to account for iShares erroneous file format
    :param verbose: set True for explicit print statements
    :return:
    """
    # Load holdings (and section of additional info) from local CSV
    holdings, extra = load_holdings_csv(etf_name, asof_datelike, file_dir=file_dir, file_name=file_name, verbose=False)
    if holdings.empty:
        raise ValueError(f"ERROR: empty \"as of\" date: {asof_datelike}")
    asof_date = extra['Fund Holdings as of'].squeeze()  # Obtain pd.Timestamp this way, in case asof_datelike is None
    asof_date_str = asof_date.strftime('%Y-%m-%d')
    # Derive trade date and settle date
    trade_date = asof_date + BUSDAY_OFFSET
    settle_date = trade_date + 2*BUSDAY_OFFSET  # Formerly T+3 back in 2016ish
    if verbose:
        print(f"\"As of\" date: {asof_date_str}\n"
              f"Trade date: {trade_date.strftime('%Y-%m-%d')}\n"
              f"Settlement date: {settle_date.strftime('%Y-%m-%d')}")
    # Obtain shares outstanding
    if shift_shares:
        _, next_extra = load_holdings_csv(etf_name, trade_date, file_dir=file_dir, file_name=file_name, verbose=False)
        shares_outstanding = next_extra['Shares Outstanding'].squeeze()
        if verbose:
            print("Purposefully pulling shares outstanding from holdings CSV 1 day after \"as of\" date...")
    else:
        shares_outstanding = extra['Shares Outstanding'].squeeze()
    if verbose:
        print(f"Shares outstanding: {shares_outstanding}")

    # Focus only on Treasury notes/bonds, exclude cash-like assets
    notesbonds = holdings[holdings['Asset Class'] == 'Fixed Income'].reset_index(drop=True)
    # Map out all unique upcoming coupon dates
    # NOTE: coupon stops showing up when "as of" date reaches coupon arrival date, so want coupon dates after "as of"
    coupon_schedules = notesbonds['Maturity'].map(lambda m: list(create_coupon_schedule(m, asof_date)))
    unique_coupon_dates = sorted(set(coupon_schedules.sum()))
    # Map out all unique upcoming maturity dates
    unique_maturity_dates = sorted(set(notesbonds['Maturity']))

    # Initialize empty DataFrame with a column for each unique coupon date
    coupon_flow_df = pd.DataFrame(columns=unique_coupon_dates)
    # Initialize empty DataFrame with a column for each unique maturity date
    face_flow_df = pd.DataFrame(columns=unique_maturity_dates)
    # Fill each holding's cash flows into DataFrames according to schedule
    for i, holding in notesbonds.iterrows():
        # Note/bond's coupon amounts
        scaled_coupon_payment = coupon_payment_from_holding(holding, shares_outstanding)
        coupon_flow_df.loc[holding['ISIN'], coupon_schedules[i]] = scaled_coupon_payment    # To all coupon dates
        # Note/bond's maturity face amount
        scaled_face_payment = face_payment_from_holding(holding, shares_outstanding)
        face_flow_df.loc[holding['ISIN'], holding['Maturity']] = scaled_face_payment    # To maturity date

    # Compress interest (coupon) and principal (face) into a DataFrame
    interest_ser = coupon_flow_df.sum()
    principal_ser = face_flow_df.sum()
    cashflows_df = pd.DataFrame({'INTEREST': interest_ser,
                                 'PRINCIPAL': principal_ser}).replace(np.NaN, 0)

    # Change from raw maturity dates (15th) to cash flows dates (next business dates if 15th is not)
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=PerformanceWarning)     # Following operation is not vectorized
        cashflows_df.index = cashflows_df.index - TREASURY_BUSDAY_OFFSET + TREASURY_BUSDAY_OFFSET
    cashflows_df.index.name = 'CASHFLOW_DATE'

    # Calculate implied cash
    # NOTE: implied cash must be reduced on ex-dividend date by distribution amount
    bond_mv = notesbonds['Market Value'].sum()
    _, nav_per_share, _, _ = get_historical_xls_info('TLT', asof_date, verbose=False)
    if not live_calc:
        # If calculating historically, get dividends from iShares XLS which updates at end of each trade date
        try:
            _, _, div, _ = get_historical_xls_info('TLT', trade_date, verbose=False)    # Allowed to raise ValueError
        except ValueError as e:
            raise ValueError(f"No XLS historical data found for {asof_date_str}; "
                             f"set live_calc=True if day-of dividends are needed.\n"
                             f"{e}")
        if div != 0:
            nav_per_share -= div
            if verbose:
                print(f"Dividend: {div} found for ex-dividend date {asof_date_str}")
    else:
        # Pull latest dividends info from website sidebar (dividend will be available morning of ex-date)
        pass
    nav_mv = nav_per_share * shares_outstanding
    implied_cash = nav_mv - bond_mv
    implied_cash_scaled = to_per_million_shares(implied_cash, shares_outstanding)
    # Add into cash flows as a principal
    implied_cash_maturity_date = settle_date + BUSDAY_OFFSET
    if implied_cash_maturity_date in cashflows_df.index:
        cashflows_df.loc[implied_cash_maturity_date, 'PRINCIPAL'] += implied_cash_scaled
    else:
        cashflows_df.loc[implied_cash_maturity_date] = (0, implied_cash_scaled)
    cashflows_df = cashflows_df.sort_index()
    if verbose:
        print(f"Implied cash maturity date: {implied_cash_maturity_date.strftime('%Y-%m-%d')}\n"
              f"Implied cash per million shares: {implied_cash_scaled}")

    # Create sum of interest and principal column, for convenience like in iShares cash flow CSV
    cashflows_df['CASHFLOW'] = cashflows_df.sum(axis=1)
    return cashflows_df


###############################################################################

if __name__ == '__main__':
    # Test get_historical_xls_info() to known examples
    assert (get_historical_xls_info('TLT', '2020-06-01', verbose=False)
            == (150.197131, 162.293398, 0.210189, 113000000.0))
    assert (get_historical_xls_info('TLT', '2020-06-05', verbose=False)
            == (143.930002, 155.544228, 0.0, 110600000.0))
    assert (get_historical_xls_info('TLT', '2020-06-08', verbose=False)
            == (144.609098, 156.275692, 0.0, 110100000.0))

    # Compare get_cashflows_from_holdings() to official iShares cash flow files
    test_asof_date = '2020-07-10'
    # In-house
    test_cashflows_inhouse = get_cashflows_from_holdings('TLT', test_asof_date,
                                                         live_calc=False, shift_shares=False, verbose=False)
    # Official
    raw_cashflows = load_cashflows_csv('TLT', test_asof_date, verbose=False)
    test_cashflows = (raw_cashflows.loc[raw_cashflows['CALL_TYPE'] == 'MATURITY',
                                        ['CASHFLOW_DATE', 'INTEREST', 'PRINCIPAL', 'CASHFLOW']]
                      .set_index('CASHFLOW_DATE'))
    # Compare
    diffs = (test_cashflows - test_cashflows_inhouse.round(3)).round(3).abs().sum()
    print(f"Difference in $ between in-house and official iShares cashflows:\n{diffs}")
    if diffs['INTEREST'] > 0:
        raise AssertionError(f"INTEREST DIFFERENCE FOUND: ${diffs['INTEREST']:.2f}")
    if diffs['PRINCIPAL'] > 10:
        raise AssertionError(f"PRINCIPAL DIFFERENCE >$10 FOUND: ${diffs['PRINCIPAL']:.2f}")
