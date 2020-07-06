import pandas as pd
from options_futures_expirations_v3 import BUSDAY_OFFSET, datelike_to_timestamp
from web_tools import download_file
import os

ETF_NAMES = ['SHY', 'IEI', 'IEF', 'TLH', 'TLT']
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
    }
}
URL_ASOFDATE_API_FORMAT = '&asOfDate={}'    # ...&asOfDate=20200623
ETF_FILEDIR = '//bats.com/projects/ProductDevelopment/Database/Production/ETF_Tsy_VIX/ETF Holdings/'


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
    holdings = pd.read_csv(full_local_name,
                           skiprows=range(9),
                           dtype={'Weight (%)': float, 'Price': float, 'Market Value': float,
                                  'Notional Value': float, 'Coupon (%)': float, 'YTM (%)': float,
                                  'Yield to Worst (%)': float, 'Duration': float, 'Par Value': float},
                           thousands=',',
                           na_values=['-', '\xa0'],
                           parse_dates=['Maturity']).dropna(how='all')
    holdings.loc[(round(holdings['Coupon (%)'] % 1, 2) == 0.13)
                 | (round(holdings['Coupon (%)'] % 1, 2) == 0.38)
                 | (round(holdings['Coupon (%)'] % 1, 2) == 0.63)
                 | (round(holdings['Coupon (%)'] % 1, 2) == 0.88), 'Coupon (%)'] -= 0.005
    if verbose:
        print("Holdings section successfully formatted.")
    # Read irregularly-formatted section (first 8 rows, 7 if not counting header)
    extra_info = pd.read_csv(full_local_name, nrows=7, na_values=['-']).T
    date_fields = ['Fund Holdings as of', 'Inception Date']
    for date_field in date_fields:
        # No vectorized way to modify multiple columns' dtypes
        extra_info[date_field] = pd.to_datetime(extra_info[date_field])
    extra_info['Shares Outstanding'] = float(extra_info['Shares Outstanding'].squeeze().replace(',', ''))
    percent_fields = ['Stock', 'Bond', 'Cash', 'Other']     # NaN in recent files
    extra_info[percent_fields] = extra_info[percent_fields].astype(float)
    if verbose:
        print("Extra info section successfully formatted.")
        print(f"{file_name} read.")
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


def _handle_no_overwrite_temp_extraction(etf_name, identifier, file_query_url, file_dir, verbose=True):
    """ Helper: Handle situation of extracting data from temporary file then deleting file
    :param etf_name: 'TLT', 'IEF', etc.
    :param identifier: string uniquely identifying file, e.g. 'holdings', 'cashflows'
    :param file_query_url: URL to download from
    :param file_dir: directory to write data file
    :param verbose: set True for explicit print statements
    :return: (holdings DataFrame, extra info DataFrame) (same as load_holdings_csv)
    """
    if verbose:
        print("Initial download failed, likely because filename already exists.\n"
              "Will try downloading to temporary filename...")
    # Download to temporary file (overwriting allowed to ensure success)
    temp_file_name = _handle_download_to_temp(etf_name, identifier, file_query_url, file_dir)
    # Extract info from freshly downloaded temporary file
    holdings, extra_info = \
        load_holdings_csv(etf_name, file_dir=file_dir, file_name=temp_file_name, verbose=False)
    # Delete freshly downloaded temporary file
    temp_full_local_name = f'{file_dir}{temp_file_name}'
    os.remove(temp_full_local_name)
    if verbose:
        print(f"no_overwriting was set to True, so existing file was not touched.\n"
              f"Temporary download file {temp_file_name} has been used and deleted.")
    return holdings, extra_info


def pull_current_holdings_csv(etf_name, file_dir=None, file_name=None, no_overwrite=True, verbose=True):
    """ Download current iShares ETF holdings file from website and write to disk
        NOTE: this function always returns freshly downloaded holdings/extra info,
              even if nothing is written to disk due to no_overwrite setting
    :param etf_name: 'TLT', 'IEF', etc.
    :param file_dir: directory to write data file (overrides default directory)
    :param file_name: exact file name to write to file_dir (overrides default file name)
    :param no_overwrite: set True to do nothing rather than overwrite existing file
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
                _handle_no_overwrite_temp_extraction(etf_name, 'holdings', file_query_url, file_dir, verbose=verbose)
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
    :param no_overwrite: set True to do nothing rather than overwrite existing file
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
            _handle_no_overwrite_temp_extraction(etf_name, 'holdings', file_query_url, file_dir, verbose=verbose)
    else:
        # Open freshly downloaded file
        holdings, extra_info = load_holdings_csv(etf_name, file_dir=file_dir, file_name=file_name, verbose=False)
        if verbose:
            print(f"Wrote (or overwrote) file {full_local_name}.")
    return holdings, extra_info


# def pull_holdings_csv(etf_name='TLT', asof_datelike=None,
#                       file_dir=None, file_name=None, no_overwrite=True, verbose=True):
#     """ Download iShares ETF holdings file from website and write to disk
#     :param etf_name: 'TLT', 'IEF', etc.
#     :param asof_datelike: desired "as of" date of information; set None to get latest file
#     :param file_dir: directory to write data file (overrides default directory)
#     :param file_name: exact file name to write to file_dir (overrides default file name)
#     :param no_overwrite: set True to do nothing rather than overwrite existing file
#     :param verbose: set True for explicit print statements
#     :return: (holdings DataFrame, extra info DataFrame) (same as load_holdings_csv)
#     """
#     # Derive 1) web URL to query, 2) tentative "as of" date of download, 3) local filename of download
#     file_query_url = ETF_FILE_URL_DICT[etf_name]['Holdings']
#     if asof_datelike is None:
#         # Prep to grab latest from website; make temporary guess on "as of" date and correct after download
#         today = pd.Timestamp('now').normalize()
#         asof_date = today - BUSDAY_OFFSET   # Guess that file has been updated to the latest available today
#     else:
#         # Prep to query website's API specifically for historical files
#         asof_date = datelike_to_timestamp(asof_datelike)
#         file_query_url += URL_ASOFDATE_API_FORMAT.format(asof_date.strftime('%Y%m%d'))
#     asof_date_str = asof_date.strftime('%Y-%m-%d')
#     if file_dir is None:
#         file_dir = ETF_FILEDIR
#     if file_name is None:
#         file_name = f'{asof_date_str}_{etf_name}_holdings.csv'
#     temp_full_local_name = f'{file_dir}temp_{file_name}'    # Not finalized, but meant to be unique
#     if verbose:
#         print("Pre-download details:\n"
#               f"\tFile query URL: {file_query_url}\n"
#               f"\tPresumed \"as of\" date: {asof_date_str}\n"
#               f"\tTemporary local filename to save as: {temp_full_local_name}")
#     # Download file and give it temporary name
#     download_success = download_file(file_query_url, temp_full_local_name)  # Will not overwrite if temp name exists
#     if verbose:
#         print(f"{asof_date_str} {etf_name} holdings CSV downloaded: {download_success}")
#     if not download_success:
#         raise RuntimeError("Download failed, likely because file with name already exists.")
#     # Open freshly downloaded file to ensure correctness of "as of" date; correct if needed
#     holdings, extra_info = load_holdings_csv(etf_name, file_dir=file_dir, file_name=file_name, verbose=False)
#     correct_asof_date = extra_info['Fund Holdings as of'].squeeze()
#     if correct_asof_date != asof_date:
#         # Rename file
#         correct_asof_date_str = correct_asof_date.strftime('%Y-%m-%d')
#         correct_file_name = f'{correct_asof_date_str}_{etf_name}_holdings.csv'
#         correct_full_local_name = f'{file_dir}{correct_file_name}'
#         try:
#             # NOTE: this segment of code can lead to successful return statement
#             os.rename(full_local_name, correct_full_local_name)     # Does nothing if file is open, etc.
#             if verbose:
#                 print(f"\"As of\" correction needed! Renamed {full_local_name} to {correct_full_local_name}.")
#         except FileExistsError:
#             # No need to rename - file already exists
#             os.remove(full_local_name)
#             raise RuntimeError("Rename failed, file with \"as of\" date already exists.\n"
#                                "Fresh download has been deleted; directory is back to state prior to function call.")
#     return holdings, extra_info
