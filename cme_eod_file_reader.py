import pandas as pd
from options_futures_expirations_v2 import last_friday

TICKSIZE = 0.015625     # 1/64
EOD_FILEDIR_TEMPLATE = 'P:/PrdDevSharedDB/CME Data/{}Y/EOD/Unzipped/'
EOD_FILENAME_TEMPLATE = '{}y_{}_EOD_raw_{}.csv'


def read_eod_file(tenor, trade_date_str, letter, file_dir=None, file_name=None):
    """Read CME EOD Treasury files from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_date_str: trade date as a string, e.g. '2019-03-21'
    :param letter: 'e' (available starting 2019-02-25), 'p', or 'f'
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :return: pd.DataFrame with consistent and labeled columns
    """
    # Use default directory and file name templates
    if file_dir is None:
        file_dir = EOD_FILEDIR_TEMPLATE.format(tenor)
    if file_name is None:
        file_name = EOD_FILENAME_TEMPLATE.format(tenor, trade_date_str, letter)

    # Load raw data file
    basic_fields = ['Contract Year', 'Contract Month', 'Last Trade Date',
                    'Put/Call', 'Strike Price', 'Settlement']
    extra_fields = ['Open Interest', 'Total Volume', 'Delta', 'Implied Volatility']
    if letter == 'e':
        # 'e' has fewer usable fields
        data = pd.read_csv(file_dir + file_name,
                           usecols=basic_fields)
    else:
        data = pd.read_csv(file_dir + file_name,
                           usecols=basic_fields+extra_fields)
    print(file_name + " read.")

    # Clean and filter data

    # Create helper column, useful if we must generate our own expiration dates
    data['Contract Year-Month'] = \
        data.apply(lambda row: '{}-{:02d}'.format(row['Contract Year'],
                                                  row['Contract Month']), axis=1)
    # Ensure that expiration dates are available in 'Last Trade Date' column and formatted as Timestamps
    if data['Last Trade Date'].dropna().empty:
        # Generate expiration dates manually
        contract_year_months = data['Contract Year-Month'].unique()
        month_ofs = pd.to_datetime(contract_year_months) - pd.Timedelta(days=1)
        contract_year_month_exps = [last_friday(month_of) for month_of in month_ofs]
        contract_year_month_exps_df = \
            pd.DataFrame({'Contract Year-Month': contract_year_months,
                          'Last Trade Date': contract_year_month_exps})
        data = (data.drop('Last Trade Date', axis=1)
                    .merge(contract_year_month_exps_df, how='left', on='Contract Year-Month'))
    else:
        # Use given expiration dates
        data['Last Trade Date'] = pd.to_datetime(data['Last Trade Date'].astype(str))

    # Handle erratically formatted strike field
    if data['Strike Price'].max() > 300:
        # Strike would never be above 300; must add decimal point back in (10-year sometimes formats otherwise)
        # Fix bizarre strike formatting for 0.25 (10-year) and 0.125 (2-year) increments as well
        data.loc[(data['Strike Price'] % 10 == 2) |
                 (data['Strike Price'] % 10 == 7), 'Strike Price'] += 0.5
        data.loc[(data['Strike Price'] % 10 == 1) |
                 (data['Strike Price'] % 10 == 6), 'Strike Price'] += 0.25
        data.loc[(data['Strike Price'] % 10 == 3) |
                 (data['Strike Price'] % 10 == 8), 'Strike Price'] += 0.75
        data['Strike Price'] /= 10

    # ['f'/'p' FILE ONLY] Convert settlement price from ticks of 1/64 to dollars
    if letter in ['f', 'p']:
        if tenor in [2, 5]:
            # Note that for 2-year and 5-year, last digit of settle ticks is a 0.5 not 5
            data['Settlement'] *= 0.1 * TICKSIZE
        else:
            data['Settlement'] *= TICKSIZE
    return data
