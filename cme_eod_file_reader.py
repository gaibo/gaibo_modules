import pandas as pd
from options_futures_expirations_v2 import last_friday

TICKSIZE = 0.015625     # 1/64
REASONABLE_DOLLAR_STRIKE_LIMIT = 300    # $300 on a $100 face value is pushing it
REASONABLE_DOLLAR_PRICE_LIMIT = 120     # $120 premium on $100 face value is pushing it
EOD_FILEDIR_TEMPLATE = 'P:/PrdDevSharedDB/CME Data/{}Y/EOD/Unzipped/'
EOD_FILENAME_TEMPLATE = '{}y_{}_EOD_raw_{}.csv'
FIVE_YEAR_SETTLEMENT_FORMAT_CHANGE_DATE = pd.Timestamp('2008-03-03')
TWO_FIVE_YEAR_RANDOM_BAD_E_SETTLEMENT_DATE_STR = '2017-08-28'


def _handle_expirations(data):
    """ Helper function for handling missing expirations
    :param data: DataFrame from read_eod_file
    :return: DataFrame with expiration dates completed
    """
    # Create helper column, useful if we must generate our own expiration dates
    data['Contract Year-Month'] = \
        data.apply(lambda row: '{}-{:02d}'.format(row['Contract Year'],
                                                  row['Contract Month']), axis=1)
    # Ensure that expiration dates are available in 'Last Trade Date' column and formatted as Timestamps
    if data['Last Trade Date'].dropna().empty:
        # Calculate expiration dates manually
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
    return data


def _handle_strikes(data):
    """ Helper function for handling bizarrely-formatted strikes
    :param data: DataFrame from read_eod_file
    :return: DataFrame with strikes normalized
    """
    max_strike = data['Strike Price'].max()     # Used as format indicator
    if max_strike > 10*REASONABLE_DOLLAR_STRIKE_LIMIT:
        # This indicates a special day (in only 5-year) on which a few rows have strikes
        # formatted with an additional "0" (e.g. "10300" instead of "1030", to mean "$103"),
        # suspected to provide Globex-specific trade info
        # NOTE: we drop these rows for now, due to uncertainty, as they either
        #       1) do not present new prices (only volumes) or 2) conflict with existing prices
        is_usable_strike = data['Strike Price'].map(lambda k: k <= 10*REASONABLE_DOLLAR_STRIKE_LIMIT)
        n_bad_strikes = (~is_usable_strike).sum()
        data = data[is_usable_strike].reset_index(drop=True)
        print("WARNING: Dropped {} suspicious strike row(s).".format(n_bad_strikes))
    if max_strike > REASONABLE_DOLLAR_STRIKE_LIMIT:
        # Strike would never be above $300; add decimal point back in, since it appears
        # to be multiplied by 10x; this is necessary as 10-year sometimes does not require this
        # Also, fix bizarre strike formatting for 0.25 (10-year) and 0.125 (2-year) increments
        data.loc[(data['Strike Price'] % 10 == 2) |
                 (data['Strike Price'] % 10 == 7), 'Strike Price'] += 0.5
        data.loc[(data['Strike Price'] % 10 == 1) |
                 (data['Strike Price'] % 10 == 6), 'Strike Price'] += 0.25
        data.loc[(data['Strike Price'] % 10 == 3) |
                 (data['Strike Price'] % 10 == 8), 'Strike Price'] += 0.75
        data['Strike Price'] /= 10
    return data


def _handle_pf_settlement_prices(data, tenor, trade_date_str):
    """ Helper function for handling bizarrely-formatted settlement prices for "p" and "f" files
    :param data: DataFrame from read_eod_file
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_date_str: trade date as a string, e.g. '2019-03-21'
    :return: DataFrame with settlement prices in dollars
    """
    # Repair settlement prices that are clearly not in whole ticks direcly into dollars
    is_integer_settlement = data['Settlement'].astype(float).apply(float.is_integer)
    n_bad_settlements = (~is_integer_settlement).sum()
    if n_bad_settlements > 0:
        max_nontick_settlement = data.loc[~is_integer_settlement, 'Settlement'].max()   # Used as format indicator
        if max_nontick_settlement > REASONABLE_DOLLAR_PRICE_LIMIT:
            # Option premium would never be above $120; move decimal based on empirical profiling
            if tenor in [2, 5]:
                data.loc[~is_integer_settlement, 'Settlement'] /= 1000
            else:
                data.loc[~is_integer_settlement, 'Settlement'] /= 100
        print("WARNING: Encountered {} non-tick settlement price row(s). "
              "Tried to convert them all to dollars.".format(n_bad_settlements))
    # Convert whole ticks settlement prices into dollars
    trade_date = pd.Timestamp(trade_date_str)
    if tenor == 2 or (tenor == 5 and trade_date >= FIVE_YEAR_SETTLEMENT_FORMAT_CHANGE_DATE):
        # For 2-year (all dates) and for 5-year starting 2008-03-03, the last digit
        # of settlement ticks is actually a decimal (e.g. "1055" means "105.5 ticks of 64th")
        data.loc[is_integer_settlement, 'Settlement'] *= 0.1 * TICKSIZE
    else:
        data.loc[is_integer_settlement, 'Settlement'] *= TICKSIZE
    return data


def _handle_e_settlement_prices(data, tenor, trade_date_str):
    """ Helper function for handling bizarrely-formatted settlement prices for "e" files
    :param data: DataFrame from read_eod_file
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_date_str: trade date as a string, e.g. '2019-03-21'
    :return: DataFrame with settlement prices in dollars
    """
    if (tenor == 2 or tenor == 5) and trade_date_str == TWO_FIVE_YEAR_RANDOM_BAD_E_SETTLEMENT_DATE_STR:
        data['Settlement'] /= 10
        print("WARNING: This day ({}) has unexplainable 'e' prices for 2- and 5-year. "
              "Settlements do not match to whole ticks and seem to be 10x those of 'p'/'f'."
              .format(TWO_FIVE_YEAR_RANDOM_BAD_E_SETTLEMENT_DATE_STR))
    return data


def _handle_duplicate_series(data):
    """ Helper function for handling duplicate series
    :param data: DataFrame from read_eod_file
    :return: DataFrame with no duplicate series
    """
    data_indexed = data.set_index(['Last Trade Date', 'Put/Call', 'Strike Price']).sort_index()
    # Find indexes where there are duplicates
    # NOTE: .unique() is necessary since multiple dupes per series is possible
    dupe_indexes = data_indexed.index[data_indexed.index.duplicated()].unique()
    if len(dupe_indexes) == 0:
        # Remove DataFrame indexing and retain its sorting, for aesthetic consistency
        return data_indexed.reset_index()
    else:
        # Examine each series where duplicates exist
        for dupe_index in dupe_indexes:
            dupe_exp_pc = dupe_index[0:2]
            dupe_pc = dupe_index[1]
            dupe_strike = dupe_index[2]
            # Get all potential series prices
            dupe_prices = data_indexed.loc[dupe_index, 'Settlement']
            # Get neighboring strikes' prices (avoid considering neighboring duplicates)
            neighboring_prices = data_indexed.loc[dupe_exp_pc, 'Settlement']
            neighboring_prices = neighboring_prices.loc[neighboring_prices.index.drop_duplicates(False)]
            try:
                dupe_prev_price = neighboring_prices[neighboring_prices.index < dupe_strike].iloc[-1]
            except IndexError:
                # No previous price found - create upper or lower bound depending on call or put
                if dupe_pc == 'C':
                    dupe_prev_price = REASONABLE_DOLLAR_PRICE_LIMIT
                else:
                    dupe_prev_price = 0
            try:
                dupe_next_price = neighboring_prices[neighboring_prices.index > dupe_strike].iloc[0]
            except IndexError:
                # No next price found - create upper or lower bound depending on call or put
                if dupe_pc == 'C':
                    dupe_next_price = 0
                else:
                    dupe_next_price = REASONABLE_DOLLAR_PRICE_LIMIT
            if dupe_pc == 'C':
                # For calls, lower strikes have higher prices
                good_prices = dupe_prices[(dupe_prices >= dupe_next_price) &
                                          (dupe_prices <= dupe_prev_price)].drop_duplicates()
            else:
                # For puts, higher strikes have higher prices
                good_prices = dupe_prices[(dupe_prices >= dupe_prev_price) &
                                          (dupe_prices <= dupe_next_price)].drop_duplicates()
            n_good_prices = good_prices.count()
            # If no reasonable prices found, remove all such series
            if n_good_prices == 0:
                print("WARNING: Duplicates found for series {} but NONE of the prices were reasonable."
                      .format(dupe_index))
                data_indexed = data_indexed.drop(dupe_index)    # Drop all
                continue
            # If 1 or more reasonable prices found, retain 1
            if n_good_prices > 1:
                print("WARNING: Duplicates found for series {} and MULTIPLE prices were reasonable ({})."
                      .format(dupe_index, n_good_prices))
            else:
                print("WARNING: Duplicates fixed for series {}.".format(dupe_index))
            if dupe_pc == 'C':
                # Choose max, since next strike could also be a duplicate and lower priced
                data_indexed.loc[dupe_index, 'Settlement'] = good_prices.max()
            else:
                # Choose min, since next strike could also be a duplicate and higher priced
                data_indexed.loc[dupe_index, 'Settlement'] = good_prices.min()
        # Earlier we overwrote corrected values to all duplicate indexes, so only retain first,
        # then remove DataFrame indexing, then reset number index
        return data_indexed.loc[~data_indexed.index.duplicated()].reset_index().reset_index(drop=True)


def read_eod_file(tenor, trade_date_str, letter, file_dir=None, file_name=None):
    """ Read CME EOD Treasury files from disk and load them into consistently formatted DataFrames
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
    e_fields = ['Last Trade Date', 'Put/Call', 'Strike Price',
                'Settlement', 'Contract Year', 'Contract Month']
    pf_fields = ['Last Trade Date', 'Put/Call', 'Strike Price',
                 'Settlement', 'Open Interest', 'Total Volume',
                 'Delta', 'Implied Volatility', 'Contract Year', 'Contract Month']
    if letter == 'e':
        # 'e' has fewer usable fields
        data = pd.read_csv(file_dir + file_name,
                           usecols=e_fields)[e_fields]  # Enforce column ordering
    else:
        data = pd.read_csv(file_dir + file_name,
                           usecols=pf_fields)[pf_fields]    # Enforce column ordering
    print(file_name + " read.")

    # Clean data
    # Handle missing expiration dates
    data = _handle_expirations(data)
    # Handle erratically-formatted strike field
    data = _handle_strikes(data)
    # Handle erratically-formatted settlement price field
    if letter == 'e':
        data = _handle_e_settlement_prices(data, tenor, trade_date_str)
    else:
        data = _handle_pf_settlement_prices(data, tenor, trade_date_str)
    # Handle duplicate series
    data = _handle_duplicate_series(data)

    return data
