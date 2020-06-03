import pandas as pd
import numpy as np
from cboe_exchange_holidays_v3 import datelike_to_timestamp
from options_futures_expirations_v3 import last_friday

REASONABLE_DOLLAR_STRIKE_LIMIT = 300    # $300 on a $100 face value is pushing it
REASONABLE_DOLLAR_PRICE_LIMIT = 120     # $120 premium on $100 face value is pushing it
EOD_FILEDIR_TEMPLATE = 'P:/PrdDevSharedDB/CME Data/PURCHASED/{}y/EOD/Unzipped/'
EOD_FILENAME_TEMPLATE = '{}y_{}_EOD_raw_{}.csv'
FIVE_YEAR_SETTLEMENT_FORMAT_CHANGE_DATE = pd.Timestamp('2008-03-03')
RANDOM_BAD_E_SETTLEMENT_DATE_STR = '2017-08-28'
FIRST_E_DATE = pd.Timestamp('2016-02-25')


def _handle_expirations(data):
    """ Helper: Handle missing expirations
    :param data: DataFrame from read_eod_file
    :return: DataFrame with expiration dates completed
    """
    data_copy = data.copy()
    # Create helper column, useful if we must generate our own expiration dates
    data_copy['Contract Year-Month'] = \
        data.apply(lambda row: '{}-{:02d}'.format(row['Contract Year'],
                                                  row['Contract Month']), axis=1)
    # Ensure that expiration dates are available in 'Last Trade Date' column and formatted as Timestamps
    if data['Last Trade Date'].dropna().empty:
        # Calculate expiration dates manually
        contract_year_months = data_copy['Contract Year-Month'].unique()
        month_ofs = pd.to_datetime(contract_year_months) - pd.Timedelta(days=1)
        contract_year_month_exps = [last_friday(month_of) for month_of in month_ofs]
        contract_year_month_exps_df = \
            pd.DataFrame({'Contract Year-Month': contract_year_months,
                          'Last Trade Date': contract_year_month_exps})
        data_copy = (data_copy.drop('Last Trade Date', axis=1)
                     .merge(contract_year_month_exps_df, how='left', on='Contract Year-Month'))
    else:
        # Use given expiration dates
        data_copy['Last Trade Date'] = pd.to_datetime(data['Last Trade Date'].astype(str))
    return data_copy


def _handle_strikes(data):
    """ Helper: Handle bizarrely-formatted strikes
    :param data: DataFrame from read_eod_file
    :return: DataFrame with strikes normalized
    """
    data_copy = data.copy()
    max_strike = data['Strike Price'].max()     # Used as format indicator
    if max_strike > 10*REASONABLE_DOLLAR_STRIKE_LIMIT:
        # This indicates a special day (in only 5-year) on which a few rows have strikes
        # formatted with an additional "0" (e.g. "10300" instead of "1030", to mean "$103"),
        # suspected to provide Globex-specific trade info
        # NOTE: we drop these rows for now, due to uncertainty, as they either
        #       1) do not present new prices (only volumes) or 2) conflict with existing prices
        is_usable_strike = data['Strike Price'].map(lambda k: k <= 10*REASONABLE_DOLLAR_STRIKE_LIMIT)
        n_bad_strikes = (~is_usable_strike).sum()
        data_copy = data[is_usable_strike].reset_index(drop=True)
        print("WARNING: Suspicious strike rows encountered and dropped: {}.".format(n_bad_strikes))
    if max_strike > REASONABLE_DOLLAR_STRIKE_LIMIT:
        # Strike would never be above $300; add decimal point back in, since it appears
        # to be multiplied by 10x; this is necessary as 10-year sometimes does not require this
        # Also, fix bizarre strike formatting for 0.25 (10-year) and 0.125 (2-year) increments
        data_copy.loc[(data_copy['Strike Price'] % 10 == 2) |
                      (data_copy['Strike Price'] % 10 == 7), 'Strike Price'] += 0.5
        data_copy.loc[(data_copy['Strike Price'] % 10 == 1) |
                      (data_copy['Strike Price'] % 10 == 6), 'Strike Price'] += 0.25
        data_copy.loc[(data_copy['Strike Price'] % 10 == 3) |
                      (data_copy['Strike Price'] % 10 == 8), 'Strike Price'] += 0.75
        data_copy['Strike Price'] /= 10
    return data_copy


def _settlement_field_to_dollars(settlements, half_ticks=False):
    """ Utility: Convert CME EOD settlement price format into dollars
    :param settlements: prices in dollar-and-spare-ticks format, e.g. "841" means 8 + 41/64 dollars
    :param half_ticks: True means half ticks are possible (2- and some of 5-year), e.g. "8415"
                       means "841.5" which means 8 + 41.5/64 dollars
    :return: settlements converted into dollar
    """
    settlements_copy = settlements.copy()
    if half_ticks:
        settlements_copy /= 10
    whole_dollars = settlements_copy // 100
    spare_ticks = settlements_copy % 100
    return whole_dollars + spare_ticks/64


def _handle_pf_settlement_prices(data, tenor, trade_date_str):
    """ Helper: Handle bizarrely-formatted settlement prices for "p" and "f" files
        NOTE: this function is unable to detect the case of an unexpected whole number dollar value
              being interpreted as a number of ticks; that must be accounted for in final step
    :param data: DataFrame from read_eod_file
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_date_str: trade date as a string, e.g. '2019-03-21'
    :return: DataFrame with settlement prices in dollars
    """
    data_copy = data.copy()
    # Convert unexpected decimal prices that are clearly not in ticks direcly into dollars
    is_integer_settlement = data['Settlement'].astype(float).apply(float.is_integer)
    n_nontick_settlements = (~is_integer_settlement).sum()
    if n_nontick_settlements > 0:
        max_nontick_settlement = \
            data.loc[~is_integer_settlement, 'Settlement'].max()   # Used as format indicator
        if max_nontick_settlement > REASONABLE_DOLLAR_PRICE_LIMIT:
            # Option premium would never be above $120; move decimal based on empirical profiling
            if tenor in [2, 5]:
                data_copy.loc[~is_integer_settlement, 'Settlement'] /= 1000
            else:
                data_copy.loc[~is_integer_settlement, 'Settlement'] /= 100
        print("WARNING: Non-tick settlement price rows encountered and merged: {}."
              .format(n_nontick_settlements))
    # Convert whole ticks settlement prices into dollars
    trade_date = pd.Timestamp(trade_date_str)
    if tenor == 2 or (tenor == 5 and trade_date >= FIVE_YEAR_SETTLEMENT_FORMAT_CHANGE_DATE):
        # For 2-year (all dates) and for 5-year starting 2008-03-03, the last digit
        # of settlement ticks is actually a decimal (e.g. "1055" should be treated
        # as "105.5", i.e. 1 + 5.5/64 dollars)
        data_copy.loc[is_integer_settlement, 'Settlement'] = \
            _settlement_field_to_dollars(data.loc[is_integer_settlement, 'Settlement'],
                                         half_ticks=True)
    else:
        data_copy.loc[is_integer_settlement, 'Settlement'] = \
            _settlement_field_to_dollars(data.loc[is_integer_settlement, 'Settlement'],
                                         half_ticks=False)
    return data_copy


def _handle_e_2017_08_28(data, tenor):
    """ Utility: Really complicated logic to back out legitimate data from the
        extremely mishandled 2017-08-28 "e" files
        2- and 5-year conversion from "p" file settlement format to "e" is as follows:
            tens = p//1000, tens_remainder = p%1000
            ones = tens_remainder//64, ticks = ones%64
            e = tens*10 + ones + ticks*(25/1024)
        10- and 30-year conversion from "p" file settlement format to "e" is as follows:
            ones = p//100, ticks = p%100
            e = ones + ticks*(25/1024)
        We reverse these processes and convert directly to dollar
    :param data: DataFrame from read_eod_file
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :return: DataFrame with settlement prices in dollars
    """
    special_tick = 25/1024  # Used instead of 1/64 for some reason (it is (1/64)**2 * 100?)
    one_64th = 1/64     # Must convert these to 0 in 5-year
    one_128th = one_64th/2  # Must convert these to 0 in 2-year
    one_640th = one_64th/10     # Must convert these to 0 in 10- and 30-year

    # Back out the "whole dollars" and "whole ticks" which were used to create "e" prices
    # NOTE: these dollars and ticks are misnomers - they may neeed to be reformatted before dollar conversion
    prices = data['Settlement'].copy()
    prices[(prices == one_64th) | (prices == one_128th) | (prices == one_640th)] = 0  # Match 0s in "p" file
    # Case 1: ticks do not create an extra dollar
    prices_floor = np.floor(prices)
    prices_floor_remainder = prices - prices_floor
    possible_whole_ticks_1 = prices_floor_remainder / special_tick
    # Case 2: ticks create an extra dollar (64*special_tick = 1.5625, so 1 extra dollar possible)
    prices_floor_minus_one = prices_floor - 1
    prices_floor_minus_one[prices_floor_minus_one < 0] = 0
    prices_floor_minus_one_remainder = prices - prices_floor_minus_one
    possible_whole_ticks_2 = prices_floor_minus_one_remainder / special_tick
    # Extract whole number ticks
    whole_ticks = pd.Series(None, index=prices.index)
    whole_ticks_1_is_good = abs(possible_whole_ticks_1 - round(possible_whole_ticks_1)) < 0.0001
    whole_ticks_2_is_good = abs(possible_whole_ticks_2 - round(possible_whole_ticks_2)) < 0.0001
    whole_ticks[whole_ticks_1_is_good] = round(possible_whole_ticks_1[whole_ticks_1_is_good])
    whole_ticks[whole_ticks_2_is_good] = round(possible_whole_ticks_2[whole_ticks_2_is_good])
    # Check for errors
    bad_prices = prices[whole_ticks.isna()]
    if len(bad_prices) > 0:
        print("WARNING: Un-fixable prices found on 2017-08-28: {}.".format(bad_prices))
    # Get corresponding whole dollars
    whole_dollars = pd.Series(None, index=prices.index)
    whole_dollars[whole_ticks_1_is_good] = prices_floor[whole_ticks_1_is_good]
    whole_dollars[whole_ticks_2_is_good] = prices_floor_minus_one[whole_ticks_2_is_good]

    # Convert to actual dollars
    if tenor in [2, 5]:
        # 2- and 5-year require extra reformatting due to additional mishandling of half-ticks
        actual_dollars = whole_dollars.values // 10     # Using .values to sidestep PyCharm inspection
        actual_ticks = ((whole_dollars.values % 10)*64 + whole_ticks.values) / 10
    else:
        actual_dollars = whole_dollars.values
        actual_ticks = whole_ticks.values
    actual_prices = actual_dollars + actual_ticks * one_64th

    data_copy = data.copy()
    data_copy['Settlement'] = actual_prices
    return data_copy


def _handle_e_settlement_prices(data, tenor, trade_date_str):
    """ Helper: Handle bizarrely-formatted settlement prices for "e" files
    :param data: DataFrame from read_eod_file
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_date_str: trade date as a string, e.g. '2019-03-21'
    :return: DataFrame with settlement prices in dollars
    """
    if trade_date_str == RANDOM_BAD_E_SETTLEMENT_DATE_STR:
        return _handle_e_2017_08_28(data, tenor)
    else:
        return data


def _handle_duplicate_series(data):
    """ Helper: Handle duplicate series
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
                print("WARNING: Duplicates fixed for series {}, though MULTIPLE prices were reasonable ({})."
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


def _correct_price_against_standard(price, standard, half_ticks=False):
    """ Utility: Price repair error leeway logic for _repair_series (to prevent incorrect cascades)
    :param price: price in question
    :param standard: "correct" value against which price is compared
    :param half_ticks: True means half ticks are possible (2- and some of 5-year)
    :return: "correct" price, whether a multiplier is applied or not
    """
    if half_ticks:
        multiplier = 640    # Additional 10x multiplier due to half-tick formatting
    else:
        multiplier = 64
    if abs(price*multiplier - standard) < abs(price - standard):
        # Likely price was a whole dollar value mixed in with ticks (it is way too small);
        # restore price back to dollar
        return price*multiplier
    else:
        # Likely price was just priced badly; do not try to correct it
        return price


def _repair_series(prices, pc, half_ticks=False, verbose=False):
    """ Utility: Price repair logic for _repair_misinterpreted_whole_dollars()
    :param prices: series prices (indexed and sorted by strike)
    :param pc: 'C' for call, 'P' for put
    :param half_ticks: True means half ticks are possible (2- and some of 5-year)
    :param verbose: True prints every correction that is made
    :return: prices series with corrections made
    """
    # Fix prices one at a time
    prices_copy = prices.copy()
    if pc == 'C':
        ascending_prices = prices_copy[::-1]    # Call prices higher as strikes decrease
    else:
        ascending_prices = prices_copy
    # Get for each strike the price that should be just lower than its price
    prev_ascending_prices = ascending_prices.shift()
    # Find locations where pricing is inverted
    is_bad_price = ascending_prices < prev_ascending_prices
    bad_prices = ascending_prices[is_bad_price]
    n_bad_prices = len(bad_prices)
    acceptable_error_indexes_list = []
    while n_bad_prices > 0:
        # Correct smallest bad price, since cascade is possible
        bad_price = bad_prices.iloc[0]
        bad_price_index = bad_prices.index[0]
        standard = prev_ascending_prices[bad_price_index]
        corrected_price = _correct_price_against_standard(bad_price, standard, half_ticks)
        if corrected_price < standard:
            acceptable_error_indexes_list.append(bad_price_index)
            if verbose:
                print("_repair_series: Bad price {} left as is, though it should be greater than {}."
                      .format(bad_price, standard))
        else:
            ascending_prices.loc[bad_price_index] = corrected_price     # This propagates to prices_copy
            if verbose:
                print("_repair_series: Bad price {} corrected to {} since it must be greater than {}."
                      .format(bad_price, corrected_price, standard))
        prev_ascending_prices = ascending_prices.shift()
        is_bad_price = ascending_prices.lt(prev_ascending_prices)
        for index in acceptable_error_indexes_list:
            is_bad_price[index] = False
        bad_prices = ascending_prices[is_bad_price]
        n_bad_prices = len(bad_prices)
    return prices_copy


def _repair_misinterpreted_whole_dollars(data, tenor, trade_date_str):
    """ Helper: Repair prices that were originally (unexpectedly) whole dollars and thus
        mistaken to be in ticks format and converted into significantly lower prices
    :param data: DataFrame from read_eod_file
    :return: DataFrame with repaired prices
    """
    trade_date = pd.Timestamp(trade_date_str)
    if tenor == 2 or (tenor == 5 and trade_date >= FIVE_YEAR_SETTLEMENT_FORMAT_CHANGE_DATE):
        half_ticks = True
    else:
        half_ticks = False
    exps = data['Last Trade Date'].unique()
    data_indexed = data.set_index(['Last Trade Date', 'Put/Call', 'Strike Price']).sort_index()
    total_corrections = 0
    # Address strike range of each series in isolation
    for exp in exps:
        for pc in ['C', 'P']:
            try:
                prices = data_indexed.loc[(exp, pc), 'Settlement']
            except KeyError:
                continue    # Apparently no calls or no puts exist for this expiry
            if len(prices) < 2:
                continue    # Cannot evaluate if only 1 price or none
            repaired_prices = _repair_series(prices, pc, half_ticks, verbose=True)
            # Save and log differences
            diff_indexes = prices[~prices.eq(repaired_prices)].index
            n_corrections = len(diff_indexes)
            if n_corrections > 0:
                data_indexed.loc[(exp, pc, diff_indexes), 'Settlement'] = \
                    repaired_prices[diff_indexes].values
            total_corrections += n_corrections
    if total_corrections > 0:
        print("WARNING: Misinterpreted whole dollar settlement prices identified and repaired: {}."
              .format(total_corrections))
    return data_indexed.reset_index()


def read_cme_file(tenor, trade_datelike, letter='e', file_dir=None, file_name=None, verbose=True):
    """ Read CME EOD Treasury files from disk and load them into consistently formatted DataFrames
    :param tenor: 2, 5, 10, or 30 (2-, 5-, 10-, 30-year Treasury options)
    :param trade_datelike: trade date as date object or string, e.g. '2019-03-21'
    :param letter: 'e' (available starting 2019-02-25), 'p', or 'f'
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :param verbose: set True to print name of file read
    :return: pd.DataFrame with consistent and labeled columns
    """
    # Ensure string version of trade date is available
    trade_date = datelike_to_timestamp(trade_datelike)
    trade_date_str = trade_date.strftime('%Y-%m-%d')
    # Raise error if 'e' file did not yet exist
    if trade_date < FIRST_E_DATE:
        raise ValueError("CME did not produce 'e' files until 2016-02-25.")
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
    if verbose:
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
    # Repair misinterpreted unexpected
    data = _repair_misinterpreted_whole_dollars(data, tenor, trade_date_str)
    # # Adjust 0-prices to be NaN instead - they are not really legitimate for use
    # data.loc[data['Settlement'] == 0, 'Settlement'] = None

    return data
