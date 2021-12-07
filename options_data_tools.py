import pandas as pd
import numpy as np
from treasury_rates_reader import load_treasury_rates, pull_treasury_rates, get_rate
from options_futures_expirations_v3 import DAY_OFFSET, DAY_NAME_TO_WEEKDAY_NUMBER_DICT, ensure_bus_day


def remove_duplicate_series(data, trade_date_col='trade_date', exp_date_col='exp_date',
                            strike_col='strike', cp_col='cp', volume_col='volume'):
    """ Remove duplicate series, ideally retaining series with highest volume
        NOTE: this is necessary to avoid unleashing permutations when duplicate
              series serve as indexes for join operations down the line
    :param data: unindexed input DataFrame containing all fields
    :param trade_date_col: column name of trade/quote dates
    :param exp_date_col: column name of expiration dates
    :param strike_col: column name of strikes
    :param cp_col: column name of call-put indicator (can be Boolean or 'C' and 'P')
    :param volume_col: column name of volume traded; set None if volume not available
    :return: unindexed DataFrame that is copy of input with no duplicate series
    """
    series_cols = [trade_date_col, exp_date_col, strike_col, cp_col]
    if volume_col is None:
        # Arbitrarily retain last series
        return data.drop_duplicates(series_cols, keep='last')
    else:
        # Sort by ascending volume, then retain last series
        return (data.sort_values(series_cols + [volume_col])
                    .drop_duplicates(series_cols, keep='last'))


def add_t_to_exp(data, trade_date_col='trade_date', exp_date_col='exp_date',
                 new_t_to_exp_col='t_to_exp'):
    """ Calculate time to expiration and add it to DataFrame
        NOTE: outputs are in YEARS to facilitate options-related calculations
    :param data: unindexed input DataFrame containing all fields
    :param trade_date_col: column name of trade/quote dates
    :param exp_date_col: column name of expiration dates
    :param new_t_to_exp_col: name for time to expiration column generated by this function
    :return: unindexed DataFrame that is copy of input plus days to expiration column
    """
    data_copy = data.copy()
    data_copy[new_t_to_exp_col] = (data[exp_date_col] - data[trade_date_col]) / pd.Timedelta(days=365)
    return data_copy


def add_rate(data, trade_date_col='trade_date', t_to_exp_col='t_to_exp',
             new_rate_col='rate'):
    """ Get Treasury rate (as risk-free rate) and add it to DataFrame
        NOTE: output rates are NOT IN PERCENT, i.e. exp(rate) is the factor, to
              facilitate options-related calculations
    :param data: unindexed input DataFrame containing all fields
    :param trade_date_col: column name of trade/quote dates
    :param t_to_exp_col: column name of numerical time (years) to expiration
    :param new_rate_col: name for rate column generated by this function
    :return: unindexed DataFrame that is copy of input plus rate column
    """
    # Get unique set of trade date-days to expiration combinations
    data_indexed = data.set_index([trade_date_col, t_to_exp_col])
    unique_index = data_indexed.index.unique()
    trade_dates = unique_index.get_level_values(trade_date_col)
    t_to_exps = unique_index.get_level_values(t_to_exp_col)
    # Get rates, automatically pulling fresh rates if needed
    tr_rates = load_treasury_rates()    # First try using local rates
    max_data_date, max_loaded_date = max(trade_dates.unique()), max(tr_rates.index)
    if max_data_date > max_loaded_date:
        print(f"Requested rate {max_data_date.strftime('%Y-%m-%d')} beyond "
              f"latest local rate {max_loaded_date.strftime('%Y-%m-%d')}. Pulling fresh CMT yields...")
        tr_rates = pull_treasury_rates()
    # Get corresponding unique set of Treasury rates
    unique_rates_list = [get_rate(trade_date, t_to_exp, tr_rates, time_in_years=True)/100
                         for trade_date, t_to_exp, in zip(trade_dates, t_to_exps)]
    unique_rates_df = pd.DataFrame({new_rate_col: unique_rates_list}, index=unique_index)
    # Join unique rates back to full data
    data_indexed_with_rate = data_indexed.join(unique_rates_df, how='left')
    return data_indexed_with_rate.reset_index()


def add_forward(data, trade_date_col='trade_date', exp_date_col='exp_date', strike_col='strike',
                cp_col='cp', price_col='price', t_to_exp_col='t_to_exp', rate_col='rate',
                new_forward_col='forward'):
    """ Calculate forward price using put-call parity and add it to DataFrame
        NOTE: series for which forward cannot be computed are dropped from output
    :param data: unindexed input DataFrame containing all fields
    :param trade_date_col: column name of trade/quote dates
    :param exp_date_col: column name of expiration dates
    :param strike_col: column name of strikes
    :param cp_col: column name of call-put indicator (can be Boolean or 'C' and 'P')
    :param price_col: column name of options premiums
    :param t_to_exp_col: column name of numerical time (years) to expiration
    :param rate_col: column name of risk-free rates (NOT in percent)
    :param new_forward_col: name for forward column generated by this function
    :return: unindexed DataFrame that is copy of input plus forward column
    """
    data_indexed_orig = data.set_index([trade_date_col, exp_date_col, strike_col]).sort_index()
    # Avoid using rows with price of 0 - they are meaningless and throw off forward determination
    data_indexed = data_indexed_orig[data_indexed_orig[price_col] > 0]
    if data_indexed_orig.shape != data_indexed.shape:
        print("WARNING add_forward(): Prices of 0 exist in data and will be ignored.\n"
              "                       However, attention is recommended as prices of 0 are generally bad.")
    # Create DataFrame with only strikes with both call and put (need both for forward)
    if data_indexed[cp_col].dtypes == bool:
        # Boolean style: True means "call", False means "put"
        is_call = data_indexed[cp_col]
    else:
        # String style: 'C' means "call", 'P' means "put"
        is_call = data_indexed[cp_col] == 'C'
    calls = data_indexed.loc[is_call]
    puts = data_indexed.loc[~is_call]
    if calls.index.duplicated().sum() > 0 or puts.index.duplicated().sum() > 0:
        print("ERROR add_forward(): Duplicate series exist in data.")
        return data
    cp_df = calls[[price_col]].join(puts[[price_col]], how='inner', lsuffix='_C', rsuffix='_P')
    # Determine current strikes for each series at which call price and put price are closest
    cp_df['c_minus_p'] = cp_df[price_col+'_C'] - cp_df[price_col+'_P']
    cp_df['abs_c_minus_p'] = cp_df['c_minus_p'].abs()
    cp_df_noindex = cp_df.reset_index()
    min_abs_c_minus_p_idx = cp_df_noindex.groupby([trade_date_col, exp_date_col])['abs_c_minus_p'].idxmin()
    c_minus_p_min_df = (cp_df_noindex.loc[min_abs_c_minus_p_idx]
                                     .set_index([trade_date_col, exp_date_col, strike_col]))
    # Join time to expiration and risk-free rate back in
    c_minus_p_min_df = (c_minus_p_min_df.join(calls[[t_to_exp_col, rate_col]], how='left')
                                        .reset_index(strike_col))
    # Calculate forward and inner join it back to full data
    k = c_minus_p_min_df[strike_col]
    r = c_minus_p_min_df[rate_col]
    t = c_minus_p_min_df[t_to_exp_col]
    c_p = c_minus_p_min_df['c_minus_p']
    forward_df = k + np.exp(r*t)*c_p
    data_indexed_orig_with_forward = data_indexed_orig.join(forward_df.rename(new_forward_col), how='inner')
    return data_indexed_orig_with_forward.reset_index()


def lookup_val_in_col(data, lookup_val, lookup_col, exact_only=False, leq_only=False, groupby_cols=None):
    """ Return row (of first occurrence, if multiple) of nearest value in selected column
        NOTE: operates per aggregation group if groupby_cols parameter is used
    :param data: unindexed input DataFrame containing all fields
    :param lookup_val: value to look for in column
    :param lookup_col: column to look in
    :param exact_only: set True if only exact value match is desired
    :param leq_only: set True if only less than or equal match is desired (e.g. strike just below forward price)
    :param groupby_cols: use instead of df.groupby(groupby_cols).apply(lambda data: lookup_val_in_col(...))
    :return: row (per aggregation, if applicable) containing column value that matches lookup value;
             if multiple matches, only first occurrence; if exact_only and no exact match, empty
    """
    if exact_only:
        # Simple process for exact matches
        exact_matches = data[data[lookup_col] == lookup_val]
        if groupby_cols is not None:
            return exact_matches.groupby(groupby_cols).first()
        else:
            return exact_matches.iloc[0].copy() if not exact_matches.empty else exact_matches.copy()
    else:
        if leq_only:
            data = data[data[lookup_col] <= lookup_val]     # Essentially leq_matches
        # Find index(es) of minimum difference between lookup column and lookup value
        col_val_abs_diff = (data[lookup_col] - lookup_val).abs()
        if groupby_cols is not None:
            # Aggregate by groupby_cols
            data_copy = data.copy()
            data_copy['col_val_abs_diff'] = col_val_abs_diff
            nearest_val_idxs = data_copy.groupby(groupby_cols)['col_val_abs_diff'].idxmin()
            return data.loc[nearest_val_idxs].set_index(groupby_cols)
        else:
            # No need to aggregate
            nearest_val_idx = col_val_abs_diff.idxmin()
            return data.loc[nearest_val_idx].copy()


def change_weekday(data, date_col, old_weekday, new_weekday,
                   do_ensure_bus_day=False, ensure_bus_day_shift_to='prev', verbose=False):
    """ Change weekday of given date column to another weekday in the same (Monday-Sunday) week
    :param data: input DataFrame containing at least date_col
    :param date_col: column name of column containing dates
    :param old_weekday: weekday to change; can be 0-6 or 'Monday'-'Sunday', i.e. number or string
    :param new_weekday: weekday to change to; can be 0-6 or 'Monday'-'Sunday', i.e. number or string
    :param do_ensure_bus_day: set True to ensure that all new dates are business days
    :param ensure_bus_day_shift_to: if do_ensure_bus_day is True, set 'prev' or 'next' to indicate
                                    which business day to correct to
    :param verbose: set True to print a DataFrame showing changes
    :return: DataFrame that is copy of input with date_col modified
    """
    if not data.index.is_unique:
        raise ValueError("input DataFrame has non-unique index. this is generally problematic")
    if isinstance(old_weekday, str):
        old_weekday = DAY_NAME_TO_WEEKDAY_NUMBER_DICT[old_weekday]
    if isinstance(new_weekday, str):
        new_weekday = DAY_NAME_TO_WEEKDAY_NUMBER_DICT[new_weekday]
    # Find dates in need of change
    date_col_data = data[date_col]  # Slice of the original, which will go unmodified
    dates_to_change = date_col_data[date_col_data.dt.weekday == old_weekday]
    change_index = dates_to_change.index
    # Apply changes to copy of DataFrame
    data_copy = data.copy()
    n_days_shift = new_weekday - old_weekday
    data_copy.loc[change_index, date_col] += n_days_shift*DAY_OFFSET
    if do_ensure_bus_day:
        data_copy.loc[change_index, date_col] = \
            ensure_bus_day(data_copy.loc[change_index, date_col], ensure_bus_day_shift_to)
    if verbose:
        change_weekday_df = pd.DataFrame({'old_dates': dates_to_change,
                                          'new_dates': data_copy.loc[change_index, date_col]})
        print(change_weekday_df)
    print(f"{len(change_index):,} changes, {len(dates_to_change.unique()):,} unique")
    return data_copy
