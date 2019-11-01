import pandas as pd
import numpy as np
from treasury_rates_reader import load_treasury_rates, get_treasury_rate


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
    data_copy[new_t_to_exp_col] = \
        (data[exp_date_col] - data[trade_date_col]).map(lambda td: td.days)/365
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
    tr_rates = load_treasury_rates()
    # Get unique set of trade date-days to expiration combinations
    data_indexed = data.set_index([trade_date_col, t_to_exp_col])
    unique_index = data_indexed.index.unique()
    trade_dates = unique_index.get_level_values(trade_date_col)
    t_to_exps = unique_index.get_level_values(t_to_exp_col)
    # Get corresponding unique set of Treasury rates
    unique_rates_list = [get_treasury_rate(tr_rates, trade_date, t_to_exp, time_in_years=True)/100
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
    :param price_col: column name of option premiums
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
    forward_df = k + np.exp(-r*t)*c_p
    data_indexed_orig_with_forward = data_indexed_orig.join(forward_df.rename(new_forward_col), how='inner')
    return data_indexed_orig_with_forward.reset_index()
