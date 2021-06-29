import pandas as pd
from sklearn.linear_model import LinearRegression

BUS_DAYS_IN_MONTH = 21
BUS_DAYS_IN_YEAR = 252
BUS_DAYS_IN_SIX_MONTHS = 126
ONE_DAY = pd.Timedelta(days=1)
ONE_NANOSECOND = pd.Timedelta(nanoseconds=1)


def construct_timeseries(ts_data, time_col=None, value_col=None, index_is_time=False, ensure_dates=True):
    """ Construct uniform time-series object, i.e. a pandas Series with 'time' and 'value'
    :param ts_data: DataFrame or Series object with at least a time column and a value column
    :param time_col: name of DataFrame column that represents time
    :param value_col: name of DataFrame column that represents value
    :param index_is_time: set True if index of ts_data is the desired time column
    :param ensure_dates: set True to ensure 'time' index is DatetimeIndex
    :return: pd.Series object with 'time' as index name and 'value' as name
    """
    ts = ts_data.copy()     # Avoid modifying original data
    if isinstance(ts, pd.DataFrame):
        # Find the 2 relevant columns
        n_cols = ts.shape[1]
        if n_cols == 0:
            raise ValueError("empty DataFrame (0 columns)")
        if n_cols == 1:
            ts = ts.squeeze()   # Essentially a glorified Series
        else:
            # Extract 'time' from columns/index
            if index_is_time:
                time_extract = ts.index
            else:
                if time_col is not None:
                    if time_col in ts.columns:
                        time_extract = ts[time_col]
                    elif ts.index.name == time_col:
                        time_extract = ts.index     # User did not flag index_is_time, but okay
                    else:
                        raise ValueError(f"'{time_col}' column for 'time' not found in data")
                else:
                    time_extract = ts.iloc[:, 0]    # Arbitrarily take first column as 'time'
            # Extract 'value' from columns/index
            if value_col is not None:
                if value_col in ts.columns:
                    value_extract = ts[value_col]
                elif ts.index.name == value_col:
                    value_extract = ts.index    # Indexed by value is weird, but okay
                else:
                    raise ValueError(f"'{value_col}' column for 'value' not found in data")
            else:
                value_extract = ts.iloc[:, 1]  # Arbitrarily take second column as 'value'
            # Stitch together into Series
            ts = pd.Series(value_extract.values, index=time_extract)
    elif not isinstance(ts, pd.Series):
        raise ValueError(f"expected pd.Series or pd.DataFrame, not '{type(ts)}'")
    # Rename
    ts.index.name, ts.name = 'time', 'value'    # ts is at this point a Series
    # Check 'time'
    if not isinstance(ts.index, pd.DatetimeIndex):
        if ensure_dates:
            ts.index = pd.to_datetime(ts.index)
        else:
            print(f"WARNING: time-series index type '{ts.index.inferred_type}', not 'datetime64'")
    # Drop NaNs and sort
    return ts.dropna().sort_index()


def share_dateindex(timeseries_list):
    """ Align a list of time-series by their shared date-times
    :param timeseries_list: list of time-series
    :return: list of aligned/truncated time-series
    """
    column_list = range(len(timeseries_list))
    combined_df = pd.DataFrame(dict(zip(column_list, timeseries_list))).dropna()
    return [combined_df[column].rename('value') for column in column_list]


def get_best_fit(x_data, y_data, fit_intercept=True):
    """ Find line of best fit for x and y data using linear regression
    :param x_data: first set of data (does not need to match y_data in date index)
    :param y_data: second set of data
    :param fit_intercept: whether to fit an intercept (set False to force 0)
    :return: tuple - (R^2, slope of line, best fit model)
    """
    [joined_x_data, joined_y_data] = share_dateindex([x_data, y_data])
    x = joined_x_data.values.reshape(-1, 1)
    y = joined_y_data.values
    model = LinearRegression(fit_intercept=fit_intercept).fit(x, y)
    r_sq = model.score(x, y)
    slope = model.coef_[0]
    return r_sq, slope, model


def chop_segments_off_string(s, delimiter='_', n_segments=4, from_direction='end'):
    """ Crop from given string a number of segments from start or end
        NOTE: for future versions, consider passing arrays of corresponding parameters
        NOTE: current functionality is conservative with error handling
    :param s: the string to chop off of, from which substring is obtained
    :param delimiter: 'fdsa_rewqf_fdsa' can be delimited by '_' into 3 segments
    :param n_segments: number of segments to remove from substring
    :param from_direction: 'start' to chop from beginning of string, 'end' from end
    :return: substring; original string if number of segments to crop is too many
    """
    s_list = s.split(delimiter)
    if len(s_list) <= n_segments:
        return s    # Return original string to prevent unintentional loss of info
    else:
        # Concat important parts for identification
        if from_direction == 'end':
            return delimiter.join(s_list[:-n_segments])
        elif from_direction == 'start':
            return delimiter.join(s_list[n_segments:])
        else:
            raise ValueError(f"Cannot recognize from_direction=\"{from_direction}\"; must be 'end' or 'start'")
