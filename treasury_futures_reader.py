import pandas as pd

TREASURY_FUT_CSV_FILEDIR = 'P:/PrdDevSharedDB/BBG Pull Scripts/'
TREASURY_FUT_CSV_FILENAME = 'treasury_futures_pull.csv'
TENOR_TO_CODE_DICT = {2: 'TU', 5: 'FV', 10: 'TY', 30: 'US'}
EXPMONTH_CODES_DICT = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                       7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}


def month_to_quarter_shifter(month, shift=-1):
    """ Obtain any quarterly month given an input month using flexible shifting
        Flexibility of this function lies in experimenting with the shift parameter, e.g.:
        - shift=-1 (default) returns [3,  3,  3,  6,  6,  6,  9,  9,  9, 12, 12, 12]
        - shift=0 returns            [3,  3,  6,  6,  6,  9,  9,  9, 12, 12, 12,  3]
        - shift=2 returns            [6,  6,  6,  9,  9,  9, 12, 12, 12,  3,  3,  3]
    :param month: input month number(s); arrays above are returned when np.arange(1, 13) is inputted
    :param shift: see explanation above
    :return: "shifted" quarterly month number(s)
    """
    return ((month+shift) // 3 % 4 + 1) * 3


def undl_fut_quarter_month(opt_contr_month):
    """ Find the Treasury future month underlying the Treasury option month
    :param opt_contr_month: numerical month of the options month code;
                            note that for example, September options (U) actually expire
                            in August, but here would be referred to as 9 instead of 8
    :return: numerical month of the quarterly futures (can be used with EXPMONTH_CODES_DICT)
    """
    # For actual month of expiration date, use: month_to_quarter_shifter(opt_exp_month, shift=0)
    return (((opt_contr_month-1) // 3) + 1) * 3     # month_to_quarter_shifter(opt_contr_month, shift=-1)


def load_fut_prices(file_dir=None, file_name=None):
    """ Read Treasury futures prices from disk and load them into a DataFrame
    :param file_dir: optional directory to search for data file (overrides default directory)
    :param file_name: optional exact file name to load from file_dir (overrides default file name)
    :return: pd.DataFrame with Treasury futures prices
    """
    # Use default directory and file name
    if file_dir is None:
        file_dir = TREASURY_FUT_CSV_FILEDIR
    if file_name is None:
        file_name = TREASURY_FUT_CSV_FILENAME
    # Load designated CSV containing Treasury futures prices
    try:
        return pd.read_csv(file_dir + file_name, index_col=0, parse_dates=True, header=[0, 1])
    except FileNotFoundError:
        print("ERROR: '{}' Treasury futures prices file not found.".format(file_dir+file_name))
        return None


def get_fut_price(data, tenor, trade_date, opt_contr_year, opt_contr_month, opt_exp_date=None):
    """ Retrieve Treasury options' underlying futures price from Bloomberg-exported data
    :param data: Bloomberg-formatted dataset loaded via load_fut_prices
    :param tenor: 2, 5, 10, 30, etc. (-year Treasury futures)
    :param trade_date: trade date on which to get price
    :param opt_contr_year: option contract year
    :param opt_contr_month: option contract month
    :param opt_exp_date: optional option expiration date (overrides opt_contr_year and opt_contr_month)
    :return: numerical price
    """
    if opt_exp_date is not None:
        # Override contract year and month
        if isinstance(opt_exp_date, str):
            opt_exp_date = pd.to_datetime(opt_exp_date)
        next_month_and_year = opt_exp_date + pd.DateOffset(months=1)
        opt_contr_year = next_month_and_year.year
        opt_contr_month = next_month_and_year.month
    tenor_code = TENOR_TO_CODE_DICT[tenor]
    quarter_code = EXPMONTH_CODES_DICT[undl_fut_quarter_month(opt_contr_month)]
    try:
        year_code = '{:02d}'.format(opt_contr_year - 2000)  # Two digits for past years
        ticker = tenor_code + quarter_code + year_code + ' Comdty'
        timeseries = data[ticker]['PX_LAST'].dropna()
    except KeyError:
        year_code = '{}'.format((opt_contr_year - 2000) % 10)   # One digit only for most recent year
        ticker = tenor_code + quarter_code + year_code + ' Comdty'
        timeseries = data[ticker]['PX_LAST'].dropna()
    return timeseries.loc[trade_date]
