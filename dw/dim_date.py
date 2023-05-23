import pandas as pd
import dask.dataframe as dd
from math import ceil

TABLE_NAME = 'dim_date'

###############################################################################
# Extract functions
###############################################################################

###############################################################################
# Transform functions
###############################################################################
def transform(df, dw=None, dw_sample=None, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Dask DataFrame

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse Object

    Returns
    -------
        df, df_len
    """
    if(verbose):
        print(f'{TABLE_NAME}: Transform. ', end='', flush=True)

    date_from='1950-01-01'
    date_to='2050-12-31'
    df = pd.DataFrame({"Date": pd.date_range(date_from, date_to)})
    df['YEAR_NUMBER'] = df.Date.dt.year
    df['MONTH_NUMBER'] = df.Date.dt.month
    df['MONTH_NUMBER'] = df.Date.dt.month
    df['DAY_OF_YEAR_NUMBER'] = df.Date.dt.dayofyear
    df['DAY_OF_MONTH_NUMBER'] = df.Date.dt.day
    df['DAY_OF_WEEK_NUMBER'] = df.Date.dt.dayofweek # Monday=0, Sunday=6
    df['WEEK_OF_YEAR_NUMBER'] = df.Date.dt.isocalendar().week
    df['DAY_NAME'] = df['DAY_OF_WEEK_NUMBER'].apply(day_name)
    df['MONTH_NAME'] = df['MONTH_NUMBER'].apply(month_name)
    df['MONTH_SHORT_NAME'] = df['MONTH_NUMBER'].apply(month_short_name)
    df['QUARTER_NUMBER'] = df.Date.dt.quarter
    df['WEEKEND_IND'] = df['DAY_OF_WEEK_NUMBER'].apply(weekend_indicator)
    df['LEAP_YEAR'] = df.Date.dt.is_leap_year
    df['DAYS_IN_MONTH'] = df['Date'].apply(
        lambda x: days_in_month(x.month, x.is_leap_year))
    df['DAY_DESC'] = df.apply(
        lambda x: '{:02d}/{:02d}/{:04d}'.format(
            x['DAY_OF_MONTH_NUMBER'],
            x['MONTH_NUMBER'],
            x['YEAR_NUMBER']),
        axis=1
    )
    df['DAY_DATE'] = df['Date']
    df['YEARMO_NUMBER'] = df.apply(
        lambda x: x['YEAR_NUMBER']*100 + x['MONTH_NUMBER'], axis=1)
    df['DATE_SK'] = df.apply(
        lambda x: x['YEARMO_NUMBER']*100 + x['DAY_OF_MONTH_NUMBER'], axis=1)
    df['WEEK_OF_MONTH_NUMBER'] = df['Date'].apply(week_of_month)
    df["YEAR_HALF_NUMBER"] = (df.QUARTER_NUMBER + 1) // 2
    df.set_index('DATE_SK', inplace=True)

    # Set surrogate keys
    df.reset_index(inplace=True, drop=True)
    df['date_sk'] = df.index + 1

    # select and order columns
    df = df[[
        'date_sk',
        'YEAR_NUMBER', 'YEARMO_NUMBER', 'MONTH_NUMBER', 'DAY_OF_YEAR_NUMBER',
        'DAY_OF_MONTH_NUMBER', 'DAY_OF_WEEK_NUMBER', 'WEEK_OF_YEAR_NUMBER',
        'DAY_NAME', 'MONTH_NAME', 'MONTH_SHORT_NAME', 'QUARTER_NUMBER',
        'WEEKEND_IND', 'DAYS_IN_MONTH', 'DAY_DESC', 'DAY_DATE',
        'WEEK_OF_MONTH_NUMBER', 'YEAR_HALF_NUMBER', 'LEAP_YEAR',
    ]]

    # Lowercase columns names
    df.columns = [x.lower() for x in df.columns]
    df_len = len(df)


    if(verbose):
        print(f'{df_len} registries transformed.')

    return df, df_len

###############################################################################
# Load functions
###############################################################################
def load(df, dw=None, dw_sample=None, truncate=False, verbose=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        df | Dask DataFrame
            Data to be loaded

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse object

        truncate | boolean
            If true, truncate table before loading data
    """
    if(verbose):
        print(f'{TABLE_NAME}: Load. ', end='', flush=True)

    if isinstance(dw, str): # target=='parquet':
        return load_dask(dw, df, verbose=verbose)

    else: # target=='postgres':
        return load_postgres(
            dw, df, truncate=truncate, verbose=verbose
        )

def load_postgres(dw, df, truncate=False, verbose=False, chunksize=None):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | DataWarehouse object or String
            DataWarehouse object or path to parquet files

        df | Pandas or Dask DataFrame
            Data to be loaded

        truncate | boolean
            If true, truncate table before loading data

        verbose | boolean

        chunksize | int
    """

    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME)

    dw.write_table(TABLE_NAME, df, chunksize=chunksize)

    df_len = len(df)
    if(verbose):
        print(f'{df_len} registries loaded.\n')

    return df, df_len

def load_dask(dw, df, verbose=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | String
            Path to parquet files

        df | DataFrame
            Data to be loaded

        verbose | boolean
    """
    datadir = dw + '/' + TABLE_NAME

    if(verbose):
        print(f'(dask). ', end='', flush=True)

    # Check if destination dir exists
    import os
    if not os.path.exists(datadir):
        os.makedirs(datadir)
        print(f'INFO: {TABLE_NAME}: directory "{datadir}" created')

    # Remove old parquet files
    if verbose:
        print(f'Removing old parquet files. ', end='', flush=True)
    try:
        for f in os.listdir(datadir):
            if f.endswith(".parquet"):
                os.remove(os.path.join(datadir, f))
    except FileNotFoundError:
        pass

    # Write parquet files
    dd.from_pandas(df, npartitions=1).to_parquet(datadir)

    df_len = len(df) #df.map_partitions(len).compute().sum()

    if(verbose):
        print(f'{df_len} registries loaded.')

    return df, df_len

def load_sample(dw, df, frac=0.01, truncate=False, verbose=False, chunksize=None):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | DataWarehouse object or String
            DataWarehouse object or path to parquet files

        df | Pandas or Dask DataFrame
            Data to be loaded

        frac | integer between 0 and 1
            Fraction of table to be sampled

        truncate | boolean
            If true, truncate table before loading data

        verbose | boolean

        chunksize | int
    """
    if(verbose):
        print(f'{TABLE_NAME}: Load. (sample db) ', end='', flush=True)

    if not (0 <= frac <=1):
        raise ValueError

    # retrieve sample data
    dfs = df.sample(frac=frac).compute()

    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME)

    dw.write_table(TABLE_NAME, dfs, chunksize=chunksize)

    if(verbose):
        print(f'{len(dfs)} registries loaded.')

    return

###############################################################################
# lib
###############################################################################

def weekend_indicator(day_number):
    """Returns true if day_number indicates a weekend day, false otherwise
    """
    return day_number in(5, 6)

def day_name(day_number):
    """Returns day name
    """
    switcher = {
        0: 'segunda-feira',
        1: 'terça-feira',
        2: 'quarta-feira',
        3: 'quinta-feira',
        4: 'sexta-feira',
        5: 'sábado',
        6: 'domingo'
    }
    return switcher.get(day_number, 'err')

def days_in_month(month_number, leap_year):
    """Returns number of days in month
    """
    if month_number == 2 and leap_year:
        return 29

    switcher = {
        1:  31,
        2:  28,
        3:  31,
        4:  30,
        5:  31,
        6:  30,
        7:  31,
        8:  31,
        9:  30,
        10: 31,
        11: 30,
        12: 31,
    }
    return switcher.get(month_number, 'err')

def month_name(month_number):
    """Returns month name
    """
    switcher = {
        1:  'janeiro',
        2:  'fevereiro',
        3:  'março',
        4:  'abril',
        5:  'maio',
        6:  'junho',
        7:  'julho',
        8:  'agosto',
        9:  'setembro',
        10: 'outubro',
        11: 'novembro',
        12: 'dezembro',
    }
    return switcher.get(month_number, 'err')

def month_short_name(month_number):
    """Returns month short name
    """
    switcher = {
        1:  'JAN',
        2:  'FEV',
        3:  'MAR',
        4:  'ABR',
        5:  'MAI',
        6:  'JUN',
        7:  'JUL',
        8:  'AGO',
        9:  'SET',
        10: 'OUT',
        11: 'NOV',
        12: 'DEZ',
    }
    return switcher.get(month_number, 'err')

# https://stackoverflow.com/questions/3806473/python-week-number-of-the-month
def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()
    return int(ceil(adjusted_dom/7.0))
