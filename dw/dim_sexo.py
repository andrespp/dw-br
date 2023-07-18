import pandas as pd
import dask.dataframe as dd
from math import ceil

TABLE_NAME = 'dim_sexo'

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

    # Define data
    data = {
        'sexo_sk':[1,2,3],
        'sexo':['maculino', 'feminino', 'n/e'],
        # 1	Homem
        # 3	Mulher
        # 9	Não Identificado
        'sexo_novo_caged_cod':[1, 3, 9],
        'sexo_novo_caged_desc':['Homem', 'Mulher', "Não Identificado"],

        }
    df = pd.DataFrame(data)

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

