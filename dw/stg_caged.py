"""stg_caged.py
"""
import pandas as pd
import numpy as np
import dask.dataframe as dd
from unidecode import unidecode

TABLE_NAME = 'stg_caged'

def extract_dask(ds_files, verbose=False):
    """Extract data from source

    Parameters
    ----------
        ds_files | cvs list of filenames

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data
    """
    if(verbose):
        print('{}: (dask)Extract. '.format(TABLE_NAME), end='', flush=True)

    ddf = dd.read_csv(
        ds_files,
        encoding='utf8',
        sep=';',
        thousands='.',
        decimal=',',
        assume_missing=True,
    )

    if(verbose):
        ddf_len = ddf.map_partitions(len).compute().sum()
        print(f'{ddf_len} registries extracted.')

    return ddf

def transform_dask(df, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

        dw | DataWarehouse Object
            Object to be used in data lookups

    Returns
    -------
        data : Pandas DataFrame
            Data to be tranformed
    """

    if(verbose):
        print('{}: (dask)Transform. '.format(TABLE_NAME), end='', flush=True)

    # Remove special chars and Lowercase columns names
    new_columns = [unidecode(x.lower()) for x in df.columns]
    df.rename(columns=dict(zip(df.columns, new_columns)))

    if(verbose):
        df_len = df.map_partitions(len).compute().sum()
        print(f'{df_len} registries transformed.')

    return df

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
        print('{}: (dask)Load. '.format(TABLE_NAME), end='', flush=True)

    # Remove old parquet files
    if verbose:
        print(f'Removing old parquet files.', end='', flush=True)
    import os
    for f in os.listdir(datadir):
        if f.endswith(".parquet"):
            os.remove(os.path.join(datadir, f))

    # Write parquet files
    df.to_parquet(datadir)

    if(verbose):
        df_len = df.map_partitions(len).compute().sum()
        print(f'{df_len} registries loaded.\n')

    return

def extract(ds_files, verbose=False, use_dask=False):
    """Extract data from source

    Parameters
    ----------
        ds_files | cvs list of filenames

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data

        use_dask : boolean
            Whether to use dask dataframes or plain pandas dataframes.

    """
    if use_dask: return extract_dask(ds_files, verbose)

    if(verbose):
        print('{}: Extract. '.format(TABLE_NAME), end='', flush=True)


    # Read files
    df_list = []

    for filename in ds_files:
        if verbose:
            print(f'{filename.split("/")[-1]}, ', end='', flush=True)
        df = pd.read_csv(filename,
                         encoding='utf8',
                         sep=';',
                         thousands='.',
                         decimal=',')
        df_list.append(df)

    # Select common attributes (columns intersection)
    columns = df_list[0].columns
    for i in df_list:
        columns = list(set(columns) & set(i.columns))

    for i in np.arange(len(df_list)):
        df_list[i] = df_list[i][columns]

    # Concatenate datasets
    df = pd.concat(df_list, axis=0, ignore_index=True)

    if(verbose):
        print('{} registries extracted.'.format(len(df)))

    return df


def transform(df, dw=None, verbose=False, use_dask=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

        dw | DataWarehouse Object
            Object to be used in data lookups

        use_dask : boolean
            Whether to use dask dataframes or plain pandas dataframes.

    Returns
    -------
        data : Pandas DataFrame
            Data to be tranformed
    """
    if use_dask: return transform_dask(df, verbose)

    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    dw

    # Remove special chars and Lowercase columns names
    df.columns = [unidecode(x.lower()) for x in df.columns]

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df

def load(dw, df, truncate=False, verbose=False, chunksize=None, use_dask=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | DataWarehouse object or String
            DataWarehouse object or path to parquet files if use_dask=True

        df | Pandas DataFrame
            Data to be loaded

        truncate | boolean
            If true, truncate table before loading data

        use_dask : boolean
            Whether to use dask dataframes or plain pandas dataframes.
    """
    if use_dask: return load_dask(dw, df, verbose)

    if(verbose):
        print('{}: Load. '.format(TABLE_NAME), end='', flush=True)

    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME)

    dw.write_table(TABLE_NAME, df, chunksize=chunksize)

    if(verbose):
        print('{} registries loaded.\n'.format(len(df)))

    return

