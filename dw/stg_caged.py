"""stg_caged.py
"""
import pandas as pd
import numpy as np
from unidecode import unidecode

TABLE_NAME = 'stg_caged'

def extract(ds_files, verbose=False):
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


def transform(df, dw=None, verbose=False):
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
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    dw

    # Remove special chars and Lowercase columns names
    df.columns = [unidecode(x.lower()) for x in df.columns]

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df

def load(dw, df, truncate=False, verbose=False, chunksize=None):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | DataWarehouse object
            DataWarehouse object

        df | Pandas DataFrame
            Data to be loaded

        truncate | boolean
            If true, truncate table before loading data
    """
    if(verbose):
        print('{}: Load. '.format(TABLE_NAME), end='', flush=True)

    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME)

    dw.write_table(TABLE_NAME, df, chunksize=chunksize)

    if(verbose):
        print('{} registries loaded.\n'.format(len(df)))

    return

