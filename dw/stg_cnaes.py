import pandas as pd
import numpy as np
import dask.dataframe as dd

TABLE_NAME = 'stg_cnaes'

###############################################################################
# Extract functions
###############################################################################
def extract(ds_files, verbose=False):
    """Extract data from source

    Parameters
    ----------
        ds_files | cvs list of filenames

    Returns
    -------
        df, df_len
    """
    if(verbose):
        print(f'{TABLE_NAME}: Extract. ', end='', flush=True)

    dtype={
        'cnae':str,
        'classe':'str',
    }

    cols = ['cnae', 'descricao_atividade_economica']

    # Data fits in memory, using Pandas
    df = pd.read_csv(
        ds_files,
        names=cols
        , sep=';',
        encoding='latin1',
        dtype=dtype,
    )

    df_len = len(df)

    if(verbose):
        print('{} registries extracted.'.format(df_len))

    return df, df_len

###############################################################################
# Transform functions
###############################################################################
def transform(df, dw=None, dw_sample=None, verbose=False):
    """Transform data
    """

    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    # Dataset len
    df_len = len(df)

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df, df_len

###############################################################################
# Load functions
###############################################################################
def load(df, dw=None, dw_sample=None, truncate=False, verbose=False):
    """Load data into the Data Warehouse
    """
    if(verbose):
        print('{}: Load. '.format(TABLE_NAME), end='', flush=True)

    if isinstance(dw, str): # target=='parquet':

        if(verbose):
            print('(dask) ', end='', flush=True)

        datadir = dw + '/' + TABLE_NAME

        # Write parquet files
        dd.from_pandas(df, npartitions=1).to_parquet(datadir)

    else: # target=='postgres':
        raise NotImplementedError

    df_len = len(df)

    if(verbose):
        print('{} registries loaded.\n'.format(df_len))

    return df, df_len

