"""dim_municipio.py
"""
import pandas as pd
import numpy as np
import dask.dataframe as dd

TABLE_NAME = 'dim_municipio'

###############################################################################
# Extract functions
###############################################################################
def extract(ds_files, target, verbose=False):
    """Extract data from source

    Parameters
    ----------
        ds_files | cvs list of filenames

        target | string
            DW's load target. Options are 'parquet', 'postgres', 'sample'

    Returns
    -------
        data : Pandas or Dask DataFrame
            Extracted Data
    """
    if(verbose):
        print(f'{TABLE_NAME}: Extract. ', end='', flush=True)

    dtype={'CÓDIGO SIAFI':int,
           'CNPJ':str,
           'DESCRIÇÃO':str,
           'UF':str,
           'CÓDIGO IBGE':int}

    cols = ['CÓDIGO SIAFI', 'CNPJ', 'DESCRIÇÃO', 'UF', 'CÓDIGO IBGE']

    # Extract by target
    if target in ['parquet', 'postgres']:

        if target == 'parquet' and verbose:
            print('(dask) ', end='', flush=True)

        # Data fits in memory, using Pandas
        df = pd.read_csv(
            ds_files, names=cols, sep=';', encoding='latin1', dtype=dtype,
        )
        df_len = len(df)

    else:
        if (verbose):
            print('WARN: Target not implemented, skipping.')
        return pd.DataFrame()

    if(verbose):
        print('{} registries extracted.'.format(df_len))

    return df

###############################################################################
# Transform functions
###############################################################################
def transform(df, target, dw=None, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

        target | string
            DW's load target. Options are 'parquet', 'postgres', 'sample'

        dw | DataWarehouse Object
            Object to be used in data lookups

    Returns
    -------
        data | Pandas or DataFrame
            Data to be tranformed
    """
    if target not in ['postgres', 'parquet']:
        if (verbose):
            print('WARN: Target not implemented, skipping.')
        return pd.DataFrame()

    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)
        if target=='parquet':
            print('(dask) ', end='', flush=True)

    # Rename Columns
    df.rename(index=str,
              columns={'CÓDIGO SIAFI': 'COD_SIAFI',
                       'DESCRIÇÃO': 'NOME',
                       'CÓDIGO IBGE': 'COD_IBGE',
                      },
              inplace=True)

    ## Select and Reorder columns
    df = df[['COD_SIAFI', 'COD_IBGE', 'CNPJ', 'UF', 'NOME']]

    # Remove invalid IBGE Codes
    df = df[df['COD_IBGE']!=0]

    # Lowercase columns names
    df.columns = [x.lower() for x in df.columns]

    # Set surrogate keys
    df.set_index(np.arange(1, len(df)+1), inplace=True)

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df

###############################################################################
# Load functions
###############################################################################
def load(dw, df, target, truncate=False, verbose=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | DataWarehouse object
            DataWarehouse object

        df | Pandas DataFrame
            Data to be loaded

        target | string
            DW's load target. Options are 'parquet', 'postgres', 'sample'

        truncate | boolean
            If true, truncate table before loading data
    """
    if(verbose):
        print('{}: Load. '.format(TABLE_NAME), end='', flush=True)

    if target=='parquet':

        if(verbose):
            print('(dask) ', end='', flush=True)

        datadir = dw + '/' + TABLE_NAME

        # Write parquet files
        dd.from_pandas(df, npartitions=1).to_parquet(datadir)

    elif target=='postgres':

        # Truncate table
        if truncate:
            dw.truncate(TABLE_NAME, cascade=True)

            dw.write(TABLE_NAME, df)

    else:
        if (verbose):
            print('WARN: Target not implemented, skipping.')


    if(verbose):
        print('{} registries loaded.\n'.format(len(df)))

    return

