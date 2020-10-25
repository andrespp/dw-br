"""dim_municipio.py
"""
import pandas as pd
import numpy as np
import xlrd

TABLE_NAME = 'dim_municipio'

def extract(data_src, verbose=False):
    """Extract data from source

    Parameters
    ----------
        data_src | XLS filename

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data
    """
    if(verbose):
        print('{}: Extract. '.format(TABLE_NAME), end='', flush=True)

    dtype={'CÓDIGO SIAFI':int,
           'CNPJ':str,
           'DESCRIÇÃO':str,
           'UF':str,
           'CÓDIGO IBGE':int}

    df = pd.read_excel(data_src, sheet_name='TABMUN SIAFI',dtype=dtype)

    if(verbose):
        print('{} registries extracted.'.format(len(df)))

    return df

def transform(df, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

    Returns
    -------
        data : Pandas DataFrame
            Data to be tranformed
    """
    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

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

def load(dw, df, truncate=False, verbose=False):
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

    if(verbose):
        print('{} registries loaded.\n'.format(len(df)))

    return dw.write(TABLE_NAME, df, verbose=verbose)
