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
    if (verbose):
        print('\n{}: '.format(TABLE_NAME))

    dtype={'CÓDIGO SIAFI':int,
           'CNPJ':str,
           'DESCRIÇÃO':str,
           'UF':str,
           'CÓDIGO IBGE':int}

    return pd.read_excel(data_src, sheet_name='TABMUN SIAFI',dtype=dtype)

def transform(df):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

    Returns
    -------
        data : Pandas DataFrame
            Data to be tranformed
    """
    # Rename Columns
    df.rename(index=str,
              columns={'CÓDIGO SIAFI': 'COD_SIAFI',
                       'DESCRIÇÃO': 'NOME',
                       'CÓDIGO IBGE': 'COD_IBGE',
                      },
              inplace=True)

    ## Select and Reorder columns
    df = df[['COD_SIAFI', 'COD_IBGE', 'CNPJ', 'UF', 'NOME']]

    # Lowercase columns names
    df.columns = [x.lower() for x in df.columns]

    # Set surrogate keys
    df.set_index(np.arange(1, len(df)+1), inplace=True)

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
    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME)

    return dw.write(TABLE_NAME, df, verbose=verbose)
