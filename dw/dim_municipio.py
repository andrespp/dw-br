import pandas as pd
import dask.dataframe as dd
import os.path

TABLE_NAME = 'dim_municipio'

###############################################################################
# Extract functions
###############################################################################
def extract(datasrc, verbose=False):
    """Extract data from source

    Parameters
    ----------
        datasrc | parquet path or dw object

        verbose | boolean

    Returns
    -------
        df, df_len
    """

    if(verbose):
        print(f'{TABLE_NAME}: Extract. ', end='', flush=True)

    # Check datasrc
    if os.path.isdir(datasrc): # parquet src
        parquet_table_path = os.path.join(
                datasrc, 'stg_municipiosbrasileiros'
                )
        if os.path.isdir(parquet_table_path):
            df, df_len = extract_parquet(parquet_table_path)
        else:
            raise FileNotFoundError
    else:
        raise NotImplementedError


    if(verbose):
        print('{} registries extracted.'.format(df_len))

    return df, df_len

def extract_parquet(datasrc):

    df = pd.read_parquet(datasrc)
    df_len = len(df)

    return df, df_len


###############################################################################
# Transform functions
###############################################################################
def transform(df, dw=None, dw_sample=None, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse Object

    Returns
    -------
        data | Pandas or Dask DataFrame
    """
    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    if dw_sample: pass
    if dw: pass

    # Set surrogate keys
    df.reset_index(inplace=True, drop=True)
    df['municipio_sk'] = df.index + 1

    ## Select and Reorder columns
    df = df[[
        'municipio_sk', 'cod_siafi', 'cod_ibge', 'cnpj', 'uf', 'nome',
        ]]

    # Dataset length
    df_len = len(df)

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df, df_len

###############################################################################
# Load functions
###############################################################################
def load(df, dw=None, dw_sample=None, verbose=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        df | Pandas DataFrame
            Data to be loaded

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse object

        truncate | boolean
            If true, truncate table before loading data

        verbose | boolean
    """
    if dw_sample: pass

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

    # dataset length
    df_len = len(df)

    if(verbose):
        print('{} registries loaded.\n'.format(df_len))

    return df, df_len

