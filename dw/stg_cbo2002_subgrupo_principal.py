import pandas as pd
import dask.dataframe as dd

TABLE_NAME = 'stg_cbo2002_subgrupo_principal'

###############################################################################
# Extract functions
###############################################################################
def extract(ds_config, verbose=False):
    """Extract data from source
    """
    if(verbose):
        print(f'{TABLE_NAME}: Extract. ', end='', flush=True)

    ds_file = ds_config['SUBGRUPO_PRINCIPAL']
  
    dtype={
        'code':str,
        'name':str,
    }

    cols = ['subgrupo_principal', 'subgrupo_principal_descricao']

    # Data fits in memory, using Pandas
    df = pd.read_csv(
        ds_file,
        names=cols,
        skiprows=[0],
        sep=';',
        encoding='utf8',
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

    # cod
    df['subgrupo_principal'] = df['subgrupo_principal'].apply(
        lambda x: str(x).zfill(2)
    )

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

        datadir = dw  + '/' + TABLE_NAME

        # Write parquet files
        dd.from_pandas(df, npartitions=1).to_parquet(datadir)

    else: # target=='postgres':
        raise NotImplementedError

    df_len = len(df)

    if(verbose):
        print('{} registries loaded.\n'.format(df_len))

    return df, df_len

