import pandas as pd
import numpy as np
import dask.dataframe as dd
from unidecode import unidecode

TABLE_NAME = 'stg_caged'

###############################################################################
# Extract functions
###############################################################################
def extract(ds_files, verbose=False):
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

    # deprecated, using only dask
    #return extract_postgres(ds_files, verbose)

    return extract_dask(ds_files, verbose)

def extract_postgres(ds_files, verbose=False):
    """Extract data from source

    Parameters
    ----------
        ds_files | cvs list of filenames

        target | string
            DW's load target. Options are 'parquet', sample'

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data

        use_dask : boolean
            Whether to use dask dataframes or plain pandas dataframes.

    """

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
        print(f'{len(df)} registries extracted.')

    return df

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
        print('(dask) ', end='', flush=True)

    ddf = dd.read_csv(
        ds_files,
        encoding='utf8',
        sep=';',
        thousands='.',
        decimal=',',
        assume_missing=True,
        dtype= {
            'competenciamov':'int64',
            'subclasse':str,
            'regiao':'int64',
            'uf':'int64',
            'municipio':str,
            'saldomovimentacao':np.int32,
            'cbo2002ocupacao':str,
            'categoria':'int64',
            'graudeinstrucao':'int64',
            'idade':str,
            'horascontratuais':'float64',
            'racacor':'int64',
            'sexo':'int64',
            'tipoempregador':'int64',
            'tipoestabelecimento':'int64',
            'tipomovimentacao':'int64',
            'tipodedeficiencia':'int64',
            'indtrabintermitente':'int64',
            'indtrabparcial':'int64',
            'salario':'float64',
            'tamestabjan':'int64',
            'indicadoraprendiz':'int64',
            'origemdainformacao':'int64',
            'competenciaexc':'int64',
            'indicadordeforadoprazo':'int64',
            'unidadesalariocodigo':'int64',
            'valorsalariofixo':'float64',
        }
    )

    if(verbose):
        ddf_len = ddf.map_partitions(len).compute().sum()
        print(f'{ddf_len} registries extracted.')

    return ddf, ddf_len

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
        data | Pandas or Dask DataFrame
    """
    if(verbose):
        print(f'{TABLE_NAME}: Transform. ', end='', flush=True)

    #return transform_postgres(df, dw, verbose)

    return transform_dask(df, dw, verbose)

def transform_postgres(df, dw=None, verbose=False):
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
    dw

    # Remove special chars and Lowercase columns names
    df.columns = [unidecode(x.lower()) for x in df.columns]

    if(verbose):
        print(f'{len(df)} registries transformed.')

    return df

def transform_dask(df, dw=None, verbose=False):
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
    dw

    if(verbose):
        print('(dask) ', end='', flush=True)

    # Remove special chars and Lowercase columns names
    new_columns = [unidecode(x.lower()) for x in df.columns]
    df = df.rename(columns=dict(zip(df.columns, new_columns)))

    if(verbose):
        df_len = df.map_partitions(len).compute().sum()
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
            dw, df, truncate=truncate, verbose=verbose, chunksize=chunksize
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

    # Remove old parquet files
    if verbose:
        print(f'Removing old parquet files. ', end='', flush=True)
    import os
    try:
        for f in os.listdir(datadir):
            if f.endswith(".parquet"):
                os.remove(os.path.join(datadir, f))
    except FileNotFoundError:
        pass

    # Write parquet files
    df.to_parquet(datadir)

    if(verbose):
        df_len = df.map_partitions(len).compute().sum()
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
