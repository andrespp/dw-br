"""fato_capes_avaliacao_ppg.py
"""
import pandas as pd
import numpy as np
from unidecode import unidecode
from dw import aux_lookups as l

TABLE_NAME = 'fato_capes_avaliacao_ppg'

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
        df = pd.read_csv(filename, encoding='latin1', sep=';')
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


def transform(df, dw, verbose=False):
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

    # Cleanup Data
    df['MUNICIPIO'] = df['NM_MUNICIPIO_PROGRAMA_IES'].apply(unidecode)

    # lookup data
    dim_municipio = l.lookup_dim_municipio(dw)
    df['MUNICIPIO_SK'] = df.apply(lambda x:
                                    lookup_municipio_sk(dim_municipio,
                                                        x['MUNICIPIO'],
                                                        x['SG_UF_PROGRAMA']),
                                  axis=1)
    # Rename Columns

    # Select and Reorder columns
    df = df[['MUNICIPIO_SK',
             'AN_BASE', 'NM_GRANDE_AREA_CONHECIMENTO',
             'SG_ENTIDADE_ENSINO', 'NM_ENTIDADE_ENSINO',
             'NM_REGIAO', 'NM_MUNICIPIO_PROGRAMA_IES', 'SG_UF_PROGRAMA',
             'CD_PROGRAMA_IES', 'NM_PROGRAMA_IES',
             'CD_CONCEITO_PROGRAMA']]

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

    dw.write(TABLE_NAME, df)

    if(verbose):
        print('{} registries loaded.\n'.format(len(df)))

    return

def lookup_municipio_sk(dim_municipio, nome, uf):
    """Lookup municipio_sk by Name and UF (exact match)

    Parameters
    ----------
        dim_municipio | Pandas DataFrame
            dim_municipio table

        name | string
            City name to lookup for

        uf | string
            UF to lookup for

    Returns
    -------
        municipio_sk : int
            MUNICIPIO_SK. 1 if not found
    """
    r = dim_municipio[(dim_municipio['nome']==nome) &
                      (dim_municipio['uf']==uf)]['municipio_sk']

    if len(r) > 0:
        return r.iloc[0]
    else:
        return 1
