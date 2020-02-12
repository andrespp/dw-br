"""aux_lookups.py
"""
import pandas as pd

def lookup_dim_municipio(data_src):
    """Lookup municipio_sk by Name and UF (exact match)

    Parameters
    ----------
        data_src | Datasrc object
            Object to be used in data lookups

    Returns
    -------
        df : Pandas DataFrame
            dim_municipio table
    """
    sql = "SELECT * FROM DIM_MUNICIPIO"

    return data_src.query(sql)
