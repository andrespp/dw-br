"""aux_dw_updates.py

"""
import datetime as dt
import pandas as pd

TABLE_NAME = 'aux_dwupdate'

def load(dw, hostname, elapsed_time, truncate=False, verbose=False):
    """Log update to aux_dwupdate table

    Parameters
    ----------
        dw | DataWarehouse object
            DataWarehouse object

        hostname | str
            Host name where etl process was runned

        elapsed_time | float
            ETL elapsed time in minutes

        truncate | boolean
            If true, truncate table before loading data

        verbose | boolean
    """

    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME, verbose)

    df = pd.DataFrame([[dt.datetime.now(), hostname, elapsed_time]],
                      columns='update hostname elapsed_time'.split())

    return dw.write_table(TABLE_NAME, df, verbose)
