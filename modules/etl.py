from dw import dim_municipio
from dw import stg_caged

datasets = {

    'municipios':{
        'stg':None,
        'dim':['dim_municipio'],
        'fact':None,
    },
 
    'caged':{
        'stg':['stg_caged'],
        'dim':None,
        'fact':None,
    },
}

def trigger_etl(
        ds_name,
        run=['stg', 'dim', 'fact'],
        tables=None,
        verbose=False
    ):
    """Trigger ETL process.

    Parameters
    ----------

        ds_name | string

        run | list of strings
            default = ['stg', 'dim', 'fact']

        tables | list of strings
            Specify specific datasets
            default = None

        verbose | bool
            default = False

    Returns
    -------
        ETL statistics dictionary
    """
    from app import CONFIG, DWO, CHUNKSIZE

    # MUNICIPIOS BRASILEIROS
    if ds_name == 'municipios' and (tables is None or ds_name in tables):
        if 'dim' in run:
            df = dim_municipio.extract(CONFIG['MUNICIPIOS']['FILE'], verbose)
            df = dim_municipio.transform(df, verbose)
            dim_municipio.load(DWO, df, truncate=True, verbose=verbose)

    # CAGED
    elif ds_name == 'caged' and (tables is None or ds_name in tables):
        if 'stg' in run:
            ds_list = CONFIG['CAGED']['CONJUNTOS'].split(',\n')
            df = stg_caged.extract(ds_list, verbose=verbose)
            df = stg_caged.transform(df, DWO, verbose=verbose)
            stg_caged.load(DWO, df, verbose=verbose, chunksize=CHUNKSIZE)

