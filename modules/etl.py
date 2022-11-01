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
        target,
        run=['stg', 'dim', 'fact'],
        tables='all',
        verbose=False,
    ):
    """Trigger ETL process.

    Parameters
    ----------

        ds_name | string

        run | list of strings
            default = ['stg', 'dim', 'fact']

        target | string
            DW's load target. Options are 'parquet', 'postgres', 'sample'

        tables | list of strings
            Specify specific datasets
            default = 'all'

        verbose | bool
            default = False

    Returns
    -------
        ETL statistics dictionary
    """
    from app import CONFIG, DWO

    # Target object
    if target=='parquet':
        DW = CONFIG['DWP']['DATADIR']
    elif target in ['postgres', 'sample']:
        DW = DWO
    else:
        DW = None
        print('WARN: Target not implemented')

    # MUNICIPIOS BRASILEIROS
    if ds_name == 'municipios' and (tables=='all' or ds_name in tables):
        if 'dim' in run:
            df = dim_municipio.extract(
                CONFIG['MUNICIPIOS']['FILE'], target, verbose
            )
            df = dim_municipio.transform(df, target, verbose=verbose)
            dim_municipio.load(DW, df, target, truncate=True, verbose=verbose)

    # CAGED
    elif ds_name == 'caged' and (tables=='all' or ds_name in tables):
        if 'stg' in run:
            ds_list = CONFIG['CAGED']['CONJUNTOS'].split(',\n')

            # Dask
            df = stg_caged.extract(ds_list, target, verbose=verbose)
            df = stg_caged.transform(df, target, DW, verbose=verbose)
            stg_caged.load(DW, df, target, verbose=verbose)

