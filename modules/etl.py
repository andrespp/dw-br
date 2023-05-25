from modules import cnae, municipiosbrasileiros, caged, dwbr
from prefect import flow, task
# from prefect.task_runners import SequentialTaskRunner
import os

# Used by the CLI
datasets = {

    'cnae':{
        'stg':['stg_cnaes'],
    },

    'municipios':{
        'stg':['stg_municipiosbrasileiros'],
    },

    'caged':{
        'stg':['stg_caged'],
    },

    'dwbr':{
        'dim':['dim_date', 'dim_sexo', 'dim_municipio'],
    }

}

def trigger_etl(
        ds_name,
        target,
        no_sample=False,
        ds_group=['stg', 'dim', 'fact'],
        ds_table='all',
        verbose=False,
    ):
    """Trigger ETL process.

    Parameters
    ----------

        ds_name | string

        ds_group | list of strings
            default = ['stg', 'dim', 'fact']

        target | string
            DW's load target. Options are 'parquet', 'postgres', 'sample'

        no_sample | bool
            Don't build sample DW. Default = False

        ds_table | list of strings
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
        DW_SAMPLE = DWO
    elif target in ['postgres']:
        DW = DWO
        DW_SAMPLE = None
        if not no_sample:
            no_sample = True
            print('INFO: Full DB will be written to postgres, not sample db.')
    else:
        DW = None
        DW_SAMPLE = None
        print('WARN: Target not implemented')

    # Build tables
    if not isinstance(ds_name, list): ds_name = [ds_name]
    if not isinstance(ds_group, list): ds_group = [ds_group]
    if not isinstance(ds_table, list): ds_table = [ds_table]

    if verbose:
        print(
            f"""
            ETL Trigger blueprint
            =====================
            ds_name = {ds_name}
            ds_group = {ds_group}
            ds_table = {ds_table}
            target = {target}
            no_sample = {no_sample}
            """
        )

    # "cnae" tables flow
    cnae_ds_stats = None
    if set(ds_name).intersection(set(['all', 'cnae'])):

        cnae_ds_stats = cnae.dataset_flow(
            DW, DW_SAMPLE, CONFIG['CNAE']['FILE'],
            ds_group, ds_table, verbose
        )

    # "municipiosbrasileiros" tables flow
    municipios_ds_stats = None
    if set(ds_name).intersection(set(['all', 'municipiosbrasileiros'])):

        municipios_ds_stats = municipiosbrasileiros.dataset_flow(
            DW, DW_SAMPLE, CONFIG['MUNICIPIOS']['FILE'],
            ds_group, ds_table, verbose
        )

    # "caged" tables flow
    caged_ds_stats = None
    if set(ds_name).intersection(set(['all', 'caged'])):

        caged_ds_stats = caged.dataset_flow(
            DW, DW_SAMPLE, CONFIG['CAGED']['CONJUNTOS'].split(',\n'),
            ds_group, ds_table, verbose
        )

    # "dwbr" tables flow
    dwbr_ds_stats = None
    if set(ds_name).intersection(set(['all', 'dwbr'])):

        dwbr_ds_stats = dwbr.dataset_flow(
            DW=DW,
            DW_SAMPLE=DW_SAMPLE,
            DATASRC=DW,
            ds_group=ds_group,
            ds_table=ds_table,
            verbose=verbose,
        )

    # Global stats
    global_stats = {}

    if cnae_ds_stats:
        global_stats['cnae'] = cnae_ds_stats

    if municipios_ds_stats:
        global_stats['municipiosbrasileiros'] = municipios_ds_stats

    if caged_ds_stats:
        global_stats['caged'] = caged_ds_stats

    if dwbr_ds_stats:
        global_stats['dwbr'] = dwbr_ds_stats

    if verbose:
        print_etl_summary(global_stats)

    return global_stats


def print_etl_summary(global_stats):

    import pandas as pd

    datasets = list(global_stats.keys())
    data = []

    for ds in datasets:

        tables = list(global_stats[ds].keys())

        for table in tables:

            if 'extract' in global_stats[ds][table].keys():
                le = global_stats[ds][table]['extract']
            else:
                le = 0

            if 'transform' in global_stats[ds][table].keys():
                lt = global_stats[ds][table]['transform']
            else:
                lt = 0

            if 'load' in global_stats[ds][table].keys():
                ll = global_stats[ds][table]['load']
            else:
                ll = 0

            if 'duration' in global_stats[ds][table].keys():
                duration = global_stats[ds][table]['duration']
            else:
                duration = None

            data.append([ds, table, le, lt, ll, duration])

    df = pd.DataFrame(
        data,
        columns=[
            'dataset', 'tabela', 'extract', 'transform',
            'load', 'duration (min)'
        ],
    )
    df['extract'] = df['extract'].apply(int)
    df['transform'] = df['transform'].apply(int)
    df['load'] = df['load'].apply(int)

    datasets = df['dataset'].unique()
    n_datasets = len(datasets)
    n_tables = len(df)
    n_loaded = df['load'].sum()

    print(
        f"""
        ETL Run Summary
        ===============
        {n_datasets} datasets processed: {datasets}
        {n_tables} tables processed
        {n_loaded} registries loaded
        """
    )
    print(df)

    return

