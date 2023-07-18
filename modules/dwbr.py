from dw import dim_cnae
from dw import dim_date
from dw import dim_municipio
from dw import dim_sexo
from dw import fato_caged
from prefect import flow, task
#from prefect.task_runners import SequentialTaskRunner
import os
import time

stats = {}

# Dataset's Tables Prefect Flow
@flow(
    name='DWBR Dataset Workflow',
    description='Compute "DWBR" datasets',
    version=os.getenv("GIT_COMMIT_SHA"),
    # task_runner=SequentialTaskRunner()
)
def dataset_flow(DW, DW_SAMPLE, DATASRC, ds_group, ds_table, verbose):

    global stats

    # Check datasrc
    if os.path.isdir(DATASRC):
        pass # parquet src dir exists
    else:
        raise NotImplementedError # sql

    # stg
    if set(ds_group).intersection(['all', 'stg']):
        print('INFO: DWBR has no stagging tables.')

    # dim
    if set(ds_group).intersection(['all', 'dim']):

        # DIM_CNAE
        if set(ds_table).intersection(['all', 'dim_cnae']):
            dim_cnae_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

        # DIM_DATE
        if set(ds_table).intersection(['all', 'dim_date']):
            dim_date_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )
        # DIM_SEXO
        if set(ds_table).intersection(['all', 'dim_sexo']):
            dim_sexo_etl.submit(
                DW, DW_SAMPLE, verbose
            )
        # DIM_MUNICIPIO
        if set(ds_table).intersection(['all', 'dim_municipio']):
            dim_municipio_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # fact
    if set(ds_group).intersection(['all', 'fact']):

        # FATO_CAGED
        if set(ds_table).intersection(['all', 'fato_caged']):
            fato_caged_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    return stats

@task(
    name='DIM_CNAE ETL',
    description='ETL Process',
    tags=['dwbr', 'dimension'],
)
def dim_cnae_etl(DW, DW_SAMPLE, DATASRC, verbose):

    table_name='dim_cnae'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = dim_cnae.extract(DATASRC, verbose)

    df, lt = dim_cnae.transform(df, DW, DW_SAMPLE, verbose)

    df, ll = dim_cnae.load(df, DW, verbose=verbose)

    #if DW_SAMPLE:
    #    df = stg_<dsname>.load(
    #        DW_SAMPLE, df, truncate=True, verbose=verbose
    #    )

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            table_name:{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

@task(
    name='DIM_DATE ETL',
    description='ETL Process',
    tags=['dwbr', 'dimension'],
)
def dim_date_etl(DW, DW_SAMPLE, DATASRC, verbose):

    table_name='dim_date'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    ddf, le = None, 0

    ddf, lt = dim_date.transform(ddf, DW, DW_SAMPLE, verbose)

    ddf, ll = dim_date.load(ddf, DW, truncate=True, verbose=verbose)

    #if DW_SAMPLE:
    #    df = stg_<dsname>.load(
    #        DW_SAMPLE, df, truncate=True, verbose=verbose
    #    )

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            table_name:{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return ddf

@task(
    name='DIM_SEXO_ETL',
    description='ETL Process',
    tags=['dwbr', 'dimension'],
)
def dim_sexo_etl(DW, DW_SAMPLE, verbose):

    table_name='dim_sexo'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    ddf, le = None, 0

    ddf, lt = dim_sexo.transform(ddf, DW, DW_SAMPLE, verbose)

    ddf, ll = dim_sexo.load(ddf, DW, truncate=True, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            table_name:{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return ddf

@task(
    name='DIM_MUNICIPIO_ETL',
    description='ETL Process',
    tags=['dwbr', 'dimension'],
)
def dim_municipio_etl(DW, DW_SAMPLE, DATASRC, verbose):

    table_name='dim_municipio'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    ddf, le = dim_municipio.extract(DATASRC, verbose)

    ddf, lt = dim_municipio.transform(ddf, DW, DW_SAMPLE, verbose)

    ddf, ll = dim_municipio.load(ddf, DW, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            table_name:{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return ddf

@task(
    name='FATO_CAGED_ETL',
    description='ETL Process',
    tags=['dwbr', 'fact'],
)
def fato_caged_etl(DW, DW_SAMPLE, DATASRC, verbose):

    table_name='fato_caged'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    # dfs, le = fato_caged.extract(DW, verbose)

    df, lt = fato_caged.transform(None, DW, DW_SAMPLE, verbose)

    df, ll = fato_caged.load(df, DW, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            table_name:{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df
