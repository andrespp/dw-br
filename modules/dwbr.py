from dw import dim_date, dim_sexo
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

    # stg
    if set(ds_group).intersection(['all', 'stg']):
        print('INFO: DWBR has no stagging tables.')

    # dim
    if set(ds_group).intersection(['all', 'dim']):

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

    # fact
    if set(ds_group).intersection(['all', 'fact']):
        print('INFO: DWBR has no fact tables.')

    return stats

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

