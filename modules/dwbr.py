from dw import dim_data
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

        # DIM_DATA
        if set(ds_table).intersection(['all', 'dim_data']):
            dim_data_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # fact
    if set(ds_group).intersection(['all', 'fact']):
        print('INFO: DWBR has no fact tables.')

    return stats

@task(
    name='DIM_DATA ETL',
    description='ETL Process',
    tags=['caged', 'staging'],
)
def dim_data_etl(DW, DW_SAMPLE, DATASRC, verbose):

    table_name='dim_data'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    ddf, le = None, 0

    ddf, lt = dim_data.transform(ddf, DW, DW_SAMPLE, verbose)

    ddf, ll = dim_data.load(ddf, DW, truncate=True, verbose=verbose)

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

