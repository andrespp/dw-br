from dw import stg_caged
from prefect import flow, task
#from prefect.task_runners import SequentialTaskRunner
import os
import time

stats = {}

# Dataset's Tables Prefect Flow
@flow(
    name='CAGED Dataset Workflow',
    description='Compute "caged" datasets',
    version=os.getenv("GIT_COMMIT_SHA"),
    # task_runner=SequentialTaskRunner()
)
def dataset_flow(DW, DW_SAMPLE, DATASRC, ds_group, ds_table, verbose):

    global stats


    # stg
    if set(ds_group).intersection(['all', 'stg']):

        # STG_CAGED
        if set(ds_table).intersection(['all', 'stg_caged']):
            stg_caged_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # dim
    if set(ds_group).intersection(['all', 'dim']):

        # DIM_CAGED
        if set(ds_table).intersection(['all', 'dim_caged']):
            pass

    # fact
    if set(ds_group).intersection(['all', 'fact']):
        pass

    return stats

@task(
    name='STG_CAGED ETL',
    description='ETL Process',
    tags=['caged', 'staging'],
)
def stg_caged_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    ddf, le = stg_caged.extract(DATASRC, verbose)

    ddf, lt = stg_caged.transform(ddf, DW, DW_SAMPLE, verbose)

    ddf, ll = stg_caged.load(ddf, DW, truncate=True, verbose=verbose)

    #if DW_SAMPLE:
    #    df = stg_<dsname>.load(
    #        DW_SAMPLE, df, truncate=True, verbose=verbose
    #    )

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_caged':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return ddf

