from dw import stg_cnaes
from prefect import flow, task
#from prefect.task_runners import SequentialTaskRunner
import os
import time

stats = {}

# Dataset's Tables Prefect Flow
@flow(
    name='CNAEs Dataset Workflow',
    description='Process "CNAEs" datasets',
    version=os.getenv("GIT_COMMIT_SHA"),
    # task_runner=SequentialTaskRunner()
)
def dataset_flow(DW, DW_SAMPLE, DATASRC, ds_group, ds_table, verbose):

    global stats

    # stg
    if set(ds_group).intersection(['all', 'stg']):

        # STG_CNAES
        if set(ds_table).intersection(['all', 'stg_cnaes']):
            stg_cnaes_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # dim
    if set(ds_group).intersection(['all', 'dim']):

        pass

    # fact
    if set(ds_group).intersection(['all', 'fact']):
        pass

    return stats

@task(
    name='STG_CNAES ETL',
    description='ETL Process',
    tags=['cnaes', 'staging'],
)
def stg_cnaes_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = stg_cnaes.extract(DATASRC, verbose)

    df, lt = stg_cnaes.transform(df, DW, DW_SAMPLE, verbose)

    df, ll = stg_cnaes.load(df, DW, truncate=True, verbose=verbose)

    #if DW_SAMPLE:
    #    df = stg_cnaes.load(
    #        DW_SAMPLE, df, truncate=True, verbose=verbose
    #    )

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_cnaes':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

