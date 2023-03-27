from dw import stg_municipiosbrasileiros
from prefect import flow, task
#from prefect.task_runners import SequentialTaskRunner
import os
import time

stats = {}

# Dataset's Tables Prefect Flow
@flow(
    name='Municipiosbrasileirs Dataset Workflow',
    description='Compute "municipiosbrasileiros" datasets',
    version=os.getenv("GIT_COMMIT_SHA"),
    # task_runner=SequentialTaskRunner()
)
def dataset_flow(DW, DW_SAMPLE, DATASRC, ds_group, ds_table, verbose):

    global stats

    # stg
    if set(ds_group).intersection(['all', 'stg']):

        # STG_MUNICIPIOSBRASILEIROS
        if set(ds_table).intersection(['all', 'municipiosbrasileiros']):
            stg_municipiosbrasileiros_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # dim
    if set(ds_group).intersection(['all', 'dim']):

        # DIM_MUNICIPIO
        if set(ds_table).intersection(['all', 'dim_municipio']):
            pass

    # fact
    if set(ds_group).intersection(['all', 'fact']):
        pass

    return stats

@task(
    name='STG_MUNICIPIOSBRASILEIROS ETL',
    description='ETL Process',
    tags=['municipiosbrasileiros', 'staging'],
)
def stg_municipiosbrasileiros_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df = stg_municipiosbrasileiros.extract(DATASRC, verbose
    )
    le = len(df)

    df = stg_municipiosbrasileiros.transform(df, DW, DW_SAMPLE, verbose)
    lt = len(df)

    print(DW)
    stg_municipiosbrasileiros.load(df, DW, truncate=True, verbose=verbose)
    ll = len(df)

    #if DW_SAMPLE:
    #    df = stg_municipiosbrasileiros.load(
    #        DW_SAMPLE, df, truncate=True, verbose=verbose
    #    )

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_municipiosbrasileiros':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

