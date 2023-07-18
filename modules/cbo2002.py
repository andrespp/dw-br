from dw import dim_cbo2002
from dw import stg_cbo2002_familia
from dw import stg_cbo2002_grande_grupo
from dw import stg_cbo2002_ocupacao
from dw import stg_cbo2002_subgrupo
from dw import stg_cbo2002_subgrupo_principal
from prefect import flow, task
#from prefect.task_runners import SequentialTaskRunner
import os
import time

stats = {}

# Dataset's Tables Prefect Flow
@flow(
    name='CBO 2002 Dataset Workflow',
    description='Process "CBO 2002" datasets',
    version=os.getenv("GIT_COMMIT_SHA"),
    # task_runner=SequentialTaskRunner()
)
def dataset_flow(DW, DW_SAMPLE, DATASRC, ds_group, ds_table, verbose):

    global stats

    # stg
    if set(ds_group).intersection(['all', 'stg']):

        # STG_CBO2002_GRANDE_GRUPO
        if set(ds_table).intersection(['all', 'stg_cbo2002_grande_grupo']):
            stg_cbo2002_grande_grupo_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

        # STG_CBO2002_SUBGRUPO_PRINCIPAL
        if set(ds_table).intersection(['all', 'stg_cbo2002_subgrupo_principal']):
            stg_cbo2002_subgrupo_principal_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

        # STG_CBO2002_SUBGRUPO
        if set(ds_table).intersection(['all', 'stg_cbo2002_subgrupo']):
            stg_cbo2002_subgrupo_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

        # STG_CBO2002_FAMILIA
        if set(ds_table).intersection(['all', 'stg_cbo2002_familia']):
            stg_cbo2002_familia_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

        # STG_CBO2002_OCUPACAO
        if set(ds_table).intersection(['all', 'stg_cbo2002_ocupacao']):
            stg_cbo2002_ocupacao_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # dim
    if set(ds_group).intersection(['all', 'dim']):

        # DIM_CBO2002
        if set(ds_table).intersection(['all', 'dim_cbo2002']):
            dim_cbo2002_etl.submit(
                DW, DW_SAMPLE, DATASRC, verbose
            )

    # fact
    if set(ds_group).intersection(['all', 'fact']):
        pass

    return stats

@task(
    name='STG_CBO2002_GRANDE_GRUPO ETL',
    description='ETL Process',
    tags=['cbo2002', 'staging'],
)
def stg_cbo2002_grande_grupo_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = stg_cbo2002_grande_grupo.extract(DATASRC, verbose)

    df, lt = stg_cbo2002_grande_grupo.transform(df, DW, DW_SAMPLE, verbose)

    df, ll = stg_cbo2002_grande_grupo.load(df, DW, truncate=True, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_cbo2002_grande_grupo':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

@task(
    name='STG_CBO2002_SUBGRUPO_PRINCIPAL ETL',
    description='ETL Process',
    tags=['cbo2002', 'staging'],
)
def stg_cbo2002_subgrupo_principal_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = stg_cbo2002_subgrupo_principal.extract(DATASRC, verbose)
    df, lt = stg_cbo2002_subgrupo_principal.transform(df, DW, DW_SAMPLE, verbose)
    df, ll = stg_cbo2002_subgrupo_principal.load(df, DW, truncate=True, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_cbo2002_subgrupo_principal':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

@task(
    name='STG_CBO2002_SUBGRUPO ETL',
    description='ETL Process',
    tags=['cbo2002', 'staging'],
)
def stg_cbo2002_subgrupo_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = stg_cbo2002_subgrupo.extract(DATASRC, verbose)
    df, lt = stg_cbo2002_subgrupo.transform(df, DW, DW_SAMPLE, verbose)
    df, ll = stg_cbo2002_subgrupo.load(df, DW, truncate=True, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_cbo2002_subgrupo':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

@task(
    name='STG_CBO2002_FAMILIA ETL',
    description='ETL Process',
    tags=['cbo2002', 'staging'],
)
def stg_cbo2002_familia_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = stg_cbo2002_familia.extract(DATASRC, verbose)
    df, lt = stg_cbo2002_familia.transform(df, DW, DW_SAMPLE, verbose)
    df, ll = stg_cbo2002_familia.load(df, DW, truncate=True, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_cbo2002_familia':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

@task(
    name='STG_CBO2002_OCUPACAO ETL',
    description='ETL Process',
    tags=['cbo2002', 'staging'],
)
def stg_cbo2002_ocupacao_etl(DW, DW_SAMPLE, DATASRC, verbose):

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    df, le = stg_cbo2002_ocupacao.extract(DATASRC, verbose)
    df, lt = stg_cbo2002_ocupacao.transform(df, DW, DW_SAMPLE, verbose)
    df, ll = stg_cbo2002_ocupacao.load(df, DW, truncate=True, verbose=verbose)

    global stats

    # Track execution time
    duration = (time.time() - start_time) / 60

    stats.update(
        {
            'stg_cbo2002_ocupacao':{
                'extract':le,
                'transform':lt,
                'load':ll,
                'duration':duration,
            }
        }
    )
    return df

@task(
    name='DIM_CBO2002 ETL',
    description='ETL Process',
    tags=['dwbr', 'dimension'],
)
def dim_cbo2002_etl(DW, DW_SAMPLE, DATASRC, verbose):

    table_name='dim_cbo2002'

    # Track execution time
    le = lt = ll = 0
    start_time = time.time()

    dfs, le = dim_cbo2002.extract(DW, verbose)

    df, lt = dim_cbo2002.transform(dfs, DW, DW_SAMPLE, verbose)

    df, ll = dim_cbo2002.load(df, DW, verbose=verbose)

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
