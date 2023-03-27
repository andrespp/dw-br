#!/usr/bin/env python3
import os
import py7zr
import shutil
import zipfile
from prefect import task, flow, get_run_logger
#from prefect_dask.task_runners import DaskTaskRunner
#from prefect.task_runners import ConcurrentTaskRunner
from prefect.task_runners import SequentialTaskRunner

# 1st level subdirs are datasets. Nth level belengs to 1st level ds

DATASRC_DIR = './data/src'
DATASET_DIR = './data/raw'

# Set Loglevel
os.environ['PREFECT_LOGGING_LEVEL'] = 'DEBUG' # workaround to task WARNIING level

@task(
    name='Extract file'
)
def extract_ds_file(ds_dir, ds_name, file):

    log = get_run_logger()

    # Check if destination dir exists
    path = f'{ds_dir}/{ds_name}'
    if not os.path.exists(path):
        log.info(f'{ds_name}: directory "{path}" created')
        os.makedirs(path)

    if py7zr.is_7zfile(file):
        try:
            with py7zr.SevenZipFile(file, mode='r') as z:
                z.extractall(path=path)
                log.info(f'{ds_name}: "{file}" extracted to "{path}"')
        except py7zr.exceptions.Bad7zFile as e:
            log.warning(f'{ds_name}: {file} 7z file is corrupted')

    elif zipfile.is_zipfile(file):
        try:
            with zipfile.ZipFile(file, 'r') as z:
                z.extractall(path)
                log.info(
                    f'{ds_name}: "{file}" extracted to ' \
                    f'"{path}/{z.namelist()}"'
                )
        except Exception as e:
            log.warning(f'{ds_name}: {file} 7z file is corrupted')

    else:
        try:
            shutil.copy(file, path)
            log.info(f'{ds_name}: "{file}" copyed to "{path}"')
        except Exception as e:
            log.error(f'{ds_name}: Unable to copy "{file}" to "{path}"')

@flow(
    name='Extract Datasets',
    #task_runner=DaskTaskRunner(),
    #task_runner=ConcurrentTaskRunner(),
    task_runner=SequentialTaskRunner(),
)
def ds_extract_flow(datasrc_dir, dataset_dir):

    log = get_run_logger()

    # Retrieve datasets
    ds_list = []
    with os.scandir(datasrc_dir) as it:
        for entry in it:
            if not entry.name.startswith('.') and entry.is_dir():
                ds_list.append(entry.name)

    # Lookup dataset files
    datasets = {}

    for ds_name in ds_list:

        datasets[ds_name] = {}
        file_list = []

        for root, dirs, files in os.walk(os.path.join(datasrc_dir, ds_name)):

            for file in files:
                file_list.append(os.path.join(root, file))

        datasets[ds_name]['files'] = file_list

    log.info(f'Found datasets: {list(datasets.keys())}')

# Extract datasets
    for ds_name in datasets:
        for file in datasets[ds_name]['files']:
            extract_ds_file.submit(dataset_dir, ds_name, file)

if __name__ == "__main__":

    ds_extract_flow(DATASRC_DIR, DATASET_DIR)
