#!/usr/bin/env python3
import os
import json
import py7zr
import shutil
import zipfile
from prefect import task, flow, get_run_logger
#from prefect_dask.task_runners import DaskTaskRunner
#from prefect.task_runners import ConcurrentTaskRunner
from prefect.task_runners import SequentialTaskRunner

# 1st level subdirs are datasets. Nth level belengs to 1st level ds

DATASET_LIST = './data/datasets.json'
DATASRC_DIR = './data/src'
DATASET_DIR = './data/raw'

# Set Loglevel
os.environ['PREFECT_LOGGING_LEVEL'] = 'DEBUG' # workaround to task WARNIING level
@task(
    name='Extract file2'
)
def extract_resource_file(
        dsname, fname, src_path, destination_path, resource_type
    ):

    log = get_run_logger()

    full_resource_fname = os.path.join(src_path, fname)

    # Check if destination dir exists
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
        log.info(f'{dsname}: directory "{destination_path}" created')

    if resource_type.lower() == 'csv':
        try:
            shutil.copy(full_resource_fname, destination_path)
            log.info(f'{dsname}: "{fname}" copyed to "{destination_path}"')
        except Exception as e:
            log.error(
                f'{dsname}: Unable to copy "{fname}" to "{destination_path}. {e}"'
            )
    elif resource_type.lower() == 'zip':
        pass
    elif resource_type.lower() == '7z':
        if not py7zr.is_7zfile(full_resource_fname):
            raise TypeError('Not a 7z file')
        else:
            try:
                with py7zr.SevenZipFile(full_resource_fname, mode='r') as z:
                    z.extractall(path=destination_path)
                    log.info(
                        f'{dsname}: "{fname}" extracted to "{destination_path}"'
                    )
            except py7zr.exceptions.Bad7zFile as e:
                log.warning(f'{dsname}: {full_resource_fname} 7z file is corrupted')
    else:
        raise TypeError('Invalid resource file type')

    return

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
    with open(DATASET_LIST, 'r') as f:
        datasets = json.load(f)['datasets']
        log.info(
            f'--> {len(datasets)} datasets found in from "{DATASET_LIST}"'
        )

    # Iterate over datasets
    for ds in datasets:

            # Iterate over dataset resources
            dsname = ds['id']

            if dsname == '':
                pass

            else:
                log.info(f'--> {ds["id"]}: {ds["name"]}')
                src_path = DATASRC_DIR + '/' + dsname.lower()
                destination_path = DATASET_DIR + '/' + dsname.lower()
                for resource in ds['resources']:
                    log.info(f'\t"{resource["filename"]}"')
                    if resource['url'] == '':
                        pass
                    elif os.path.isfile(
                        os.path.join(destination_path, resource['target_file'])
                    ):
                        log.warning('Destination file exists, skiping')
                    else:
                        log.info(f'\t"{resource["filename"]}"')
                        # Extract datasets
                        extract_resource_file.submit(
                            dsname,
                            resource['filename'],
                            src_path,
                            destination_path,
                            resource['type'],
                        )


if __name__ == "__main__":

    ds_extract_flow(DATASRC_DIR, DATASET_DIR)
