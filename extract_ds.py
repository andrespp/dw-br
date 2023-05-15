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
    name='Extract dataset resource file'
)
def extract_resource_file(
        dsname, fname, src_path, destination_path, data_file, resource_type,
        target_file=None,
    ):

    log = get_run_logger()

    full_resource_fname = os.path.join(src_path, fname)
    target = os.path.join(destination_path, target_file)

    # Check if destination dir exists
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
        log.info(f'{dsname}: directory "{destination_path}" created')

    # CSV Files
    if resource_type.lower() == 'csv':
        try:
            shutil.copy(full_resource_fname, target)
            log.info(f'{dsname}: "{fname}" copyed to "{target}"')
        except Exception as e:
            log.error(
                f'{dsname}: Unable to copy "{fname}" to "{target}. {e}"'
            )

    # ZIP Files
    elif resource_type.lower() == 'zip':
        pass

    # 7-ZIP Files
    elif resource_type.lower() == '7z':
        if not py7zr.is_7zfile(full_resource_fname):
            raise TypeError('Not a 7z file')
        else:
            try:
                with py7zr.SevenZipFile(full_resource_fname, mode='r') as z:
                    z.extract(path=destination_path, targets=data_file)
                    if target_file:
                        os.rename(
                            os.path.join(destination_path, data_file),
                            os.path.join(destination_path, target_file),
                        )
                    log.info(
                        f'{dsname}: "{fname}" extracted to "{target}"'
                    )
            except py7zr.exceptions.Bad7zFile as e:
                log.warning(
                    f'{dsname}: {full_resource_fname} 7z file is corrupted'
                )

    # Invalid archive file
    else:
        raise TypeError('Invalid resource file type')

    return

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
                        log.warning(
                            f'Destination file "{resource["target_file"]}" ' \
                            f'exists, skipping'
                    )
                    else:
                        log.info(f'\t"{resource["filename"]}"')
                        # Extract datasets
                        extract_resource_file.submit(
                            dsname=dsname,
                            fname=resource['filename'],
                            src_path=src_path,
                            destination_path=destination_path,
                            data_file=resource['data_file'],
                            target_file=resource['target_file'],
                            resource_type=resource['type'],
                        )


if __name__ == "__main__":

    ds_extract_flow(DATASRC_DIR, DATASET_DIR)
