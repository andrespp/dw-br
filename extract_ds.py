#!/usr/bin/env python3
import logging
import os
import py7zr
import shutil
import zipfile

# 1st level subdirs are datasets. Nth level belengs to 1st level ds

DATASRC_DIR = './data/src'
DATASET_DIR = './data/raw'

# Create and configure logger
LOG_FORMAT = '%(levelname)s\t%(asctime)s\t%(message)s'
logging.basicConfig(
    # filename=sys.stdout,
    level = logging.DEBUG,
    format = LOG_FORMAT,
    filemode = 'w', # default is append
)
log = logging.getLogger()

# Retrieve datasets
ds_list = []
with os.scandir(DATASRC_DIR) as it:
    for entry in it:
        if not entry.name.startswith('.') and entry.is_dir():
            ds_list.append(entry.name)

# Lookup dataset files
datasets = {}

for ds_name in ds_list:

    datasets[ds_name] = {}
    file_list = []

    for root, dirs, files in os.walk(os.path.join(DATASRC_DIR, ds_name)):

        for file in files: 
            file_list.append(os.path.join(root, file))

    datasets[ds_name]['files'] = file_list

log.info(f'Found datasets: {list(datasets.keys())}')

# Extract datasets
for ds_name in datasets:
    for file in datasets[ds_name]['files']:

        # Check if destination dir exists
        path = f'{DATASET_DIR}/{ds_name}'
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

