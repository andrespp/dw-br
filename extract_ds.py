#!/usr/bin/env python3
import argparse
import configparser
import os.path
import pandas as pd
import py7zr

# Default settings
CONFIG_FILE = 'config.ini'
DATASRC_DIR = './datasrc'
DATASET_LIST = './datasets.csv'
TARGETS = ['RAIS',
           'CAPES',
]

# argparse setup
parser = argparse.ArgumentParser(
    description="DWBRA's Dataset extractor")
parser.add_argument('-c',
                    '--config-file',
                    type=str,
                    default=CONFIG_FILE,
                    help="Config file",
                    metavar='')
args = parser.parse_args()
CONFIG_FILE=args.config_file


### Main
if __name__ == '__main__':

    # Read configuration File
    if not os.path.isfile(CONFIG_FILE):
        print('ERROR: file "{}" does not exist'.format(CONFIG_FILE))
        exit(-1)
    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
    except:
        print('ERROR: Unable to read config file ("{}")'.format(CONFIG_FILE))
        exit(-1)

    # Retrieve DS Files
    df = pd.read_csv(DATASET_LIST)
    df['download'] = df['download'].apply(lambda x:
                                      True if x.upper()=='S' else False)
    ds_list = df[df['download']]

    # Extract TARGET Datasets
    if 'CAPES' in TARGETS:
        print('Extracting CAPES dataset...skiped!')

    if 'RAIS' in TARGETS:
        print('Extracting RAIS dataset')

        rais_ds_list = config['RAIS']['CONJUNTOS'].split(',\n')

        for index, ds in ds_list[ds_list['nome']=='RAIS'].iterrows():
            print(f"\t{ds['arquivo']}...", end='', flush=True)
            with py7zr.SevenZipFile(DATASRC_DIR + '/' + ds['arquivo'],
                                    mode='r') as z:
                z.extractall(path=DATASRC_DIR)
            print('ok!', flush=True)

