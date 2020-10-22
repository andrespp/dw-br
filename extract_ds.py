#!/usr/bin/env python3
import os.path
import configparser
import argparse
import py7zr

# Default settings
CONFIG_FILE = 'config.ini'
DATASRC_DIR = './datasrc'
DATASET_DIR = './dataset'
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

    # Extract TARGET Datasets
    if 'CAPES' in TARGETS:
        print('Extracting CAPES dataset...skiped!')

    if 'RAIS' in TARGETS:
        print('Extracting RAIS dataset', end='')

        rais_ds_list = config['RAIS']['CONJUNTOS'].split(',\n')

        for i in rais_ds_list:
            print('.', end='')
            with py7zr.SevenZipFile(i, mode='r') as z:
                z.extractall(path=DATASET_DIR)

        print('ok')


    #for index, ds in df.iterrows():
    #    fname = DATASRC_DIR + '/' + ds['arquivo']

    #    if os.path.isfile(fname):
    #        fhash = Fetcher().check_hash(fname)
    #        if fhash != ds['hash_md5']:
    #            print('WARN: Arquivo {} corrompido! Baixando.'
    #                  'novamente '.format(fname), end='', flush=True)
    #            Fetcher().get_urllib(ds['url'], fname)
    #        else:
    #            print('Arquivo {} íntegro. Download ignorado.'.format(fname))
    #    else:
    #        print('Arquivo {} não localizado. '
    #              'Iniciando Download. '.format(fname), end='', flush=True)
    #        Fetcher().get_urllib(ds['url'], fname)

