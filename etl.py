#!/usr/bin/env python3
"""etl.py
"""
import configparser
import argparse
import time
import os.path
import lib.datawarehouse as dw
import lib.datasrc_mssql as dsm
from dw.aux_create_tables import DW_TABLES
from dw import aux_dw_updates
from dw import dim_municipio

# Track execution time
start_time = time.time()

# Default settings
CONFIG_FILE = 'config.ini'

# argparse setup
parser = argparse.ArgumentParser(
    description="DWBRA's ETL Process")
parser.add_argument('-c',
                    '--config-file',
                    type=str,
                    default=CONFIG_FILE,
                    help="Config file",
                    metavar='')
parser.add_argument('-s',
                    '--since',
                    required=False,
                    type=int,
                    help="start period in form YYYYMM",
                    metavar='')
parser.add_argument('-v',
                    '--verbose',
                    action='store_true',
                    help="increase output verbosity",
                    )
mode = parser.add_mutually_exclusive_group(required=True)
mode.add_argument('-a',
                  '--all',
                  action='store_true',
                  help='build all targets (whole data warehouse)'
                 )
mode.add_argument('-b',
                  '--build',
                  dest='targets',
                  action='append',
                  choices=['stg', 'aux', 'dim', 'fact', 'date'],
                  metavar='',
                  help="targets to be build: 'stg', 'aux', 'dim', 'fact', 'date'"
                  )
args = parser.parse_args()

# Script setup
CONFIG_FILE=args.config_file
START_PERIOD=args.since
VERBOSE=args.verbose
FULL_DW = args.all
TARGETS = args.targets

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

    if(VERBOSE):
        print("Initizalizating ETL Process with parameters:")
        print("CONFIG_FILE: '{}'".format(CONFIG_FILE))
        print("START_PERIOD: '{}'".format(START_PERIOD))
        print("VERBOSE: '{}'\n".format(VERBOSE))
        if(FULL_DW):
            print("The whole data warehouse will be build.")
        else:
            print("The following targets will be build: {}".format(TARGETS))


    ############################################################################
    ##### Connections

    ### DATA WAREHOUSE
    # Initialize Data Warehouse object
    DWO = dw.DataWarehouse(name=config['DW']['NAME'],
                           dbms=config['DW']['DBMS'],
                           host=config['DW']['HOST'],
                           port=config['DW']['PORT'],
                           base=config['DW']['BASE'],
                           user=config['DW']['USER'],
                           pswd=config['DW']['PASS'])

    # Test dw db connection
    if DWO.test_conn():
        if(VERBOSE): print('Data Warehouse DB connection succeed!')
    else:
        print('ERROR: Data Warehouse DB failed!')
        exit(-1)

    ### OLTP Systems (Datasets)

    # Municipios
    filename = config['MUNICIPIOS']['FILE']
    if not os.path.isfile(filename):
        print('ERROR: file "{}" does not exist'.format(filename))
        exit(-1)

    ###########################################################################
    ###### DW Update
    #
    # Test if DW's tables exists
    DWO.create_tables(DW_TABLES)

    if FULL_DW or ('date' in TARGETS):
        if(VERBOSE): print("\n# Building dim_date")

    if FULL_DW or ('stg' in TARGETS):
        if(VERBOSE): print("\n# Building staging tables")

    if FULL_DW or ('dim' in TARGETS):
        if(VERBOSE): print("\n# Building dimension tables")
        dim_municipio.load(DWO,
                           dim_municipio.transform(dim_municipio.extract(
                                       config['MUNICIPIOS']['FILE'], VERBOSE)),
                           truncate=False,
                           verbose=VERBOSE)
        #dim_ccusto.load(DWO,
        #                df=dim_ccusto.transform(dim_ccusto.extract(SPE)),
        #                truncate=True)

    if FULL_DW or ('fact' in TARGETS):
        if(VERBOSE): print("\n# Building fact tables")


    # Post Processing
    elapsed_time = (time.time() - start_time) / 60

    if FULL_DW or ('aux' in TARGETS):
        if(VERBOSE):
            print("\n# Building auxiliary tables")
            aux_dw_updates.load(DWO, hostname=config['ETL']['HOST'],
                                elapsed_time=elapsed_time,
                                truncate=False,
                                verbose=VERBOSE
                               )

    # Print out elapsed time
    if(VERBOSE):
        print("\nExecution time: {0:0.4f} minutes.".format(elapsed_time))
