import configparser
import os.path
import traceback
import uetl
from modules.cli import args
from dw.aux_create_tables import DW_TABLES

CONFIG = None
VERBOSE = None
CHUNKSIZE = None
DWO = None

def init_app():
    """
    Initialize app's variables
    """
    global CONFIG
    global VERBOSE
    global CHUNKSIZE
    global DWO

    CONFIG_FILE=args.config_file
    VERBOSE=args.verbose

    if(VERBOSE):
        print("Initizalizating ETL Process with parameters:")
        print("CONFIG_FILE: '{}'".format(CONFIG_FILE))
        print("VERBOSE: '{}'\n".format(VERBOSE))

    # Read configuration File
    if not os.path.isfile(CONFIG_FILE):
        print('ERROR: file "{}" does not exist'.format(CONFIG_FILE))
        exit(-1)
    try:
        CONFIG = configparser.ConfigParser()
        CONFIG.read(CONFIG_FILE)
        if CONFIG.has_option('ETL', 'CHUNKSIZE'):
            CHUNKSIZE = int(CONFIG['ETL']['CHUNKSIZE'])
        else:
            CHUNKSIZE = None
    except Exception as e:
        print('ERROR: Unable to read config file ("{}")'.format(CONFIG_FILE))
        traceback.print_exc()
        exit(-1)

    ###########################################################################
    ### Check DATA WAREHOUSE connection
    # Initialize Data Warehouse object
    DWO = uetl.DataWarehouse(name=CONFIG['DW']['NAME'],
                             dbms=CONFIG['DW']['DBMS'],
                             host=CONFIG['DW']['HOST'],
                             port=CONFIG['DW']['PORT'],
                             base=CONFIG['DW']['BASE'],
                             user=CONFIG['DW']['USER'],
                             pswd=CONFIG['DW']['PASS'])

    # Test dw db connection
    if DWO.test_conn():
        if(VERBOSE): print('Data Warehouse DB connection succeed!')
    else:
        print('ERROR: Data Warehouse DB failed!')
        exit(-1)

    ###########################################################################
    # Test if DW's tables exists
    DWO.create_tables(DW_TABLES, verbose=VERBOSE)

# init_app() ######################
# print(CHUNKSIZE)
# print(CONFIG)
