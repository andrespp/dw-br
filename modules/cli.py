import argparse

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


