import argparse
from modules.etl import datasets


# Default settings
CONFIG_FILE = 'config.ini'

# argparse setup
ds_choices = list(datasets.keys())
parser = argparse.ArgumentParser(description="DWBRA's ETL Process")

# Arguments
parser.add_argument(
    '-c',
    '--config-file',
    type=str,
    default=CONFIG_FILE,
    help="Config file",
    metavar=''
)
parser.add_argument(
    '--quiet',
    action='store_true',
    help="decrease output verbosity",
)

# Commands
subparsers = parser.add_subparsers(
    help=f'available command',
    metavar='COMMAND',
    dest='subparser_name',
)

# 'all' subparser
parser_all =  subparsers.add_parser('all', help='run all ETL jobs')
parser_all.add_argument(
    # '-t',
    # '--tables',
    dest='tables',
    help=f'Run only listed datasets ({", ".join(ds_choices)})',
    metavar='DATASET',
    nargs='*',
    default='all',
    choices=['all'] + (ds_choices),
)

# 'stg' subparser
parser_stg =  subparsers.add_parser(
    'stg', help='run staging table\'s ETL jobs',
)
parser_stg.add_argument(
    dest='tables',
    help=f'ETL job name (ex.: caged, rais, etc)',
    metavar='JOB',
    nargs='*',
    default='all',
    choices=['all'] + (ds_choices),
)

# 'dim' subparser
parser_dim =  subparsers.add_parser(
    'dim', help='run dimension table\'s ETL jobs',
)
parser_dim.add_argument(
    dest='tables',
    help=f'Run only listed datasets ({", ".join(ds_choices)})',
    metavar='JOB',
    nargs='*',
    default='all',
    choices=['all'] + (ds_choices),
)

# 'fact' subparser
parser_fact =  subparsers.add_parser(
    'fact', help='run fact table\'s ETL jobs',
)
parser_fact.add_argument(
    dest='tables',
    help=f'Run only listed datasets ({", ".join(ds_choices)})',
    metavar='JOB',
    nargs='*',
    default='all',
    choices=['all'] + (ds_choices),
)

args = parser.parse_args()
