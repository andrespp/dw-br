import argparse

# Default settings
CONFIG_FILE = 'config.ini'

# argparse setup
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
    '-v',
    '--verbose',
    action='store_true',
    help="increase output verbosity",
)

# Commands
subparsers = parser.add_subparsers(
    help=f'available command',
    metavar='COMMAND',
    # required=True,
    dest='subparser_name',
)

# 'all' subparser
parser_all =  subparsers.add_parser('all', help='run all ETL jobs')
parser_all.add_argument(
    '-n',
    '--name',
    help=f'Run only for specifc dataset name (ex.: rais)',
    metavar='DATASET',
    nargs=1,
    # choices=sync_jobs_choices,
)

# 'stg' subparser
parser_stg =  subparsers.add_parser(
    'stg', help='run staging table\'s ETL jobs',
)
parser_stg.add_argument(
    '-t',
    '--tables',
    help=f'ETL job name (ex.: caged, rais, etc)',
    metavar='JOB',
    nargs='+',
    # choices=sync_jobs_choices,
)

# 'dim' subparser
parser_dim =  subparsers.add_parser(
    'dim', help='run dimension table\'s ETL jobs',
)
parser_dim.add_argument(
    '-t',
    '--tables',
    help=f'ETL job name (ex.: municipio, etc)',
    metavar='JOB',
    nargs='+',
    # choices=sync_jobs_choices,
)

# 'fact' subparser
parser_fact =  subparsers.add_parser(
    'fact', help='run fact table\'s ETL jobs',
)
parser_fact.add_argument(
    '-t',
    '--tables',
    help=f'ETL job name (ex.: municipio, etc)',
    metavar='JOB',
    nargs='+',
    # choices=sync_jobs_choices,
)

args = parser.parse_args()
