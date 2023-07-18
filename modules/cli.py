import argparse
from modules.etl import datasets


# Default settings
CONFIG_FILE = 'config.ini'

# argparse setup
ds_choices = list(datasets.keys())
table_choices = []
for ds in ds_choices:
    group_choices = list(datasets[ds].keys())
    for group in group_choices:
        for table in datasets[ds][group]:
            table_choices.append(table)

parser = argparse.ArgumentParser(description="Data Warehouse ETL Process")

# Arguments
parser.add_argument(
    '--target',
    type=str,
    default='parquet',
    help="DW's load target",
    metavar='parquet',
    choices=['parquet', 'postgres'],

)
parser.add_argument(
    '--no-sample',
    action='store_true',
    help="do not update sample db",
)
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
    dest='datasets',
    help=f'Run only listed datasets ({", ".join(ds_choices)})',
    metavar='DATASET',
    nargs='*',
    default='all',
    choices=['all'] + (ds_choices),
)

# 'table' subparser
parser_table =  subparsers.add_parser(
    'table', help='run table\'s ETL jobs'
)
parser_table.add_argument(
    dest='tables',
    help=f'Run only listed tables ({", ".join(table_choices)})',
    metavar='JOB',
    nargs='*',
    # default='all',
    choices=table_choices,
)

# 'stg' subparser
parser_stg =  subparsers.add_parser(
    'stg', help='run staging table\'s ETL jobs',
)
parser_stg.add_argument(
    dest='dataset',
    help=f'ETL job name (ex.: caged, municipiosbrasileiros, etc)',
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
    dest='dataset',
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
    dest='dataset',
    help=f'Run only listed datasets ({", ".join(ds_choices)})',
    metavar='JOB',
    nargs='*',
    default='all',
    choices=['all'] + (ds_choices),
)

args = parser.parse_args()
