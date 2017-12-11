""" Top-level CLI for data validation task

Example of run command at hadoop gateway:
spark-submit --executor-memory 15g  --executor-cores 3 --num-executors 3 data-validator.py --data-source-name 'source_name' --data-source-type 'table'
"""

import argparse
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)-8s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

from validator import run_validator

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-source-name', help='Data source name. e.g. dim_member', required=True)
    parser.add_argument('--data-source-type', help='Data source type. e.g. table, file', required=True)
    parser.add_argument('--criticality', help='Validation criticality level. e.g. Blocking, Low', default='low')
    parser.add_argument('--scope', help='Validation scope. e.g. incremental, full', default='full')

    args = parser.parse_args()

    run_validator(
        data_source_name=args.data_source_name,
        data_source_type=args.data_source_type,
        criticality=args.criticality,
        scope=args.scope)
