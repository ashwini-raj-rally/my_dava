"""Test Case Battery.

Module for executing specific test cases against source data.

"""

import datetime
import time
from tr_writer import *
from tc_runner import TestResult
from tr_writer import TestResultWriter
from source_reader import SourceReader
from pyspark.sql import SparkSession, Row, SQLContext


def run(source_df,table_name, val_type,column, num_rows, min_rate, max_rate, spark):
    test_results = TestResultWriter(spark)
    if val_type == 'null_check':
        split_columns = str(column).replace(' ', '').split(',')
        for cols in split_columns:
            logger.info('cols in split_column: %s', cols)
            start_ts = datetime.datetime(*time.gmtime()[:6])
            result = run_null_check(source_df, cols, num_rows, min_rate, max_rate)
            end_ts = datetime.datetime(*time.gmtime()[:6])
            test_results.result_log(table_name, result.column, val_type, result.rate, start_ts, end_ts,
                                    result.status)
    elif val_type.endswith("_sql"):
        start_ts = datetime.datetime(*time.gmtime()[:6])
        result = run_custom_sql(val_type, num_rows,column, min_rate, max_rate, spark)
        end_ts = datetime.datetime(*time.gmtime()[:6])
        test_results.result_log(table_name, result.column, val_type, result.rate, start_ts, end_ts,
                                result.status)
    else:
        logger.info('do nothing')


def run_null_check(source_df, column, num_rows, min_rate, max_rate):
    """Performs test for null check on a given data source column."""
    # Create the Test Result Object
    tr = TestResult(column)
    null_count = 0
    null_count += source_df.filter(source_df[column].isNull()).count()
    null_count += source_df.filter(source_df[column] == '').count()

    null_rate = int(float(null_count) / num_rows * 100)
    if min_rate <= null_rate <= max_rate:
        status = PASSED
    else:
        status = FAILED

    # Update the Test Result Object
    tr.min_rate = min_rate
    tr.max_rate = max_rate
    tr.rate = null_rate
    tr.status = status

    return tr


def run_custom_sql(val_type, num_rows,column, min_rate, max_rate, spark):
    tr = TestResult(column)
    result = spark.sql(column)
    result_rate = int(float(result.count()) / num_rows * 100)
    if min_rate <= result_rate <= max_rate:
        status = PASSED
    else:
        status = FAILED

    # Update the Test Result Object
    tr.min_rate = min_rate
    tr.max_rate = max_rate
    tr.rate = result_rate
    tr.status = status
    tr.column = val_type

    return tr


class TestCase(dict):
    """Object representing a single test case metadata row.

    DATA-2960
    """
    def __init__(self, row, spark):
        self.data_row = row
        logger.info('Data Row: {}'.format(self.data_row))
        self.table_name = row['table_name']
        self.decrypt_funcs = row['decrypt_funcs']
        self.criticality = row['criticality']
        self.validation_type = row['validation_type']
        self.validation_column = row['validation_column']
        self.run_frequency = row['run_frequency']
        self.load_type = row['load_type']
        self.min_expected_value = row['min_expected_value']
        self.max_expected_value = row['max_expected_value']
        self.update_ts = row['update_ts']
        self.created_by = row['created_by']
        self.table_dependency = row['table_dependency']
        self.min_rate = float(self.min_expected_value)
        self.max_rate = float(self.max_expected_value)
        self.spark = spark

    def run_test_case(self, test_metadata):
        """Runs a single test case."""
        # LOTS OF MAGIC HERE
        #self.sql_context = test_metadata.sql_context
        logger.info('Running test case: {}'.format(self.data_row))
        source_reader = SourceReader(test_metadata,self.spark)
        source_df = source_reader.get_data_frame(self.table_name)
        num_rows = source_df.count()
        run(source_df, self.table_name, self.validation_type, self.validation_column, num_rows, self.min_rate, self.max_rate, self.spark)

