"""Test Results Writer.

DATA-2969, 2971
Module for persisting historical test results to log / test results table.
"""

from tr_reader import *
import logging
import datetime

logger = logging.getLogger(__name__)

PASSED = 'passed'
FAILED = 'FAILED'

class TestResultWriter:
    """Writes historical test results to log / test results table.

    Needs to do the following:
    - Process Test Results
    - Persist Test Results
    - Transmit Test Results
    """

    def __init__(self, spark):
        logger.info('Starting test status writer')
        #self.test_results = test_results
        self.spark = spark

    def process_test_results(self):
        """Processes test results

        For now, this simply forms an aggregate status from among individual
        test results.

        TODO: Persist Test Results to Log Table
        """
        logger.info('Processing test results')
        final_status = PASSED
        for result in self.test_results:
            if result.status == FAILED:
                final_status = FAILED
        return final_status

    def print_test_results(self):
        """Prints test results to console"""
        for test_result in self.test_results:
            logger.info("TEST RESULT: {}".format(test_result))

    def result_log(self,table_name,column_name,validation_type,result,start_ts,end_ts,status):
        run_date = datetime.datetime.today().strftime('%Y-%m-%d')
        data = [(table_name, column_name, validation_type, run_date, result, start_ts, end_ts, status)]
        schema = ['table_name', 'column_name', 'validation_type', 'run_date', 'result', 'start_time', 'end_time',
                  'status']
        new_row = self.spark.createDataFrame(data, schema)
        (
            new_row
                .write
                .insertInto("qa.data_validator_log")
        )
