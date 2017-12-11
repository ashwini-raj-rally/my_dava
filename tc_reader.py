"""Test Case Reader.

Module for retrieving and processing test cases from test case metadata table.
"""

import logging
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, Row
from test_cases import TestCase

logger = logging.getLogger(__name__)

class TestCaseReader:
    """Retrieves and processes test case metadata.

    DATA-2966
    """

    def __init__(self, test_metadata):
        logger.info('Starting test case reader')
        #self.sql_context = test_metadata.sql_context
        #self.data_source = test_metadata.data_source
        #self.criticality = test_metadata.criticality
        #self.scope = test_metadata.scope
        self.spark = (
            SparkSession
                .builder
                .appName('DAVA Framework')
                .config("spark.dynamicAllocation.enabled", "false")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .enableHiveSupport()
                .getOrCreate()
        )
        self.tc_source = 'qa.data_validator_metadata'
        self.table_name = test_metadata.data_source
        self.df = self.get_data_frame()
        test_metadata.test_cases_data_frame = self.df
        self.raw_data = None
        self.get_raw_test_cases()
        self.test_case_summary = list()
        self.process_test_cases()
        test_metadata.test_case_summary = self.test_case_summary
        #test_metadata.columns = self.get_testworthy_columns()
        #test_metadata.filter = self.get_data_source_filter()

    def get_data_frame(self):
        """Gets data frame for the test case metadata table"""
        df = (
            self.spark
            .table(self.tc_source)
            .filter(col('table_name') == self.table_name)
        )
        return df

    def get_raw_test_cases(self):
        """Composes the raw list of test cases"""
        if not self.raw_data:
            self.raw_data = self.df.collect()

    def get_testworthy_columns(self):
        """Returns list of column names that should be obtained
        in order to fully test source data per test cases
        """
        columns = list(set(self.df.select(self.df.validation_column).collect()))
        return columns

    def get_data_source_filter(self):
        """Returns a data filter to be used to pare down
        source data to only those which are to be tested
        """
        # TODO: UNMOCK
        f = col('difficulty') == 'hard'
        return f

    def print_raw_test_cases(self):
        """Prints raw test cases to console"""
        self.get_raw_test_cases()
        for test_case in self.raw_data:
            logger.info("TEST CASE: {}".format(test_case))

    def process_test_cases(self):
        """Processes and accumulates raw test cases into consolidated obj"""
        self.get_raw_test_cases()
        for test_case in self.raw_data:
            self.test_case_summary.append(TestCase(test_case, self.spark))

        # TODO: CONSOLIDATE AS APPLICABLE (cf. DATA-2966)

