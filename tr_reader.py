"""Test Results Reader.

DATA-2970
Module for retrieving and processing historical test results.

Used in validating requirements where criteria reference historical
behavior.
"""

import logging

logger = logging.getLogger(__name__)

TEST_RESULT_METADATA_TARGET = {'name': 'qa_test_results_log', 'type': 'table'}
RAW_TEST_RESULT_MOCK_1 = {'test_date': '2017-10-10T10:10:10', 'target': {'name': 'dim_foo', 'type': 'table'}, 'column': 'col1', 'req': 'not null', 'status': 0.9}
RAW_TEST_RESULT_MOCK_2 = {'test_date': '2017-10-11T11:11:11', 'target': {'name': 'dim_foo', 'type': 'table'}, 'column': 'col2', 'req': 'not null', 'status': 0.0}
RAW_TEST_RESULT_MOCK = [RAW_TEST_RESULT_MOCK_1, RAW_TEST_RESULT_MOCK_2]


class HistoricalDataRow():
    """Object representing a data row of historical test results.

    DATA-2961
    """

    def __init__(self, row):
        self.data_row = row


class HistoricalData(list):
    """Object representing a collection of historical test results."""
    def __init__(self, raw_input=[]):
        logger.info('Creating historical data object')
        self.raw_data = []
        for row in raw_input:
            self.raw_data.append(HistoricalDataRow(row).data_row)

    def print_raw(self):
        for result in self.raw_data:
            logger.info("RESULT: {}".format(result))


class TestResultReader:
    """Retrieves and processes historical test results.

    Needs to do the following:
    - Formulate Query for Existing Results
    - Query Existing Results
    - Ingest Existing Results
    """

    def __init__(self, test_metadata):
        logger.info('Starting test status reader')
        self.sql_context = test_metadata.sql_context
        self.data_source = test_metadata.data_source

        self.tr_source = test_metadata.test_case_metadata_source.name
        self.df = self.get_data_frame()
        test_metadata.test_history_data_frame = self.df
        self.raw_data = None
        self.get_raw_results()

        # TODO: UNMOCK
        self.raw_data = HistoricalData(RAW_TEST_RESULT_MOCK)

        self.history = self.process_results()
        test_metadata.history = self.history

    def get_data_frame(self):
        df0 = self.sql_context.table(self.tr_source)
        df = df0.filter(df0.table_name == 'edw.edw_dim_member_stg') # self.data_source.name)
        return df

    def get_raw_results(self):
        if not self.raw_data:
            self.raw_data = self.df.collect()

    def print_raw_results(self):
        self.get_raw_results()
        for result in self.raw_data:
            logger.info("RESULT: {}".format(result))

    def process_results(self):
        """Processes and accumulates raw results into consolidated obj"""
        self.get_raw_results()

        # TODO: UNMOCK
        history = self.raw_data
        return history
