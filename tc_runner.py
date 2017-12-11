"""Test Case Runner.

DATA-2968
Module for running all test cases against source data.

"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TestResult:
    """Object representing a partial test status."""
    def __init__(self, column):
        self.test_date = datetime.utcnow().isoformat()
        self.req = ""
        self.column = column
        self.rate = None
        self.min_rate = None
        self.max_rate = None
        self.status = None
        self.data_source = None

    def __str__(self):
        out = "\n"
        out += "\n Validation {}:".format(self.status)
        out += "\n Data Source: {}.{}".format(self.data_source.name, self.column)
        out += "\n Check Type:  {}".format(self.req)
        out += "\n Expected:    {}-{}".format(self.min_rate, self.max_rate)
        out += "\n Got:         {}".format(self.rate)
        return out

    def set_data_source(self, data_source):
        self.data_source = data_source


class TestResults(list):
    """Object representing a list of test results."""
    def __init__(self):
        super(TestResults, self).__init__()


class TestCaseRunner:
    """Runs test cases and returns results. """
    def __init__(self):
        logger.info('Starting test case runner')
        self.test_results = TestResults()

    def run_test_cases(self, test_metadata):
        """Runs test cases and returns results.

        Invoked externally, and so this may be done multiple times per
        execution of top-level.

        For Each Test Cases:
          Runs Test Case
          Adds Test Result to Test Metadata
        """
        test_cases = test_metadata.test_case_summary
        for test_case in test_cases:
            # Note that some individual instances of test_case
            # will be treated as multiple test cases, esp.
            # when multiple columns are checked independently
            test_case.run_test_case(test_metadata)

