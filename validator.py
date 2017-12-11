"""Data Validator (DaVa).

Module for running Rally's Matrix Data Validation Task.
"""

from tc_reader import *
from tr_reader import *
from source_reader import *
from tc_runner import *
from tr_writer import *
from test_cases import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

logging.basicConfig(
    format='%(asctime)s - %(levelname)-8s - %(module)s - %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_SOURCE_TYPE_TABLE = 'table'
DATA_SOURCE_TYPE_FILE = 'file'
SUPPORTED_DATA_SOURCE_TYPES = [
    DATA_SOURCE_TYPE_TABLE
]

CRITICALITY_BLOCKING = 'Blocking'
CRITICALITY_LOW = 'Low'
TEST_CASE_METADATA_TABLE_NAME = 'qa.data_validator_metadata'
TEST_RESULT_TABLE_NAME = 'qa.data_validator_log'


class DataSourceTypeNotSupported(Exception):
    """Exception handler for unsupported data source types.

    Parameters:
     source_type (str): Data Source Type
    """

    def __init__(self, source_type):
        super(DataSourceTypeNotSupported, self).__init__()
        self.source_type = source_type

    def __str__(self):
        return 'Data Source Type [{}] not supported.'.format(self.source_type)


class DataSource:
    """Object representing data source to be validated."""

    def __init__(self, name, source_type):
        self.name = name
        self.source_type = source_type
        self.validate_type()

    def validate_type(self):
        if self.source_type not in SUPPORTED_DATA_SOURCE_TYPES:
            raise DataSourceTypeNotSupported(self.source_type)

    @property
    def is_a_table(self):
        return self.source_type == DATA_SOURCE_TYPE_TABLE

    @property
    def is_a_file(self):
        return self.source_type == DATA_SOURCE_TYPE_FILE

class TestMetadata:
    """Object representing test metadata, to be passed among modules.

    This includes:
      Top-Level Inputs:
        Criticality
        Scope
        Data Source Name and Type
      Derived Information:
        List of Data Source column names under test
        Data Source Filter
      Given / Global Information:
        Test Case Metadata Table Name
        Test Result Log Table Name
      Objects:
        Pyspark's SQL Context
        Test Metadata:
          Source Data Frame (pyspark)
          Test Case Metadata Data Frame (pyspark)
          Test Case Summary (distilled list of test cases)
          Test Results Data Frame (pyspark)
          Historical Data (list of historical results pertinent to test cases)


    """

    def __init__(self, data_source_name, data_source_type, criticality, scope):
        #self.criticality = criticality
        #self.scope = scope
        self.columns = []
        self.filter = None

        # Create Data Source object
        #self.data_source = DataSource(data_source_name, data_source_type)
        self.data_source = data_source_name
        # Test Case Metadata and Results Log are also DataSources
        #self.test_case_metadata_source = \
            #DataSource(TEST_CASE_METADATA_TABLE_NAME, DATA_SOURCE_TYPE_TABLE)
        #self.test_result_source = \
            #DataSource(TEST_RESULT_TABLE_NAME, DATA_SOURCE_TYPE_TABLE)

        # Create SQL Context
        conf = SparkConf().setAppName("Data Validation")
        sc = SparkContext(conf=conf)
        self.sql_context = HiveContext(sc)

        # Set Overrides
        # Until approval granted by Avi and Paul:
        self.criticality = CRITICALITY_LOW

        self.source_data_frame = None
        self.test_cases_data_frame = None
        self.test_case_summary = None
        self.test_results_data_frame = None
        self.history = None


def run_validator(data_source_name, data_source_type, criticality, scope):
    """Run the validator task at top-level
    DATA-2802

    Parameters:
     data_source_name: (string) Data Source Name (e.g. 'dim_member')
     data_source_type: (enum) Data Source Type (e.g. 'table', 'file')
     criticality: Validation Criticality Level (enum) (e.g. 'Blocking', 'Low')
     scope: Validation Scope (enum) (e.g. 'Full', 'Incremental')
    """

    logger.info('Starting data validator')

    # Create Test Metadata Object
    # (not to be confused with Test *Case* Metadata)
    test_metadata = TestMetadata(data_source_name, data_source_type, criticality, scope)

    # Create Test Case Reader, which will add Test Case Object to Test Metadata
    tcr = TestCaseReader(test_metadata)
    tcr.print_raw_test_cases()

    # Test Result Reader out of scope for now
    # # Create Test Result Reader
    # # Create historical record / History Object
    # trr = TestResultReader(test_metadata, test_case_summary)
    # history = trr.get_history()
    # trr.print_history()

    # Create Data Source Reader, which will add Source Data object to Test Metadata
    # data_reader = SourceReader(test_metadata)
    # data_reader.print_raw_source_data()

    # Create Test Case Runner
    # Run Test Cases and get Test Results object
    test_runner = TestCaseRunner()
    test_runner.run_test_cases(test_metadata)
    test_results = test_runner.test_results

    # Create Test Results Writer
    # Process the Test Results object and generate return object
    # Print the test results
    trw = TestResultWriter(test_results)
    #result = trw.process_test_results()
    #trw.print_test_results()

    logger.info('\n\nData validator completed')

    # Transmit the test results
    #return result
