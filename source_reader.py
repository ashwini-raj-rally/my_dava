"""Source Data Reader.

DATA-2967, 2803
Module for retrieving and processing source data.
"""

import logging
logger = logging.getLogger(__name__)


class SourceReader:
    """Retrieves and processes data from source under test

    DATA-2967
    """

    def __init__(self, test_metadata, spark):
        logger.info('Starting source data reader')
        self.sql_context = test_metadata.sql_context
        # self.data_source = test_metadata.data_source
        # test_metadata.source_data_frame = self.df
        # self.raw_data = None
        # self.data = self.process_source_data()
        self.spark = spark

    def get_data_frame(self,table_name):
        """Gets data frame for the data source under test"""
        template = "CREATE TEMPORARY FUNCTION {} as '{}'".format('decrypt_hs',
                                                                 'com.rallyhealth.data.honeycomb.udf.legacy.LegacyDecryptStringWithHashedSalt')
        jar = "ADD JAR hdfs:///integration/jars/data/honeycomb-v0.jar"
        #self.sql_context.sql(jar)
        #self.sql_context.sql(template)
        #self.spark.sql(jar)
        #self.spark.sql(template)
        df = (
            self.spark
            .table(table_name)
            )
        return df

    def get_raw_data(self):
        """Composes the raw source data set"""
        if not self.raw_data:
            self.raw_data = self.df.collect()

    def print_raw_source_data(self):
        """Prints raw source data to console"""
        self.get_raw_data()
        for row in self.raw_data:
            logger.info("SOURCE DATA ROW: {}".format(row))

    def process_source_data(self):
        """Processes raw source data"""

        # TODO: UNMOCK
        decrypted_data = self.decrypt_data()

        return decrypted_data

    def decrypt_data(self):
        """Decrypts raw source data

        DATA-2803

        """
        self.get_raw_data()

        # TODO: UNMOCK
        decrypted_data = self.raw_data

        return decrypted_data
