import unittest
from unittest.mock import Mock, patch
from pyspark.sql.types import *
from jobs.users_ingestion_job import UsersIngestionJob
from spark_entry import create_spark_session
from utils.misc import read_job_config

TABLE_COLS = ['username', 'address', 'description', 'email', 'value_date']
FILES_COLS = ['username', 'address', 'description', 'email', 'filename']


class UsersIngestionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.spark = create_spark_session()
        self.config = read_job_config("D:/Workspace/cb_silver_project/jobs_config/", "csv_to_delta_job")

    def tearDown(self):
        self.spark.stop()

    @patch('utils.s3_connector.S3Connector')
    @patch('data_utils.dataframe_helper.read_postgrestable_spark')
    def test_csv_to_delta_job(self, mock_s3_connector, mock_read_postgres):
        schema = StructType([
            StructField("username", StringType(), True),
            StructField("address", StringType(), True),
            StructField("description", StringType(), True),
            StructField("email", StringType(), True),
            StructField("value_date", DateType(), True)
        ])

        mock_read_postgres_df = self.spark.createDataFrame([], schema)

        mock_s3_connector.return_value.list_files.return_value = ['data-2023-09-01.csv', 'data-2023-08-01.csv']

        mock_read_postgres.return_value = mock_read_postgres_df

        job = UsersIngestionJob(self.spark, "CSVToDeltaJob", self.config)
        result = job.start()
        self.assertEqual(len(result), 2)


if __name__ == '__main__':
    unittest.main()
