from pyspark.sql.functions import *
from pyspark.sql.window import Window

from jobs.spark_job import PysparkJob
from data_utils.dataframe_helper import *
from utils.s3_connector import S3Connector
from utils.config_parser import ConfigReader
from utils.global_var_helper import *

# can create a catalog instead
TABLE_COLS = ['username', 'address', 'description', 'email', 'value_date']
FILES_COLS = ['username', 'address', 'description', 'email', 'filename']


def add_value_date_to_user_df(df):
    user_df = df\
        .withColumn('value_date', regexp_extract(col('filename'),
                                                 r'data-(\d{4}-\d{2}-\d{2})\.csv', 1))
    return user_df


def sort_users_to_write(user_df, user_table):
    value_date_window = Window.partitionBy('email').orderBy(col('value_date').desc())
    all_users = user_df.unionByName(user_table).withColumn('rank', rank().over(value_date_window)) \
        .where(col('rank') == 1).drop('rank')
    return all_users


class UsersIngestionJob(PysparkJob):
    def __init__(self, spark, name, config):
        super().__init__(spark, name)
        self.bucket_to_scan = config['bucket_to_scan']
        self.table_to_write = config['table_to_write']
        self.save_mode = config['save_mode']
        self.dry_run = eval(config['dry_run'])
        self.aws_access = ConfigReader(config_file_path).get_aws_iam_config()
        self.db_access = ConfigReader(config_file_path).get_db_config()
        self.s3_connector = S3Connector(self.aws_access)

    def start(self):
        list_of_files = self.s3_connector.list_files(self.bucket_to_scan, '')
        list_of_files = [s3_prefix + self.bucket_to_scan + '/' + x for x in list_of_files]
        user_df = self.spark.read.csv(*list_of_files, header=True, inferSchema=True).withColumn('filename',
                                                                                                input_file_name())

        user_table = read_postgrestable_spark(self.spark, self.table_to_write, self.db_access)

        if not (check_format_is_respected(user_df, FILES_COLS) and check_format_is_respected(user_table, TABLE_COLS)):
            raise Exception("Dataframe don't match expected format")

        return {
            'user_df': user_df,
            'user_table': user_table
        }

    def run(self, start_output=None):
        if start_output['user_df'] is None or start_output['user_table'] is None:
            raise Exception('Input data is required.')

        user_df = add_value_date_to_user_df(start_output['user_df']).select(TABLE_COLS)
        user_table = start_output['user_table']

        all_users = sort_users_to_write(user_df, user_table)

        return {'users_to_write': all_users}

    def end(self, run_output=None):
        if run_output is None:
            raise Exception('Previous output is required.')

        users_to_write_df = run_output['users_to_write']
        if not self.dry_run:
            write_dataframe_to_postgres(users_to_write_df, self.table_to_write, self.save_mode, self.db_access)
            print(f'CSV data has been written to Postgres table at {self.table_to_write}')
        print('end of job')
