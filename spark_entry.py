from pyspark import SparkConf

from init_db import init_db
from pyspark.sql import SparkSession
from jobs.users_ingestion_job import UsersIngestionJob
import json
import argparse

from utils.config_parser import ConfigReader
from utils.global_var_helper import jobsconfig_folder, config_file_path
from utils.misc import *


def create_spark_session(app_name='MySparkApp', local=True):
    # if local:
    spark_conf = SparkConf()
    access_conf = ConfigReader(config_file_path).get_aws_iam_config()
    spark_conf.set('spark.hadoop.fs.s3a.access.key', access_conf['access_key_id'])
    spark_conf.set('spark.hadoop.fs.s3a.secret.key', access_conf['access_secret'])
    spark_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                   '.SimpleAWSCredentialsProvider')
    spark_conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1')
    spark_conf.set('spark.jars',
                   'D:\Workspace\spark-3.2.4-bin-hadoop3.2\spark-3.2.4-bin-hadoop3.2\jars\postgresql-42.6.0.jar')

    return SparkSession.builder.master('local[*]') \
        .appName(app_name) \
        .config(conf=spark_conf) \
        .getOrCreate()
    # else:
    #    return SparkSession.builder.appName(app_name).getOrCreate()


if __name__ == '__main__':

    # Create a command-line argument parser
    parser = argparse.ArgumentParser(description='Run a Spark job with command-line arguments')

    # Define the expected command-line arguments
    parser.add_argument('--local', help='Whether to create a local session')
    parser.add_argument('--job_name', help="Name of the Spark job")
    parser.add_argument('--start_date', help='Start date for the job')

    # Parse the command-line arguments
    args = parser.parse_args()

    spark = create_spark_session(local=args.local)

    if args.job_name == "users_ingestion_job":
        init_db()
        job_config = read_job_config(jobsconfig_folder, args.job_name)
        job = UsersIngestionJob(spark, args.job_name, job_config)
        job.execute()
    else:
        pass
