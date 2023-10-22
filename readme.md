
# CB Silver - Users ingestor

The goal of this project is to ingest users from a CSV file found on an AWS S3 bucket

Python was used as a base language and PySpark for the data reading, transformation and writing needs.

## Setup
1. Use pip to install the required libraries:
```pip install -r requirements.txt```

2. Fill the the config_template.txt file with the desired informations and rename the extension to .ini

3. Make sure you have the required pyspark to run the code locally (3.3.1) as well as the PySpark libraries (spark.hadoop.fs.s3a, org.apache.hadoop:hadoop-aws and postgresql)

4. Obivously, you'll need an S3 bucket and a Postgresql RDS  with the required access rights, the naming convetion of the files is "data-yyyy-mm-dd.csv"

## Run

The spark_entry.py works a bit like a SparkJob factory:

1. Creates a SparkSession
2. Runs an SQL query to initialize the database (create table if not exist)
3. Based on provided arguments, instantiate the SparkJob
4. Calls the run method of the job

here's an example:
```spark_entry.py --local=True --job_name=users_ingestion_job --start_date=0```

The start_date is not implemented yet, but still required

The local argument is used to create a local SparkSession, as of now it's always local, but could be provided by an EMR cluster for example.

Lastly, the job name is simply for the if statement and log purposes.

## To do

1. Improve the security access providing system (use env var locally and arn on AWS)
2. Add a catalog for each job
3. Add a CI/CD for EMR deployment
4. Thinking