from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json

from pyspark.sql import SparkSession

CONFIG = 'olist_guest.json'
filename = "olist_customers_dataset.csv"
bronze_foldername = "customers"

def csv_to_parquet():
    with open(CONFIG, 'r') as file:
        data = json.load(file)

    BUCKET = data["bucket"]
    INPUT_CSV = f"s3a://{BUCKET}/raw/{filename}"
    OUTPUT_PARQUET = f"s3a://{BUCKET}/bronze/{bronze_foldername}/"

    spark = (
            SparkSession.builder
            .appName("csv_to_parquet")
            .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.4.1")
            .getOrCreate()
        )

    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )

    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    hadoop_conf.set(
        "fs.s3a.endpoint",
        "s3.ap-southeast-2.amazonaws.com"
    )

    df = spark.read.csv(
        INPUT_CSV,
        header=True,
        inferSchema=True
    )

    (
        df.write
        .mode("overwrite")
        .parquet(OUTPUT_PARQUET)
    )

    print("Conversion successful")

    spark.stop()

with DAG(
    dag_id="csv_to_parquet_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    convert_task = PythonOperator(
        task_id="convert_csv_to_parquet",
        python_callable=csv_to_parquet
    )