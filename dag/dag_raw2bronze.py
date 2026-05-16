from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json

from pyspark.sql import SparkSession

CONFIG = 'olist_guest.json'
filename = "olist_order_reviews_dataset.csv"
bronze_foldername = "order_reviews"

def csv_to_parquet():
    with open(CONFIG, 'r') as file:
        data = json.load(file)

    BUCKET = data["bucket"]
    INPUT_CSV = f"s3a://{BUCKET}/raw/{filename}"
    OUTPUT_PARQUET = f"s3a://{BUCKET}/bronze/{bronze_foldername}/"

    spark = (
            SparkSession.builder
            .appName("raw_to_bronze")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
            )
            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
            .config("spark.sql.catalog.olist","org.apache.iceberg.spark.SparkCatalog") 
            .config("spark.sql.catalog.olist.type", "hadoop") 
            .config("spark.sql.catalog.olist.warehouse", f"s3a://{BUCKET}/") 
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .getOrCreate() 
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
    dag_id="raw_to_bronze_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    convert_task = PythonOperator(
        task_id="raw_to_bronze",
        python_callable=csv_to_parquet
    )