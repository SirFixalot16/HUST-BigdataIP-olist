import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, md5, concat_ws, to_date, dayofweek, month, quarter, year, sum as spark_sum,
    translate, lower, regexp_replace, trim, length, datediff, unix_timestamp,
    radians, cos, sin, asin, sqrt, pow, avg as spark_avg, lit, count as spark_count, max as spark_max, date_format, when
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Medallion_Pipeline")


spark = SparkSession.builder.appName("Convert_CSV_To_Parquet").getOrCreate()
logger.info("Khởi động Spark tạm thời để chuyển đổi file...")

csv_dir = "/content/raw_csv"
bronze_dir = "/content/bronze"

logger.info("BẮT ĐẦU CHUYỂN ĐỔI CSV SANG PARQUET...")

def csv2Bronze():
    spark = SparkSession.builder.getOrCreate()
    bucket_name = 'olist-brazillian-ecommerce-bigdata'

    folder_mapping = {
        'orders': 'raw/olist/orders/',
        'customers': 'raw/olist/customers/',
        'products': 'raw/olist/products/',
        'order_items': 'raw/olist/order_items/',
        'payments': 'raw/olist/order_payments/',
        'reviews': 'raw/olist/order_reviews/',
        'sellers': 'raw/olist/sellers/',
        'geolocation': 'raw/olist/geolocation/',
        'translation': 'raw/olist/product_category_name_translation/',
    }

    uploaded_files = os.listdir('/content')

    for file in uploaded_files:
        if not file.endswith('.csv'):
            continue

        target_folder = 'raw/olist/misc/'

        for keyword, s3_folder in folder_mapping.items():
            if keyword in file:
                target_folder = s3_folder
                break

        local_path = f"/content/{file}"

        # Define S3 key for raw CSV upload
        raw_s3_key = f"{target_folder}{file}"

        # Extract the table name part from target_folder to append to bronze path
        table_folder = target_folder.replace('raw/olist/', '')
        bronze_parquet_path = f"s3a://{bucket_name}/warehouse/bronze/{table_folder}"

        # Upload CSV to raw path using boto3
        print(f"Uploading original CSV to s3://{bucket_name}/{raw_s3_key}...")
        s3.upload_file(local_path, bucket_name, raw_s3_key)

        # Read local CSV and Write Parquet to bronze path using Spark
        print(f"Reading {local_path} and writing Parquet to {bronze_parquet_path}...")
        df = spark.read.csv(local_path, header=True, inferSchema=True)
        df.write.mode("overwrite").parquet(bronze_parquet_path)

        print(f"Successfully processed {file}.\n")

if __name__ == "__main__":
    upload_csv_to_parquet(csv_dir, bronze_dir)