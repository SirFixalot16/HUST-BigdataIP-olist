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

def upload_csv_to_parquet(csv_dir, bronze_dir):
    for file_name in os.listdir(csv_dir):
        if file_name.endswith(".csv"):
            csv_path = os.path.join(csv_dir, file_name)
            parquet_name = file_name.replace(".csv", ".parquet")
            parquet_path = os.path.join(bronze_dir, parquet_name)

            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            df.repartition(1).write.mode("overwrite").parquet(parquet_path)

logger.info("CHUYỂN ĐỔI THÀNH CÔNG! Dữ liệu đã sẵn sàng trong thư mục /content/bronze")

if __name__ == "__main__":
    upload_csv_to_parquet(csv_dir, bronze_dir)