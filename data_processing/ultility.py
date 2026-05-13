import os
from airflow.models.connection import Connection
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql.functions import (
    col, translate, lower, regexp_replace, trim, coalesce,
    radians, cos, sin, asin, sqrt, pow, avg as spark_avg, lit, count as spark_count, max as spark_max, date_format, when
)

load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

def get_s3_client():
   # Sử dụng endpoint_url để trỏ Boto3
   s3_client = boto3.client(
       service_name='s3',
       aws_access_key_id=ACCESS_KEY,
       aws_secret_access_key=SECRET_KEY,
       region_name='us-east-1'
   )
   return s3_client

# Upload file lên S3
def upload_to_s3(s3_client, local_file, bucket, s3_key):
   try:
       s3_client.upload_file(Filename=local_file, Bucket=bucket, Key=s3_key)
       print(f"Upload thành công lên s3://{bucket}/{s3_key}")
   except ClientError as e:
       print(f"Lỗi trong quá trình upload: {e}")

# Tải file từ S3 về Local rồi đọc
def download_and_read(s3_client, bucket, key, download_path):
   print("--- Tải file về Local ---")
   s3_client.download_file(Bucket=bucket, Key=key, Filename=download_path)
   print(f"[*] Đã tải file thành công về: {download_path}")
  
   df = pd.read_parquet(download_path)
   return df

# Hàm tính khoảng cách Haversine (km)
def calc_haversine(lat1, lon1, lat2, lon2):
    lat1_r, lon1_r = radians(lat1), radians(lon1)
    lat2_r, lon2_r = radians(lat2), radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = pow(sin(dlat/2), 2) + cos(lat1_r) * cos(lat2_r) * pow(sin(dlon/2), 2)
    return 2 * 6371.0 * asin(sqrt(a))

# Hàm chuẩn hóa tên thành phố
def clean_city_name(col_name):
    c = lower(col(col_name))
    c = translate(c, "áàãâäéèêëíìîïóòõôöúùûüç", "aaaaaeeeeiiiiooooouuuuc")
    return trim(regexp_replace(c, "[0-9]", ""))

def fix_product_name(df, spark):
    translation_df = spark.read.csv("s3a://olist-brazillian-ecommerce-bigdata/raw/olist/product_category_name_translation/", header=True, inferSchema=True)

    # Join and fallback to original Portuguese name if English translation is missing
    return df.join(translation_df, on="product_category_name", how="left") \
             .withColumn("category_mapped", coalesce(col("product_category_name_english"), col("product_category_name"))) \
             .drop("product_category_name", "product_category_name_english") \
             .withColumnRenamed("category_mapped", "product_category_name")