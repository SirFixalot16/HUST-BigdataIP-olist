import os
from airflow.models.connection import Connection
import boto3
from botocore.exceptions import ClientError
import pandas as pd

def get_s3_client():
   # Sử dụng endpoint_url để trỏ Boto3
   s3_client = boto3.client(
       service_name='s3',
       endpoint_url='',
       aws_access_key_id='',
       aws_secret_access_key='',
       region_name='us-east-1'
   )
   return s3_client


# 3. Upload file lên S3
def upload_to_s3(s3_client, local_file, bucket, s3_key):
   try:
       s3_client.upload_file(Filename=local_file, Bucket=bucket, Key=s3_key)
       print(f"Upload thành công lên s3://{bucket}/{s3_key}")
   except ClientError as e:
       print(f"Lỗi trong quá trình upload: {e}")

#: Tải file từ S3 về Local rồi đọc
def download_and_read(s3_client, bucket, key, download_path):
   print("--- Tải file về Local ---")
   s3_client.download_file(Bucket=bucket, Key=key, Filename=download_path)
   print(f"[*] Đã tải file thành công về: {download_path}")
  
   df = pd.read_parquet(download_path)
   print(df.to_string(index=False))
