import boto3
import pandas as pd
from botocore.exceptions import ClientError
import os

# 1. Tạo dữ liệu mẫu định dạng Parquet tại local
def create_sample_parquet(file_path):
    data = {
        'transaction_id': [101, 102, 103, 104],
        'model_name': ['TSFM-Mamba', 'TSFM-Transformer', 'VisionTS++', 'Chronos'],
        'accuracy_score': [0.95, 0.92, 0.96, 0.89]
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_path, engine='pyarrow')
    print(f"Đã tạo file local: {file_path}")

# 2. Khởi tạo client Boto3 kết nối đến S3 (MinIO Local)
def get_s3_client():
    # Sử dụng endpoint_url để trỏ Boto3 về MinIO thay vì AWS thật
    s3_client = boto3.client(
        service_name='s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
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

if __name__ == "__main__":
    local_file_path = 'sample_tsfm_data.parquet'
    bucket_name = 'local-parquet-bucket' # Chú ý: Đổi tên này nếu bucket của bạn khác
    s3_object_key = 'raw_data/sample_tsfm_data.parquet'

    # Thực thi tuần tự
    create_sample_parquet(local_file_path)
    s3_client = get_s3_client()
    upload_to_s3(s3_client, local_file_path, bucket_name, s3_object_key)