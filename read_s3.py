import boto3
import pandas as pd
import io

# Khởi tạo client kết nối đến MinIO (S3 Local)
def get_s3_client():
    return boto3.client(
        service_name='s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )

# Kịch bản 1: Tải file từ S3 về Local rồi đọc
def download_and_read(s3_client, bucket, key, download_path):
    print("--- Kịch bản 1: Tải file về Local ---")
    s3_client.download_file(Bucket=bucket, Key=key, Filename=download_path)
    print(f"[*] Đã tải file thành công về: {download_path}")
    
    df = pd.read_parquet(download_path)
    print(df.to_string(index=False))

# Kịch bản 2: Đọc trực tiếp dữ liệu từ S3 vào bộ nhớ (In-memory)
def read_direct_from_s3(s3_client, bucket, key):
    print("\n--- Kịch bản 2: Đọc trực tiếp từ S3 (In-memory) ---")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    parquet_content = response['Body'].read()
    
    # Sử dụng io.BytesIO để chuyển đổi byte stream thành file-like object cho Pandas
    df = pd.read_parquet(io.BytesIO(parquet_content))
    print(df.to_string(index=False))

if __name__ == "__main__":
    bucket_name = 'local-parquet-bucket'
    s3_object_key = 'raw_data/sample_tsfm_data.parquet'
    download_path = 'downloaded_tsfm_data.parquet'

    client = get_s3_client()
    
    download_and_read(client, bucket_name, s3_object_key, download_path)
    read_direct_from_s3(client, bucket_name, s3_object_key)