import boto3
import json

with open('olist_guest.json', 'r') as f:
    config = json.load(f)

s3 = boto3.client('s3', 
    aws_access_key_id=config["access_key"], 
    aws_secret_access_key=config["secret_key"]
)

# Kiểm tra thư mục bronze/orders
PREFIX = "silver/orders/"
response = s3.list_objects_v2(Bucket=config["bucket"], Prefix=PREFIX)

if 'Contents' in response:
    print(f"Kiểm tra cấu hình phân vùng trong {PREFIX}:")
    # In ra 10 file đầu tiên để kiểm tra cấu trúc thư mục
    for obj in response['Contents'][:10]:
        print(f" - {obj['Key']}")
else:
    print("Chưa tìm thấy dữ liệu trong Bronze.")