import boto3
import json

with open('olist_guest.json', 'r') as f:
    config = json.load(f)

s3 = boto3.client('s3', 
    aws_access_key_id=config["access_key"], 
    aws_secret_access_key=config["secret_key"]
)

# Quét tất cả các tệp có chứa chữ "order"
response = s3.list_objects_v2(Bucket=config["bucket"], Prefix="raw/olist/")
if 'Contents' in response:
    print("--- Các tệp liên quan đến Order trên S3 ---")
    for obj in response['Contents']:
        if "order" in obj['Key']:
            print(f"Đường dẫn chuẩn: s3://{config['bucket']}/{obj['Key']}")