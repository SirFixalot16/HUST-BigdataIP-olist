import boto3
import json
from datetime import datetime, timedelta, timezone

with open('olist_guest.json', 'r') as f:
    config = json.load(f)

s3 = boto3.client('s3', 
    aws_access_key_id=config["access_key"], 
    aws_secret_access_key=config["secret_key"],
    region_name='ap-southeast-2'
)

BUCKET = config["bucket"]
one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

print(f"--- Đang quét các tệp mới tạo trong 1 giờ qua trên bucket: {BUCKET} ---")
try:
    response = s3.list_objects_v2(Bucket=BUCKET)
    found = False
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['LastModified'] > one_hour_ago:
                print(f"MỚI: {obj['Key']} ({obj['Size']} bytes) - Lúc: {obj['LastModified']}")
                found = True
    
    if not found:
        print("Không tìm thấy tệp nào mới được tải lên.")
except Exception as e:
    print(f"Lỗi truy cập: {e}")