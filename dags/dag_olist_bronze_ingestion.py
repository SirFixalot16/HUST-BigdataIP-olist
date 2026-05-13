from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import s3fs

# Cấu hình đường dẫn
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
CONFIG_PATH = os.path.join(AIRFLOW_HOME, 'olist_guest.json')

def get_s3_options():
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    return {
        "key": config["access_key"],
        "secret": config["secret_key"],
        "client_kwargs": {"region_name": "ap-southeast-2"}
    }, config["bucket"]

def ingest_order_interval(**context):
    """
    Function xử lý Interval Ingestion sử dụng Pandas
    """
    s3_opts, bucket = get_s3_options()
    
    # 1. Lấy ngày hiện tại mà Airflow đang xử lý (ví dụ: 2017-10-02)
    target_date_str = context['ds'] 
    target_date = pd.to_datetime(target_date_str).date()
    
    # 2. Đọc file RAW từ S3 bằng Pandas
    # Lưu ý: Với file lớn, có thể dùng chunksize, nhưng Olist CSV ~15MB nên load toàn bộ vẫn ổn
    input_uri = f"s3://{bucket}/raw/olist/orders/olist_order_dataset.csv"
    df = pd.read_csv(input_uri, storage_options=s3_opts)
    
    # 3. Chuyển đổi cột thời gian và lọc dữ liệu
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    
    # Lọc các dòng có ngày mua trùng với ngày Airflow đang chạy
    df_interval = df[df['order_purchase_timestamp'].dt.date == target_date].copy()
    
    if df_interval.empty:
        print(f"Không có dữ liệu cho ngày {target_date_str}. Bỏ qua.")
        return

    # 4. Tạo các cột phân vùng
    df_interval['year'] = df['order_purchase_timestamp'].dt.year
    df_interval['month'] = df['order_purchase_timestamp'].dt.month
    df_interval['day'] = df['order_purchase_timestamp'].dt.day

    # 5. Ghi trực tiếp vào Bronze layer dưới dạng Parquet với phân vùng
    # Đường dẫn mẫu: bronze/orders/year=2017/month=10/day=02/data.parquet
    year, month, day = target_date.year, f"{target_date.month:02d}", f"{target_date.day:02d}"
    output_uri = f"s3://{bucket}/bronze/orders/year={year}/month={month}/day={day}/orders_{target_date_str}.parquet"
    
    df_interval.to_parquet(
        output_uri,
        index=False,
        storage_options=s3_opts,
        engine='pyarrow'
    )
    print(f"Đã ingest {len(df_interval)} dòng vào {output_uri}")

# Cấu hình DAG
default_args = {
    'owner': 'binh_nguyen',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="olist_bronze_orders_ingestion",
    default_args=default_args,
    description="Interval Ingestion for Olist Orders using Pandas",
    start_date=datetime(2017, 10, 1), # Bắt đầu từ thời điểm có dữ liệu trong CSV
    end_date=datetime(2017, 10, 15),   # Chạy thử nghiệm 15 ngày
    schedule="@daily",                # Tự động ingest hàng ngày
    catchup=True,                     # Quan trọng: Chạy bù dữ liệu quá khứ
    tags=['olist', 'pandas', 'bronze']
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_daily_orders",
        python_callable=ingest_order_interval
    )