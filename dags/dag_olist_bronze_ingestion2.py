from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import logging

# Thiết lập môi trường
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
CONFIG_PATH = os.path.join(AIRFLOW_HOME, 'olist_guest.json')

def get_s3_credentials():
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    return {
        "key": config["access_key"],
        "secret": config["secret_key"],
        "client_kwargs": {"region_name": "ap-southeast-2"}
    }, config["bucket"]

def process_interval_ingestion(**context):
    """
    Thực hiện trích xuất dữ liệu theo ngày và nạp vào S3
    """
    s3_opts, bucket = get_s3_credentials()
    
    # 1. Lấy ngày logic của Airflow (ds = YYYY-MM-DD)
    target_date_str = context['ds'] 
    target_date = pd.to_datetime(target_date_str).date()
    logging.info(f"--- Bắt đầu Interval Ingestion cho ngày: {target_date_str} ---")
    
    # 2. Đường dẫn tệp Raw đã xác minh
    input_uri = f"s3://{bucket}/raw/olist/orders/olist_orders_dataset.csv"
    
    try:
        # Đọc dữ liệu từ S3
        df = pd.read_csv(input_uri, storage_options=s3_opts)
        logging.info(f"Đọc thành công file Raw. Tổng quy mô: {len(df)} dòng.")
        
        # 3. Chuyển đổi và Lọc dữ liệu theo ngày (Divide by time)
        df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
        df_daily = df[df['order_purchase_timestamp'].dt.date == target_date].copy()
        
        if df_daily.empty:
            logging.info(f"Không có dữ liệu phát sinh trong ngày {target_date_str}. Bỏ qua ghi file.")
            return

        # 4. Định nghĩa cấu trúc phân vùng đầu ra
        year, month, day = target_date.year, f"{target_date.month:02d}", f"{target_date.day:02d}"
        output_uri = f"s3://{bucket}/bronze/orders/year={year}/month={month}/day={day}/orders_{target_date_str}.parquet"
        
        logging.info(f"Đang tiến hành phân vùng và ghi {len(df_daily)} dòng vào S3...")
        
        # Ghi file Parquet (nén snappy mặc định)
        df_daily.to_parquet(
            output_uri, 
            index=False, 
            storage_options=s3_opts, 
            engine='pyarrow'
        )
        logging.info(f"Hoàn thành. Tệp lưu tại: {output_uri}")
        
    except Exception as e:
        logging.error(f"Lỗi hệ thống: {str(e)}")
        raise

# Cấu hình DAG
default_args = {
    'owner': 'binh_nguyen',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="olist_orders_ingestion_manager",
    default_args=default_args,
    start_date=datetime(2018, 1, 1), # Bạn có thể chỉnh lại ngày bắt đầu tùy ý
    end_date=datetime(2018, 1, 10),  # Chạy thử nghiệm 10 ngày để kiểm tra
    schedule="@daily",
    catchup=True,                    # Kích hoạt chế độ chạy bù dữ liệu quá khứ
    tags=['olist', 'production', 'ingestion']
) as dag:

    ingest_task = PythonOperator(
        task_id="daily_interval_ingestion",
        python_callable=process_interval_ingestion
    )