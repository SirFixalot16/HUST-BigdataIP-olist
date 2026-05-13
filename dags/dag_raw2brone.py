from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os

# Thiết lập đường dẫn cấu hình
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
CONFIG_PATH = os.path.join(AIRFLOW_HOME, 'olist_guest.json')

def get_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

def check_raw_file_exists(**context):
    """Sử dụng boto3 để kiểm tra file trước khi khởi động Spark"""
    import boto3
    config = get_config()
    s3 = boto3.client(
        's3',
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name=config.get("region", "ap-southeast-2")
    )
    
    # Đường dẫn file từ kết quả scan của Bạn
    S3_KEY = "raw/olist/customers/olist_customers_dataset.csv"
    
    try:
        s3.head_object(Bucket=config["bucket"], Key=S3_KEY)
        print(f"Xác nhận: Tệp {S3_KEY} đã sẵn sàng.")
    except Exception as e:
        raise FileNotFoundError(f"Lỗi: Không tìm thấy tệp đầu vào trên S3. {str(e)}")

def raw_to_silver_spark(**context):
    """Sử dụng PySpark xử lý Ingestion và Phân vùng"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    config = get_config()
    
    # Lấy thông tin thời gian chạy của Airflow (Interval Ingestion)
    logical_date = context['data_interval_start']
    year = logical_date.format("YYYY")
    month = logical_date.format("MM")
    day = logical_date.format("DD")

    spark = (SparkSession.builder
        .appName("Olist_Raw_to_Silver")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate())

    # Cấu hình Hadoop để kết nối S3 thông qua S3A
    h_conf = spark._jsc.hadoopConfiguration()
    h_conf.set("fs.s3a.access.key", config["access_key"])
    h_conf.set("fs.s3a.secret.key", config["secret_key"])
    h_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    h_conf.set("fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")

    # Đọc dữ liệu CSV
    input_path = f"s3a://{config['bucket']}/raw/olist/customers/olist_customers_dataset.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Thêm cột metadata để phân vùng (Divide data by year/month/day)
    df = df.withColumn("ingest_year", F.lit(year)) \
           .withColumn("ingest_month", F.lit(month)) \
           .withColumn("ingest_day", F.lit(day))

    # Ghi dữ liệu vào layer Silver (Vì nhóm chỉ cho phép Write ở đây)
    output_path = f"s3a://{config['bucket']}/silver/customers/"
    
    (df.write
        .mode("overwrite")
        .partitionBy("ingest_year", "ingest_month", "ingest_day")
        .parquet(output_path))
    
    print(f"Hoàn thành Ingestion phân vùng: {year}/{month}/{day}")
    spark.stop()

with DAG(
    dag_id="olist_raw_to_silver_ingestion",
    start_date=datetime(2026, 5, 1),
    schedule="@daily", # Airflow scheduling
    catchup=False,
    tags=['olist', 'silver', 'spark']
) as dag:

    task_check = PythonOperator(
        task_id="check_s3_file",
        python_callable=check_raw_file_exists
    )

    task_spark = PythonOperator(
        task_id="spark_process_ingestion",
        python_callable=raw_to_silver_spark
    )

    task_check >> task_spark