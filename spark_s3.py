import os
from pyspark.sql import SparkSession

# 1. Ép cứng biến môi trường Java và Python cho PySpark
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

def create_spark_session():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    spark = SparkSession.builder \
        .appName("PySpark_S3_Processing") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

if __name__ == "__main__":
    spark = create_spark_session()
    print("[*] Đã khởi tạo SparkSession với cấu hình s3a.")

    bucket_name = "local-parquet-bucket"
    input_path = f"s3a://{bucket_name}/raw_data/sample_tsfm_data.parquet"
    output_path = f"s3a://{bucket_name}/processed_data/high_accuracy_models.parquet"

    try:
        print(f"\n[*] Đang đọc dữ liệu từ: {input_path}")
        df = spark.read.parquet(input_path)
        print("--- Dữ liệu gốc ---")
        df.show()

        print("[*] Đang xử lý: Lọc các model có accuracy_score > 0.90...")
        processed_df = df.filter(df.accuracy_score > 0.90)
        print("--- Dữ liệu sau xử lý ---")
        processed_df.show()

        print(f"[*] Đang ghi dữ liệu phân tán xuống: {output_path}")
        processed_df.write.mode("overwrite").parquet(output_path)
        print("[*] Hoàn tất quá trình ghi.")

    except Exception as e:
        print(f"Lỗi trong quá trình xử lý Spark: {e}")
    finally:
        spark.stop()