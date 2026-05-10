import os
import time
from pyspark.sql import SparkSession
from performance_tester import LakehouseEvaluator

# 1. Ép cứng biến môi trường Java và Python cho PySpark
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# 2. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Lakehouse_Performance_Test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. Giả lập logic xử lý từ team Data Engineering
def mock_bronze_to_silver(spark_session):
    # Tạo một DataFrame giả lập (100,000 dòng)
    data = [{"order_id": i, "status": "delivered"} for i in range(10000)]
    df = spark_session.createDataFrame(data)
    
    # Xử lý: Lọc dữ liệu theo Rule DQ
    cleaned_df = df.filter(df.status == "delivered")
    return cleaned_df

if __name__ == "__main__":
    # Khởi tạo bộ đánh giá
    evaluator = LakehouseEvaluator()
    
    # Thực thi kiểm thử luồng số 1
    evaluator.evaluate_job(
        job_name="Bronze_to_Silver_Olist_Orders",
        spark_function=mock_bronze_to_silver,
        spark_session=spark
    )
    
    spark.stop()