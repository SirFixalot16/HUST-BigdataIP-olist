from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = (
    SparkSession.builder
    .appName("medallion-lakehouse")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.catalog.demo",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.demo.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.demo.warehouse",
        "s3a://big-data-didp-bucket/warehouse/"
    )
    .getOrCreate()
)

print("Starting to create database and tables for the GOLD layer")

spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.gold
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_sellers (
    seller_key              STRING,
    seller_id               STRING,
    seller_zip_code_prefix  INT,
    seller_city             STRING,
    seller_state            STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_products (
    product_key             STRING,
    product_id              STRING,
    category_name_english   STRING,
    product_weight_g        FLOAT,
    product_length_cm       FLOAT,
    product_height_cm       FLOAT,
    product_width_cm        FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_customers (
    customer_key                STRING,
    customer_unique_id          STRING,
    customer_zip_code_prefix    INT,
    customer_city               STRING,
    customer_state              STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.dim_date (
    date_key    STRING,
    full_date   TIMESTAMP,
    day_of_week INT,
    is_weekend  BOOLEAN,
    month       INT,
    quarter     INT,
    year        INT,
    is_holiday  BOOLEAN
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.fact_sales (
    sale_key                STRING,
    customer_key            STRING,
    product_key             STRING,
    seller_key              STRING,
    date_key                STRING,
    price                   FLOAT,
    freight_value           FLOAT,
    payment_value           FLOAT,
    payment_installments    INT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.fact_reviews (
    review_key      STRING,
    customer_key    STRING,
    product_key     STRING,
    seller_key      STRING,
    date_key        STRING,
    review_score    INT,
    comment_length  INT,
    response_time   INT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.gold.fact_shipping (
    shipping_key                STRING,
    customer_key                STRING,
    product_key                 STRING,
    seller_key                  STRING,
    date_key                    STRING,
    delivery_lead_time          INT,
    estimated_delivery_error    INT,
    shipping_distance_km        FLOAT,
    is_late                     BOOLEAN
)
USING iceberg
""")

print("Complete creating database and tables for the silver layer")
spark.stop()
