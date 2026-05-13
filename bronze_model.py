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
    # Add necessary packages
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2", # Iceberg runtime
            "org.apache.hadoop:hadoop-aws:3.3.4",                      # AWS Hadoop to connect to S3
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"               # AWS SDK to authenticate and interact with S3            
        ])
    )
    # S3 Authentication
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") # S3 service endpoint
    # Enable Iceberg Spark SQL extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    # Enable Iceberg Spark SQL extensions
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # Specify the S3A filesystem implementation

    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Configure a Spark catalog named "demo" using Iceberg
    .config(
        "spark.sql.catalog.demo",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    # Use Hadoop-based catalog for Iceberg metadata management
    .config(
        "spark.sql.catalog.demo.type",
        "hadoop"
    )
    # Define the warehouse location in S3 for Iceberg tables
    .config(
        "spark.sql.catalog.demo.warehouse",
        "s3a://big-data-didp-bucket/warehouse/"
    )
    .getOrCreate()
)

print("Starting to create database and tables for the bronze layer")

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS demo.bronze
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_customers_dataset (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix INT,
    customer_city STRING,
    customer_state STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_orders_dataset (
    order_id                        STRING,
    customer_id                     STRING,
    order_status                    STRING,
    order_purchase_timestamp        STRING,
    order_approved_at               STRING,
    order_delivered_carrier_date    STRING,
    order_delivered_customer_date   STRING,
    order_estimated_delivery_date     STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_order_items_dataset (
    order_id              STRING,
    order_item_id         INT,
    product_id            STRING,
    seller_id             STRING,
    shipping_limit_date   STRING,
    price                 FLOAT,
    freight_value         FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_order_payments_dataset (
    order_id               STRING,
    payment_sequential     INT,
    payment_type           STRING,
    payment_installments   INT,
    payment_value          FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_products_dataset (
    product_id                   STRING,
    product_category_name        STRING,
    product_name_lenght          FLOAT,
    product_description_lenght   FLOAT,
    product_photos_qty           FLOAT,
    product_weight_g             FLOAT,
    product_length_cm            FLOAT,
    product_height_cm           FLOAT,
    product_width_cm            FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_order_reviews_dataset (
    review_id                 STRING,
    order_id                  STRING,
    review_score              INT,
    review_comment_title      STRING,
    review_comment_message    STRING,
    review_creation_date      STRING,
    review_answer_timestamp   STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_sellers_dataset (
    seller_id                STRING,
    seller_zip_code_prefix   INT,
    seller_city              STRING,
    seller_state             STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.olist_geolocation_dataset (
    geolocation_zip_code_prefix   STRING,
    geolocation_lat               FLOAT,
    geolocation_lng               FLOAT,
    geolocation_city              STRING,
    geolocation_state             STRING
)
USING iceberg
""")

print("Complete creating database and tables for the bronze layer")

spark.stop()