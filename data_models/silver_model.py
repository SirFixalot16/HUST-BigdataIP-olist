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
        "spark.sql.catalog.olist",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.olist.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.olist.warehouse",
        "s3a://olist-brazillian-ecommerce-bigdata/warehouse/"
    )
    .getOrCreate()
)


print("Starting to create database and tables for the silver layer")

spark.sql("""
CREATE DATABASE IF NOT EXISTS olist.silver
""")


spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.customers(
    customer_id                STRING,
    customer_unique_id         STRING,
    customer_zip_code_prefix   INT,
    customer_city              STRING,
    customer_state             STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.orders(
    order_id                        STRING,
    customer_id                     STRING,
    order_status                    STRING,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date     TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.order_items(
    order_id              STRING,
    order_item_id         INT,
    product_id            STRING,
    seller_id             STRING,
    shipping_limit_date   TIMESTAMP,
    price                 FLOAT,
    freight_value         FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.order_payments(
    order_id               STRING,
    payment_sequential     INT,
    payment_type           STRING,
    payment_installments   INT,
    payment_value          FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.products(
    product_id                   STRING,
    product_category_name        STRING,
    product_name_lenght          INT,
    product_description_lenght   INT,
    product_photos_qty           INT,
    product_weight_g             FLOAT,
    product_length_cm            FLOAT,
    product_height_cm           FLOAT,
    product_width_cm            FLOAT
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.order_reviews(
    review_id                 STRING,
    order_id                  STRING,
    review_score              INT,
    review_comment_title      STRING,
    review_comment_message    STRING,
    review_creation_date      TIMESTAMP,
    review_answer_timestamp   TIMESTAMP
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.sellers(
    seller_id                STRING,
    seller_zip_code_prefix   INT,
    seller_city              STRING,
    seller_state             STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS olist.silver.geolocation(
    geolocation_zip_code_prefix   INT,
    geolocation_lat               FLOAT,
    geolocation_lng               FLOAT,
    geolocation_city              STRING,
    geolocation_state             STRING
)
USING iceberg
""")

print("Complete creating database and tables for the silver layer")
spark.stop()
