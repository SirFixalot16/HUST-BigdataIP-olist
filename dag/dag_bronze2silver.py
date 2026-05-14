import os

from pyspark.sql.functions import (
    col, translate, lower, regexp_replace, trim, coalesce, avg as spark_avg, first
)
from pyspark.sql import SparkSession

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json

CONFIG = 'olist_guest.json'
with open(CONFIG, 'r') as file:
    data = json.load(file)
BUCKET = data["bucket"]

def clean_city_name(col_name):
    c = lower(col(col_name))
    c = translate(c, "áàãâäéèêëíìîïóòõôöúùûüç", "aaaaaeeeeiiiiooooouuuuc")
    return trim(regexp_replace(c, "[0-9]", ""))

def fix_product_name(df, spark):
    translation_df = spark.read.parquet(f"s3a://{BUCKET}/bronze/product_category_name_translation/")

    # Join and fallback to original Portuguese name if English translation is missing
    return df.join(translation_df, on="product_category_name", how="left") \
             .withColumn("category_mapped", coalesce(col("product_category_name_english"), col("product_category_name"))) \
             .drop("product_category_name", "product_category_name_english") \
             .withColumnRenamed("category_mapped", "product_category_name")

def process_bronze_customers(spark, catalog):
    df_cust_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/customers/")
    df_cust = df_cust_raw.dropDuplicates(["customer_id"]).filter(col("customer_id").isNotNull()) \
                         .withColumn("customer_city", clean_city_name("customer_city")).filter(col("customer_city") != "")
    df_cust.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.customers")

def process_bronze_orders(spark, catalog):

    df_orders_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/orders/")

    df_orders = df_orders_raw

    for date_col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"]:
        df_orders = df_orders.withColumn(
        date_col,
        col(date_col).cast("timestamp")
        )

    df_orders = df_orders.filter(
        # 1. Mua hàng <= Duyệt đơn
        ((col("order_purchase_timestamp") <= col("order_approved_at")) | col("order_approved_at").isNull()) &
        # 2. Duyệt đơn <= Giao kho
        ((col("order_approved_at") <= col("order_delivered_carrier_date")) | col("order_delivered_carrier_date").isNull()) &
        # 3. Giao kho <= Khách nhận
        ((col("order_delivered_carrier_date") <= col("order_delivered_customer_date")) | col("order_delivered_customer_date").isNull())
    ).dropDuplicates(["order_id"]).filter(col("order_id").isNotNull())
    
    df_orders.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.orders")

def process_bronze_products(spark, catalog):
    df_prod_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/products/")
    df_prod = df_prod_raw.withColumnRenamed("product_name_lenght", "product_name_length") \
        .withColumnRenamed("product_description_lenght", "product_description_length") \
        .withColumn("product_name_length", col("product_name_length").cast("int")) \
        .withColumn("product_description_length", col("product_description_length").cast("int")) \
        .withColumn("product_photos_qty", col("product_photos_qty").cast("int")) \
        .dropDuplicates(["product_id"]) \
        .filter((col("product_weight_g") >= 0) | col("product_weight_g").isNull()) \
        .filter((col("product_length_cm") >= 0) | col("product_length_cm").isNull()) \
        .filter((col("product_height_cm") >= 0) | col("product_height_cm").isNull()) \
        .filter((col("product_width_cm") >= 0) | col("product_width_cm").isNull()) \
        .filter((col("product_name_length") >= 0) | col("product_name_length").isNull()) \
        .filter((col("product_description_length") >= 0) | col("product_description_length").isNull()) \
        .filter((col("product_photos_qty") >= 0) | col("product_photos_qty").isNull())
    df_prod = fix_product_name(df_prod, spark)
    
    df_prod.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.products")

def process_bronze_sellers(spark, catalog):
    df_sell_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/sellers/")

    df_sell = df_sell_raw.dropDuplicates(["seller_id"]).withColumn("seller_city", clean_city_name("seller_city"))
    df_sell.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.sellers")

def process_bronze_order_items(spark, catalog):
    df_items_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/order_items/")

    df_items = df_items_raw.withColumn("shipping_limit_date", col("shipping_limit_date").cast("timestamp")) \
        .filter((col("price") >= 0) & (col("freight_value") >= 0))

    df_items.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.order_items")

    
def process_bronze_order_reviews(spark, catalog):
    df_rev_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/order_reviews/")

    df_rev = df_rev_raw.withColumn("review_creation_date", col("review_creation_date").cast("timestamp")) \
        .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast("timestamp")) \
        .filter((col("review_score") >= 1) & (col("review_score") <= 5))

    df_rev.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.order_reviews")


def process_bronze_payments(spark, catalog):
    df_pay_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/order_payments/")

    df_pay = df_pay_raw.dropDuplicates()
    df_pay.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.order_payments")

# def process_bronze_geolocation(spark, catalog):
#     df_geo_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/geolocation/")
#     df_geo_raw.filter((col("geolocation_lat") >= -90) & (col("geolocation_lat") <= 90)) \
#         .filter((col("geolocation_lng") >= -180) & (col("geolocation_lng") <= 180)) \
#         .groupBy("geolocation_zip_code_prefix").agg(spark_avg("geolocation_lat").alias("geo_lat"), spark_avg("geolocation_lng").alias("geo_lng")) \
#         .withColumn("geolocation_city", clean_city_name("geolocation_city")).filter(col("geolocation_city") != "") \
#         .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.geolocation")

def process_bronze_geolocation(spark, catalog):
    df_geo_raw = spark.read.parquet(f"s3a://{BUCKET}/bronze/geolocation/")

    df_geo = (
        df_geo_raw
        .filter((col("geolocation_lat") >= -90) & (col("geolocation_lat") <= 90))
        .filter((col("geolocation_lng") >= -180) & (col("geolocation_lng") <= 180))
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            spark_avg("geolocation_lat").alias("geo_lat"),
            spark_avg("geolocation_lng").alias("geo_lng"),
            first("geolocation_city").alias("geolocation_city"),
            first("geolocation_state").alias("geolocation_state")
        )
        .withColumn(
            "geolocation_city",
            clean_city_name("geolocation_city")
        )
        .filter(col("geolocation_city") != "")
    )

    df_geo.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.silver.geolocation")

def bronze_to_silver():
    spark = (
            SparkSession.builder
            .appName("bronze_to_silver")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
            )
            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
            .config("spark.sql.catalog.olist","org.apache.iceberg.spark.SparkCatalog") 
            .config("spark.sql.catalog.olist.type", "hadoop") 
            .config("spark.sql.catalog.olist.warehouse", f"s3a://{BUCKET}/") 
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .getOrCreate() 
            )

    # hadoop_conf = spark._jsc.hadoopConfiguration()
    # hadoop_conf.set(
    #     "fs.s3a.aws.credentials.provider",
    #     "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    # )
    # hadoop_conf.set(
    #     "fs.s3a.impl",
    #     "org.apache.hadoop.fs.s3a.S3AFileSystem"
    # )
    # hadoop_conf.set(
    #     "fs.s3a.endpoint",
    #     "s3.ap-southeast-2.amazonaws.com"
    # )

    catalog = "olist"

    process_bronze_geolocation(spark=spark, catalog=catalog)
    process_bronze_products(spark=spark, catalog=catalog)
    process_bronze_customers(spark=spark, catalog=catalog)
    process_bronze_sellers(spark=spark, catalog=catalog)
    process_bronze_orders(spark=spark, catalog=catalog)
    process_bronze_order_items(spark=spark, catalog=catalog)
    process_bronze_order_reviews(spark=spark, catalog=catalog)
    process_bronze_payments(spark=spark, catalog=catalog)

    print("Conversion successful")

    spark.stop()

with DAG(
    dag_id="bronze_to_silver_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    convert_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver
    )