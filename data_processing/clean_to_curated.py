import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, md5, concat_ws, to_date, dayofweek, month, quarter, year, sum as spark_sum,
    translate, lower, regexp_replace, trim, length, datediff, unix_timestamp,
    radians, cos, sin, asin, sqrt, pow, avg as spark_avg, lit, count as spark_count, max as spark_max, date_format, when
)
from ultility import calc_haversine

# Thiết lập hệ thống Logging chuẩn Doanh nghiệp
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Medallion_Pipeline")



logger.info("Đang khởi tạo Spark Session với Apache Iceberg...")
spark = SparkSession.builder \
    .appName("Olist_Data_Warehouse") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local_catalog.type", "hadoop") \
    .config("spark.sql.catalog.local_catalog.warehouse", "/content/lakehouse/") \
    .getOrCreate()

catalog = "local_catalog"
logger.info(" Spark Iceberg đã sẵn sàng!")

def process_gold_dimensions():
    logger.info(" BẮT ĐẦU TẠO DIMENSION TABLES (GOLD)")

    # 1. BẢNG DIM CUSTOMERS
    spark.table(f"{catalog}.silver.customers").select("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state") \
         .dropDuplicates(["customer_unique_id"]).withColumn("customer_key", md5(col("customer_unique_id"))) \
         .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.dim_customers")

    # 3. DIM PRODUCTS (Đã có bản dịch tiếng Anh)
    df_trans = spark.read.parquet("/content/bronze/product_category_name_translation.parquet")

    spark.table(f"{catalog}.silver.products") \
         .join(df_trans, "product_category_name", "left") \
         .select("product_id",
                 col("product_category_name_english").alias("category_name_english"),
                 "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm") \
         .dropDuplicates(["product_id"]) \
         .withColumn("product_key", md5(col("product_id"))) \
         .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.dim_products")

    # 2. DIM SELLERS
    spark.table(f"{catalog}.silver.sellers") \
         .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state") \
         .dropDuplicates(["seller_id"]) \
         .withColumn("seller_key", md5(col("seller_id"))) \
         .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.dim_sellers")

    # 4. BẢNG DIM DATE
    df_orders = spark.table(f"{catalog}.silver.orders")
    df_orders.select(to_date(col("order_purchase_timestamp")).alias("full_date")) \
             .dropDuplicates().filter(col("full_date").isNotNull()) \
             .withColumn("date_key", md5(col("full_date").cast("string"))) \
             .withColumn("formatted_date", date_format(col("full_date"), "yyyy-MM-dd HH:mm:ss")) \
             .withColumn("day_of_week", dayofweek(col("full_date"))) \
             .withColumn("month", month(col("full_date"))) \
             .withColumn("quarter", quarter(col("full_date"))) \
             .withColumn("year", year(col("full_date"))) \
             .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False)) \
             .withColumn("is_holiday", lit(False)) \
             .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.dim_date")

    logger.info(" Đã tạo xong 4 bảng Dimensions (Khớp ERD)!")
    
def process_gold_facts():
    logger.info(" BẮT ĐẦU TẠO FACT TABLES (GOLD)")

    df_orders = spark.table(f"{catalog}.silver.orders")
    df_items = spark.table(f"{catalog}.silver.order_items")
    df_customers = spark.table(f"{catalog}.silver.customers")
    df_sellers = spark.table(f"{catalog}.silver.sellers")
    df_reviews = spark.table(f"{catalog}.silver.order_reviews")
    df_geo = spark.table(f"{catalog}.silver.geolocation")

    # SỬA Ở ĐÂY: Thêm cột is_voucher để sau này tính Tỷ lệ xài mã giảm giá
    df_payments = spark.table(f"{catalog}.silver.order_payments").groupBy("order_id").agg(
        spark_sum("payment_value").alias("payment_value"),
        spark_max("payment_installments").alias("payment_installments"),
        spark_max(when(col("payment_type") == "voucher", 1).otherwise(0)).alias("is_voucher")
    )

    df_base = df_items.join(df_orders, "order_id", "inner").join(df_customers, "customer_id", "inner")

    # FACT SALES (Đã có order_status để tính Hủy đơn, và is_voucher để tính Mã giảm giá)
    df_base.join(df_payments, "order_id", "left") \
        .withColumn("sale_key", md5(concat_ws("_", col("order_id"), col("order_item_id")))) \
        .withColumn("customer_key", md5(col("customer_unique_id"))) \
        .withColumn("product_key", md5(col("product_id"))) \
        .withColumn("seller_key", md5(col("seller_id"))) \
        .withColumn("date_key", md5(to_date(col("order_purchase_timestamp")).cast("string"))) \
        .select("sale_key", "customer_key", "product_key", "seller_key", "date_key",
                "order_id", "order_status", "price", "freight_value", "payment_value", "payment_installments", "is_voucher") \
        .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.fact_sales")

    # FACT REVIEWS
    df_reviews.join(df_base.dropDuplicates(["order_id"]), "order_id", "inner") \
        .withColumn("review_key", md5(col("review_id"))) \
        .withColumn("customer_key", md5(col("customer_unique_id"))) \
        .withColumn("product_key", md5(col("product_id"))) \
        .withColumn("seller_key", md5(col("seller_id"))) \
        .withColumn("date_key", md5(to_date(col("order_purchase_timestamp")).cast("string"))) \
        .withColumn("comment_length", length(col("review_comment_message"))) \
        .withColumn("response_time_hours", (unix_timestamp("review_answer_timestamp") - unix_timestamp("review_creation_date")) / 3600) \
        .select("review_key", "customer_key", "product_key", "seller_key", "date_key", "review_score", "comment_length", "response_time_hours") \
        .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.fact_reviews")

    # FACT SHIPPING
    df_geo_cust = df_geo.withColumnRenamed("geolocation_zip_code_prefix", "cust_zip").withColumnRenamed("geo_lat", "cust_lat").withColumnRenamed("geo_lng", "cust_lng")
    df_geo_sell = df_geo.withColumnRenamed("geolocation_zip_code_prefix", "sell_zip").withColumnRenamed("geo_lat", "sell_lat").withColumnRenamed("geo_lng", "sell_lng")

    df_base.join(df_sellers, "seller_id", "inner") \
        .join(df_geo_cust, col("customer_zip_code_prefix") == col("cust_zip"), "left") \
        .join(df_geo_sell, col("seller_zip_code_prefix") == col("sell_zip"), "left") \
        .withColumn("shipping_key", md5(concat_ws("_", col("order_id"), col("order_item_id")))) \
        .withColumn("customer_key", md5(col("customer_unique_id"))) \
        .withColumn("product_key", md5(col("product_id"))) \
        .withColumn("seller_key", md5(col("seller_id"))) \
        .withColumn("date_key", md5(to_date(col("order_purchase_timestamp")).cast("string"))) \
        .withColumn("delivery_lead_time", datediff("order_delivered_customer_date", "order_purchase_timestamp")) \
        .withColumn("estimated_delivery_error", datediff("order_estimated_delivery_date", "order_delivered_customer_date")) \
        .withColumn("is_late", col("order_delivered_customer_date") > col("order_estimated_delivery_date")) \
        .withColumn("shipping_distance_km", calc_haversine("cust_lat", "cust_lng", "sell_lat", "sell_lng")) \
        .select("shipping_key", "customer_key", "product_key", "seller_key", "date_key", "delivery_lead_time", "estimated_delivery_error", "shipping_distance_km", "is_late") \
        .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.gold.fact_shipment")

    logger.info(" Hoàn tất 3 bảng Fact Sự kiện!")
    
if __name__ == "__main__":
    process_gold_dimensions()
    process_gold_facts()