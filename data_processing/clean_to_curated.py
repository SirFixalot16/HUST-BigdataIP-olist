import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, md5, concat_ws, to_date, dayofweek, month, quarter, year, sum as spark_sum,
    coalesce, lower, regexp_replace, trim, length, datediff, unix_timestamp,
    sha2, avg as spark_avg, lit, count as spark_count, max as spark_max, date_format, when
)
from ultility import calc_haversine

# Thiết lập hệ thống Logging chuẩn Doanh nghiệp
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Medallion_Pipeline")



logger.info("Đang khởi tạo Spark Session với Apache Iceberg...")


spark = SparkSession.builder \
    .appName("OlistIceberg") \
    .config(
        "spark.jars",
        "/content/iceberg-spark-runtime-3.4_2.12-1.5.2.jar,"
        "/content/hadoop-aws-3.3.4.jar,"
        "/content/aws-java-sdk-bundle-1.12.262.jar"
    ) \
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ) \
    .config(
        "spark.sql.catalog.olist",
        "org.apache.iceberg.spark.SparkCatalog"
    ) \
    .config(
        "spark.sql.catalog.olist.type",
        "hadoop"
    ) \
    .config(
        "spark.sql.catalog.olist.warehouse",
        "s3a://olist-brazillian-ecommerce-bigdata/warehouse/"
    ) \
    .getOrCreate()

catalog = "local_catalog"
logger.info(" Spark Iceberg đã sẵn sàng!")

def process_dim_customers(spark):
    print("Processing dim_customers...")
    customers_silver = spark.table(f"{catalog}.silver.customers")
    dim_customers = customers_silver.select(
        sha2(concat_ws("||", col("customer_unique_id")), 256).alias("customer_key"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state")
    ).dropDuplicates(["customer_key"])

    dim_customers.writeTo(f"{catalog}.gold.dim_customers").using("iceberg") \
        .tableProperty("format-version", "2").createOrReplace()

def process_dim_products(spark):
    print("Processing dim_products...")
    products_silver = spark.table("olist.silver.products")
    dim_products = products_silver.select(
        sha2(concat_ws("||", col("product_id")), 256).alias("product_key"),
        col("product_id"),
        col("product_category_name").alias("category_name_english"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    ).dropDuplicates(["product_key"])

    dim_products.writeTo("olist.gold.dim_products").using("iceberg") \
        .tableProperty("format-version", "2").createOrReplace()
         
def process_dim_sellers(spark):
    print("Processing dim_sellers...")
    sellers_silver = spark.table("olist.silver.sellers")
    dim_sellers = sellers_silver.select(
        sha2(concat_ws("||", col("seller_id")), 256).alias("seller_key"),
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state")
    ).dropDuplicates(["seller_key"])

    dim_sellers.writeTo("olist.gold.dim_sellers").using("iceberg") \
        .tableProperty("format-version", "2").createOrReplace()

def process_dim_date(spark):
    print("Processing dim_date...")
    date_df = spark.sql("""
    SELECT explode(
        sequence(
            to_date('2016-01-01'),
            to_date('2019-12-31'),
            interval 1 day
        )
    ) AS full_date
    """)

    dim_date = date_df.select(
        date_format(col("full_date"), "yyyyMMdd").alias("date_key"),
        col("full_date"),
        date_format(col("full_date"), "EEEE").alias("day_of_week"),
        when(dayofweek(col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        month(col("full_date")).alias("month"),
        quarter(col("full_date")).alias("quarter"),
        year(col("full_date")).alias("year"),
        lit(False).alias("is_holiday")
    )

    dim_date.writeTo("olist.gold.dim_date").using("iceberg") \
        .tableProperty("format-version", "2").createOrReplace()

def process_fact_sales(spark):
    print("Processing fact_sales...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.gold")

    orders_silver = spark.table("olist.silver.orders")
    customers_silver = spark.table("olist.silver.customers")
    order_items_silver = spark.table("olist.silver.order_items")
    payments_silver = spark.table("olist.silver.order_payments")

    dim_customers = spark.table("olist.gold.dim_customers")
    dim_products = spark.table("olist.gold.dim_products")
    dim_sellers = spark.table("olist.gold.dim_sellers")

    # Use correct payment names (payment_installments, payment_value)
    payments_agg = payments_silver.groupBy("order_id").agg(
        sum("payment_value").alias("payment_value"),
        max("payment_installments").alias("payment_installments")
    )

    fact_sales = (
        order_items_silver.alias("oi")
        .join(orders_silver.alias("o"), "order_id")
        .join(payments_agg.alias("p_agg"), "order_id", "left")
        .join(customers_silver.alias("cs"), col("o.customer_id") == col("cs.customer_id"), "left")
        .join(dim_customers.alias("dc"), col("cs.customer_unique_id") == col("dc.customer_unique_id"), "left")
        .join(dim_products.alias("dp"), "product_id", "left")
        .join(dim_sellers.alias("ds"), "seller_id", "left")
    )

    fact_sales = fact_sales.select(
        sha2(concat_ws("||", col("order_id"), col("order_item_id")), 256).alias("sale_key"),
        col("dc.customer_key"),
        col("dp.product_key"),
        col("ds.seller_key"),
        date_format(col("o.order_purchase_timestamp"), "yyyyMMdd").alias("date_key"),
        col("price"),
        col("freight_value"),
        col("p_agg.payment_value"),
        col("p_agg.payment_installments")
    ).dropDuplicates()

    fact_sales.writeTo("olist.gold.fact_sales").using("iceberg").tableProperty("format-version", "2").createOrReplace()

    df_payments = spark.table(f"{catalog}.silver.order_payments").groupBy("order_id").agg(
        spark_sum("payment_value").alias("payment_value"),
        spark_max("payment_installments").alias("payment_installments"),
        spark_max(when(col("payment_type") == "voucher", 1).otherwise(0)).alias("is_voucher")
    )


def process_fact_reviews(spark):
    print("Processing fact_reviews...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.gold")

    reviews_silver = spark.table("olist.silver.order_reviews")
    orders_silver = spark.table("olist.silver.orders")
    order_items_silver = spark.table("olist.silver.order_items")
    customers_silver = spark.table("olist.silver.customers")

    dim_customers = spark.table("olist.gold.dim_customers")
    dim_products = spark.table("olist.gold.dim_products")
    dim_sellers = spark.table("olist.gold.dim_sellers")

    reviews_base = (
        reviews_silver.alias("r")
        .join(orders_silver.alias("o"), "order_id")
        .join(order_items_silver.alias("oi"), "order_id")
    )

    fact_reviews = (
        reviews_base
        .join(customers_silver.alias("cs"), col("o.customer_id") == col("cs.customer_id"), "left")
        .join(dim_customers.alias("dc"), col("cs.customer_unique_id") == col("dc.customer_unique_id"), "left")
        .join(dim_products.alias("dp"), "product_id", "left")
        .join(dim_sellers.alias("ds"), "seller_id", "left")
    )

    fact_reviews = fact_reviews.select(
        sha2(concat_ws("||", col("review_id"), col("product_id"), col("seller_id")), 256).cast("string").alias("review_key"),
        col("dc.customer_key").cast("string").alias("customer_key"),
        date_format(col("review_creation_date"), "yyyyMMdd").cast("string").alias("date_key"),
        col("order_id").cast("string").alias("order_id"),
        col("review_score").cast("int").alias("review_score"),
        length(coalesce(col("review_comment_message"), lit(""))).cast("int").alias("comment_length"),
        col("review_answer_timestamp").cast("timestamp").alias("response_time")
    ).dropDuplicates()

    fact_reviews.writeTo("olist.gold.fact_reviews").using("iceberg").tableProperty("format-version", "2").createOrReplace()
        
def process_fact_shipping(spark):
    print("Processing fact_shipping...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.gold")

    orders_silver = spark.table("olist.silver.orders")
    order_items_silver = spark.table("olist.silver.order_items")
    customers_silver = spark.table("olist.silver.customers")

    dim_customers = spark.table("olist.gold.dim_customers")
    dim_products = spark.table("olist.gold.dim_products")
    dim_sellers = spark.table("olist.gold.dim_sellers")

    shipping_base = orders_silver.alias("o").join(order_items_silver.alias("oi"), "order_id")

    fact_shipping = (
        shipping_base
        .join(customers_silver.alias("cs"), col("o.customer_id") == col("cs.customer_id"), "left")
        .join(dim_customers.alias("dc"), col("cs.customer_unique_id") == col("dc.customer_unique_id"), "left")
        .join(dim_products.alias("dp"), "product_id", "left")
        .join(dim_sellers.alias("ds"), "seller_id", "left")
    )

    fact_shipping = fact_shipping.select(
        sha2(concat_ws("||", col("order_id"), col("order_item_id"), col("seller_id")), 256).alias("shipping_key"),
        col("dc.customer_key"),
        col("dp.product_key"),
        col("ds.seller_key"),
        date_format(col("o.order_purchase_timestamp"), "yyyyMMdd").alias("date_key"),
        datediff(col("o.order_delivered_customer_date"), col("o.order_purchase_timestamp")).alias("delivery_lead_time"),
        datediff(col("o.order_delivered_customer_date"), col("o.order_estimated_delivery_date")).alias("estimated_delivery_error"),
        when(col("o.order_delivered_customer_date") > col("o.order_estimated_delivery_date"), True).otherwise(False).alias("is_late")
    )
    fact_shipping = fact_shipping.filter(col("delivery_lead_time").isNotNull()).dropDuplicates()

    fact_shipping.sort("date_key").writeTo("olist.gold.fact_shipping").using("iceberg").tableProperty("format-version", "2").createOrReplace()

def process_dimensions(spark):
    print(">>> PROCESSING DIMENSION TABLES: SILVER -> GOLD")
    process_dim_customers(spark)
    process_dim_products(spark)
    process_dim_sellers(spark)
    process_dim_date(spark)
    print("Dimension tables processed successfully!")

def process_all_facts_fixed(spark):
    print(">>> PROCESSING FACT TABLES: SILVER -> GOLD")
    process_fact_sales(spark)
    process_fact_reviews(spark)
    process_fact_shipping(spark)
    print("Fact tables processed successfully!")

    
if __name__ == "__main__":
    process_dimensions(spark)
    process_all_facts_fixed(spark)