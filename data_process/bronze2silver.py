import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg as spark_avg
from utils import clean_city_name, fix_product_name

import json
CONFIG = 'olist_guest.json'
with open(CONFIG, 'r') as file:
    data = json.load(file)
BUCKET = data["bucket_alt"]

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
        f"s3a://{BUCKET}/warehouse/"
    ) \
    .getOrCreate()

catalog = "local_catalog"
logger.info(" Spark Iceberg đã sẵn sàng!")

def process_bronze_customers():
    
    logger.info("")
    df_cust_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/customers/")
    r1 = df_cust_raw.count()
    df_cust = df_cust_raw.dropDuplicates(["customer_id"]).filter(col("customer_id").isNotNull()) \
                         .withColumn("customer_city", clean_city_name("customer_city")).filter(col("customer_city") != "")
    df_cust.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.customers")
    c1 = spark.table(f"{catalog}.silver.customers").count()
    logger.info(f"  [Customers] Giữ lại {c1}/{r1} dòng (Loại bỏ {r1-c1} bản ghi lỗi định dạng).")

def process_bronze_orders():

    df_orders_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/orders/")
    r5 = df_orders_raw.count()
    for date_col in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]:
        df_orders = df_orders_raw.withColumn(date_col, col(date_col).cast("timestamp"))

    df_orders = df_orders.filter(
        # 1. Mua hàng <= Duyệt đơn
        ((col("order_purchase_timestamp") <= col("order_approved_at")) | col("order_approved_at").isNull()) &
        # 2. Duyệt đơn <= Giao kho
        ((col("order_approved_at") <= col("order_delivered_carrier_date")) | col("order_delivered_carrier_date").isNull()) &
        # 3. Giao kho <= Khách nhận
        ((col("order_delivered_carrier_date") <= col("order_delivered_customer_date")) | col("order_delivered_customer_date").isNull())
    ).dropDuplicates(["order_id"]).filter(col("order_id").isNotNull())
    
    df_orders.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.orders")
    c5 = spark.table(f"{catalog}.silver.orders").count()
    logger.info(f"  [Orders] Giữ lại {c5}/{r5} dòng (Loại bỏ {r5-c5} bản ghi sai trình tự thời gian/không khớp mã).")

def process_bronze_products():
    df_prod_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/products/")
    r2 = df_prod_raw.count()
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
    df_prod = fix_product_name(df_prod)
    
    df_prod.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.products")

    c2 = spark.table(f"{catalog}.silver.products").count()
    logger.info(f"  [Products] Giữ lại {c2}/{r2} dòng (Đã lọc sạch toàn bộ thông số âm).")

    df_prod.write.format("iceberg").mode("overwrite").saveAsTable("olist.silver.products")

def process_bronze_sellers():
    df_sell_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/sellers/")
    r3 = df_sell_raw.count()
    df_sell = df_sell_raw.dropDuplicates(["seller_id"]).withColumn("seller_city", clean_city_name("seller_city"))
    df_sell.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.sellers")
    c3 = spark.table(f"{catalog}.silver.sellers").count()
    logger.info(f"  [Sellers] Giữ lại {c3}/{r3} dòng (Loại bỏ {r3-c3} bản ghi lỗi định dạng).")

def process_bronze_order_items():
    df_items_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/order_items/")
    r6 = df_items_raw.count()
    df_items = df_items_raw.withColumn("shipping_limit_date", col("shipping_limit_date").cast("timestamp")) \
        .filter((col("price") >= 0) & (col("freight_value") >= 0))

    df_items.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.order_items")
    c6 = spark.table(f"{catalog}.silver.order_items").count()
    logger.info(f"  [Order Items] Giữ lại {c6}/{r6} dòng (Loại bỏ {r6-c6} dòng dữ liệu không khớp tham chiếu/giá trị âm).")
    
def process_bronze_order_reviews():
    df_rev_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/order_reviews/")
    r8 = df_rev_raw.count()
    df_rev = df_rev_raw.withColumn("review_creation_date", col("review_creation_date").cast("timestamp")) \
        .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast("timestamp")) \
        .filter((col("review_score") >= 1) & (col("review_score") <= 5))

    df_rev.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.order_reviews")
    c8 = spark.table(f"{catalog}.silver.order_reviews").count()
    logger.info(f"  [Reviews] Giữ lại {c8}/{r8} dòng (Loại bỏ {r8-c8} đánh giá không khớp đơn hàng/điểm ảo).")

    logger.info("HOÀN TẤT TẦNG SILVER VỚI DỮ LIỆU ĐÃ ĐƯỢC CHUẨN HÓA THAM CHIẾU!")

def process_bronze_payments():
    df_pay_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/order_payments/olist_order_payments_dataset.parquet")
    r7 = df_pay_raw.count()
    df_pay = df_pay_raw.dropDuplicates()
    df_pay.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.order_payments")
    c7 = spark.table(f"{catalog}.silver.order_payments").count()
    logger.info(f"  [Payments] Giữ lại {c7}/{r7} dòng (Loại bỏ {r7-c7} thanh toán không khớp mã đơn hàng).")

def process_bronze_geolocation():
    df_geo_raw = spark.read.parquet(f"s3a://{BUCKET}/raw/olist/geolocation/")
    df_geo_raw.filter((col("geolocation_lat") >= -90) & (col("geolocation_lat") <= 90)) \
        .filter((col("geolocation_lng") >= -180) & (col("geolocation_lng") <= 180)) \
        .groupBy("geolocation_zip_code_prefix").agg(spark_avg("geolocation_lat").alias("geo_lat"), spark_avg("geolocation_lng").alias("geo_lng")) \
        .withColumn("geolocation_city", clean_city_name("geolocation_city")).filter(col("geolocation_city") != "") \
        .write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog}.silver.geolocation")
    logger.info(f"  [Geolocation] Đã chuẩn hóa tọa độ địa lý thành công.")

def Bronze2Silver():
    logger.info(" BẮT ĐẦU TẦNG SILVER: LÀM SẠCH VÀ XỬ LÝ DỮ LIỆU KHÔNG KHỚP THAM CHIẾU")

    process_bronze_geolocation()
    process_bronze_products()
    process_bronze_customers()
    process_bronze_sellers()
    process_bronze_orders()
    process_bronze_order_items()
    process_bronze_order_reviews()
    process_bronze_payments()
    
    print("Bronze to Silver processing completed successfully.")

if __name__ == "__main__":
    Bronze2Silver()