from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("iceberg-test") \
    .config(
        "spark.sql.catalog.demo",
        "org.apache.iceberg.spark.SparkCatalog"
    ) \
    .config(
        "spark.sql.catalog.demo.type",
        "hadoop"
    ) \
    .config(
        "spark.sql.catalog.demo.warehouse",
        "warehouse"
    ) \
    .getOrCreate()

df = spark.range(0, 100)

path = "lakehouse/silver/test_atomic"

try:
    spark.sql("""
    INSERT INTO demo.silver.orders
    SELECT * FROM demo.bronze.orders
    """)

    raise Exception("ETL crashed")

except Exception:
    print("rollback test")