import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    datediff,
    lit,
    count as spark_count,
    sum as spark_sum,
    max as spark_max
)

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

import json

CONFIG = 'olist_guest.json'
with open(CONFIG, 'r') as file:
    data = json.load(file)
BUCKET = data["bucket"]

def process_cdp_ml(spark, catalog):

    fact_sales = spark.table(f"{catalog}.gold.fact_sales")
    dim_date = spark.table(f"{catalog}.gold.dim_date")
    dim_cust = spark.table(f"{catalog}.gold.dim_customers")

    df_join = (
        fact_sales
        .join(dim_date, "date_key")
        .join(dim_cust, "customer_key")
    )

    max_date = df_join.agg(spark_max("full_date")).first()[0]
    now_date = max_date + datetime.timedelta(days=1)

    df_rfm = (
        df_join
        .groupBy("customer_unique_id", "customer_city", "customer_state")
        .agg(
            datediff(lit(now_date), spark_max("full_date")).alias("Recency"),
            spark_count("sale_key").alias("Frequency"),
            spark_sum("payment_value").alias("Monetary")
        )
        .fillna(0)
    )

    assembler = VectorAssembler(
        inputCols=["Recency", "Frequency", "Monetary"],
        outputCol="raw_features"
    )

    df_features = assembler.transform(df_rfm)

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    scaler_model = scaler.fit(df_features)
    df_scaled = scaler_model.transform(df_features)

    df_scaled.cache()

    kmeans = (
        KMeans()
        .setK(4)
        .setSeed(42)
        .setFeaturesCol("features")
        .setPredictionCol("Cluster")
    )

    model = kmeans.fit(df_scaled)

    df_clustered = model.transform(df_scaled)

    table_name = f"{catalog}.gold.cdp_rfm_segments"

    # (
    #     df_clustered
    #     .write
    #     .mode("overwrite")
    #     .saveAsTable(table_name)
    # )

    df_output = df_clustered.drop("raw_features", "features")
    (
        df_output
        .writeTo(table_name)
        .using("iceberg")
        .createOrReplace()
    )

    # pdf = df_clustered.toPandas()
    # pdf.to_csv("cdp_rfm.csv", index=False)
    pdf = df_output.toPandas()
    pdf.to_csv("cdp_rfm.csv", index=False)

def save_cdp_csv():
    spark = (
            SparkSession.builder
            .appName("cdp_csv")
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

    catalog = "olist"

    process_cdp_ml(spark, catalog)

    print("Conversion successful")

    spark.stop()

if __name__ == "__main__":
    save_cdp_csv()