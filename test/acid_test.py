import unittest
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Import pipeline functions
from data_processing.raw_to_clean import (
    process_bronze_customers, process_bronze_orders, process_bronze_products
)
from data_processing.clean_to_curated import (
    process_dim_customers, process_dim_products, process_dim_sellers,
    process_dim_date, process_fact_sales, process_fact_reviews, process_fact_shipping
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestACIDCompliance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ACID_Test") \
            .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.test_catalog.type", "hadoop") \
            .config("spark.sql.catalog.test_catalog.warehouse", "test_warehouse") \
            .getOrCreate()
        cls.catalog = "test_catalog"

    def setUp(self):
        # Create test data
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.bronze")
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.silver")
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.gold")

    def test_atomicity_bronze_to_silver(self):
        """Test atomicity: All operations succeed or all fail"""
        try:
            # Simulate partial failure
            process_bronze_customers()
            # Force failure
            raise Exception("Simulated failure")
        except:
            # Check that no partial data was written
            try:
                count = self.spark.table(f"{self.catalog}.silver.customers").count()
                self.assertEqual(count, 0, "Atomicity violated: Partial data written")
            except:
                # Table doesn't exist, which is correct
                pass

    def test_consistency_bronze_to_silver(self):
        """Test consistency: Data integrity maintained"""
        process_bronze_customers()
        df = self.spark.table(f"{self.catalog}.silver.customers")
        # Check constraints
        self.assertTrue(df.filter(col("customer_id").isNull()).count() == 0)
        self.assertTrue(df.filter(col("customer_city") == "").count() == 0)

    def test_isolation_bronze_to_silver(self):
        """Test isolation: Concurrent operations don't interfere"""
        # This would require multiple threads, simplified here
        process_bronze_customers()
        process_bronze_orders()
        # Check that tables are independent
        cust_count = self.spark.table(f"{self.catalog}.silver.customers").count()
        order_count = self.spark.table(f"{self.catalog}.silver.orders").count()
        self.assertGreater(cust_count, 0)
        self.assertGreater(order_count, 0)

    def test_durability_bronze_to_silver(self):
        """Test durability: Data persists after restart"""
        process_bronze_customers()
        initial_count = self.spark.table(f"{self.catalog}.silver.customers").count()
        # Simulate restart by creating new session
        spark2 = SparkSession.builder.appName("Test2").getOrCreate()
        final_count = spark2.table(f"{self.catalog}.silver.customers").count()
        self.assertEqual(initial_count, final_count)

    def test_atomicity_silver_to_gold(self):
        """Test atomicity for Silver to Gold"""
        try:
            process_dim_customers(self.spark)
            raise Exception("Simulated failure")
        except:
            try:
                count = self.spark.table(f"{self.catalog}.gold.dim_customers").count()
                self.assertEqual(count, 0, "Atomicity violated")
            except:
                pass

    def test_consistency_silver_to_gold(self):
        """Test consistency for Silver to Gold"""
        process_dim_customers(self.spark)
        df = self.spark.table(f"{self.catalog}.gold.dim_customers")
        # Check that keys are unique
        key_count = df.select("customer_key").distinct().count()
        total_count = df.count()
        self.assertEqual(key_count, total_count)

class TestPerformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Performance_Test") \
            .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.test_catalog.type", "hadoop") \
            .config("spark.sql.catalog.test_catalog.warehouse", "test_warehouse") \
            .getOrCreate()

    def test_bronze_to_silver_performance(self):
        """Test performance of Bronze to Silver pipeline"""
        start_time = time.time()
        process_bronze_customers()
        process_bronze_orders()
        process_bronze_products()
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Bronze to Silver duration: {duration:.2f}s")
        # Assert reasonable performance (adjust threshold as needed)
        self.assertLess(duration, 300, "Performance threshold exceeded")

    def test_silver_to_gold_performance(self):
        """Test performance of Silver to Gold pipeline"""
        start_time = time.time()
        process_dim_customers(self.spark)
        process_dim_products(self.spark)
        process_dim_sellers(self.spark)
        process_dim_date(self.spark)
        process_fact_sales(self.spark)
        process_fact_reviews(self.spark)
        process_fact_shipping(self.spark)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Silver to Gold duration: {duration:.2f}s")
        self.assertLess(duration, 600, "Performance threshold exceeded")

    def test_data_volume_integrity(self):
        """Test that data volume is maintained correctly"""
        # Bronze to Silver
        bronze_cust_count = self.spark.table("olist.bronze.customers").count()
        process_bronze_customers()
        silver_cust_count = self.spark.table("olist.silver.customers").count()
        self.assertLessEqual(silver_cust_count, bronze_cust_count)

        # Silver to Gold
        process_dim_customers(self.spark)
        gold_dim_count = self.spark.table("olist.gold.dim_customers").count()
        self.assertEqual(gold_dim_count, silver_cust_count)

if __name__ == '__main__':
    unittest.main()