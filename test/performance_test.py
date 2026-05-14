import time
import unittest
from pyspark.sql import SparkSession
import logging

# Import pipeline functions
from data_processing.raw_to_clean import (
    process_bronze_customers, process_bronze_orders, process_bronze_products
)
from data_processing.clean_to_curated import (
    process_dimensions, process_all_facts_fixed
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestPerformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Performance_Test") \
            .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.test_catalog.type", "hadoop") \
            .config("spark.sql.catalog.test_catalog.warehouse", "test_warehouse") \
            .getOrCreate()

    def measure_performance(self, func, *args, **kwargs):
        """Measure execution time"""
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        return result, duration

    def test_bronze_to_silver_performance(self):
        """Performance test for Bronze to Silver pipeline"""
        logger.info("Testing Bronze to Silver performance...")

        _, duration = self.measure_performance(
            lambda: (
                process_bronze_customers(),
                process_bronze_orders(),
                process_bronze_products()
            )
        )

        logger.info(f"Bronze->Silver: {duration:.2f}s")

        # Performance thresholds (adjust based on your environment)
        self.assertLess(duration, 300, f"Duration {duration:.2f}s exceeds threshold")

    def test_silver_to_gold_performance(self):
        """Performance test for Silver to Gold pipeline"""
        logger.info("Testing Silver to Gold performance...")

        _, duration = self.measure_performance(
            lambda: (
                process_dimensions(self.spark),
                process_all_facts_fixed(self.spark)
            )
        )

        logger.info(f"Silver->Gold: {duration:.2f}s")

        self.assertLess(duration, 600, f"Duration {duration:.2f}s exceeds threshold")

    def test_throughput_bronze_to_silver(self):
        """Test data processing throughput"""
        # Get input data size
        try:
            bronze_count = self.spark.table("olist.bronze.customers").count()
        except:
            bronze_count = 1000  # Default for test

        start_time = time.time()
        process_bronze_customers()
        end_time = time.time()

        silver_count = self.spark.table("olist.silver.customers").count()
        throughput = silver_count / (end_time - start_time)  # records/second

        logger.info(f"Throughput: {throughput:.2f} records/sec")
        self.assertGreater(throughput, 10, "Throughput too low")

    def test_scalability_test(self):
        """Test scalability with increasing data size"""
        # This would require generating test data of different sizes
        # Simplified version
        sizes = [100, 1000, 10000]
        times = []

        for size in sizes:
            # Generate test data
            test_df = self.spark.range(size)
            start_time = time.time()
            # Simulate processing
            result = test_df.groupBy().count().collect()
            end_time = time.time()
            times.append(end_time - start_time)

        # Check that time doesn't increase exponentially
        ratio_1_to_2 = times[1] / times[0]
        ratio_2_to_3 = times[2] / times[1]

        logger.info(f"Scalability ratios: {ratio_1_to_2:.2f}, {ratio_2_to_3:.2f}")
        self.assertLess(ratio_2_to_3, ratio_1_to_2 * 2, "Poor scalability")

    def test_concurrent_performance(self):
        """Test performance under concurrent load"""
        import threading

        results = []
        def run_pipeline():
            start = time.time()
            process_bronze_customers()
            end = time.time()
            results.append(end - start)

        threads = []
        for i in range(3):  # Simulate 3 concurrent runs
            t = threading.Thread(target=run_pipeline)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        avg_time = sum(results) / len(results)
        logger.info(f"Concurrent avg time: {avg_time:.2f}s")
        self.assertLess(avg_time, 100, "Concurrent performance degraded")

if __name__ == '__main__':
    unittest.main()