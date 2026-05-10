import time
import os
import psycopg2
from datetime import datetime

class LakehouseEvaluator:
    def __init__(self):
        """Khởi tạo kết nối đến PostgreSQL và tạo bảng nếu chưa có."""
        try:
            self.conn = psycopg2.connect(
                host="localhost",
                database="metrics_db",
                user="admin",
                password="admin_password",
                port="5432"
            )
            self._create_table()
            print("[*] Đã kết nối thành công tới Metrics Database.")
        except Exception as e:
            print(f"[!] Lỗi kết nối Database: {e}")

    def _create_table(self):
        """Tạo bảng chuẩn bị lưu trữ dữ liệu giám sát."""
        query = """
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            id SERIAL PRIMARY KEY,
            job_name VARCHAR(100),
            execution_time TIMESTAMP,
            duration_seconds FLOAT,
            processed_records INT,
            throughput_rec_per_sec FLOAT,
            status VARCHAR(50)
        )
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()

    def evaluate_job(self, job_name, spark_function, *args, **kwargs):
        print(f"\n[>] Bắt đầu đo lường hiệu năng: {job_name}")
        start_time = time.time()
        start_timestamp = datetime.now()
        
        row_count = 0
        try:
            result_df = spark_function(*args, **kwargs)
            row_count = result_df.count() 
            status = "SUCCESS"
        except Exception as e:
            status = f"FAILED"
            print(f"[!] Lỗi: {str(e)}")
            
        end_time = time.time()
        duration_seconds = end_time - start_time
        throughput = row_count / duration_seconds if duration_seconds > 0 else 0
        
        # Đẩy dữ liệu thẳng vào PostgreSQL
        self._write_to_db(job_name, start_timestamp, duration_seconds, row_count, throughput, status)
        
        print("-" * 40)
        print(f"BÁO CÁO NHANH: {status} | Time: {round(duration_seconds, 2)}s | Throughput: {round(throughput, 2)} rec/s")
        print("-" * 40)

    def _write_to_db(self, job, exec_time, duration, records, throughput, status):
        query = """
        INSERT INTO pipeline_metrics 
        (job_name, execution_time, duration_seconds, processed_records, throughput_rec_per_sec, status) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (job, exec_time, duration, records, throughput, status))
            self.conn.commit()

    def close(self):
        self.conn.close()