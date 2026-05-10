# TÀI LIỆU ĐẶC TẢ NGHIỆP VỤ BI (BI SPECIFICATIONS)
**Dự án:** Olist Data Lakehouse (CDP Architecture)
**Phụ trách:** BI & Data Analytics

## 1. Tổng quan Lược đồ Dữ liệu (Data Model Overview)
Dữ liệu tại tầng Gold sẽ được tổ chức theo cấu trúc Star Schema, bao gồm 3 bảng Fact đại diện cho 3 domain nghiệp vụ chính, bao quanh bởi các bảng Dimensions chung:
* **Dimensions:** Dim_Date, Dim_Customer, Dim_Product, Dim_Seller, Dim_Geography.
* **Facts:** Fact_Sale (Doanh thu), Fact_Reviews (Trải nghiệm), Fact_Shipping (Vận hành).

## 2. Đặc tả các Chủ điểm Phân tích (Dashboard Topics)

### Chủ điểm 1: Theo dõi Doanh thu & Đơn hàng (Sales & Revenue Performance)
* **Nguồn dữ liệu:** `Fact_Sale`, `Dim_Date`, `Dim_Product`
* **Metrics Yêu cầu tính toán (PySpark cần chuẩn bị):**
    * `Total Revenue`: SUM(price + freight_value)
    * `Average Order Value (AOV)`: Total Revenue / Total Orders
    * `Average Installments`: AVG(payment_installments)
    * `Total Orders`: COUNT(DISTINCT order_id)
    * `Cancel Rate`: (Tổng đơn status = 'canceled') / Tổng đơn hàng
* **Biểu đồ dự kiến (Tableau/PowerBI):** * Line Chart: Biến động doanh thu theo tháng (thử nghiệm tính năng Time Travel).
    * Bar Chart: Doanh thu theo phương thức thanh toán (`payment_value`).

### Chủ điểm 2: Hiệu suất Vận tải & Logistics (Shipping & Logistics)
* **Nguồn dữ liệu:** `Fact_Shipping`, `Dim_Date`, `Dim_Geography`
* **Metrics Yêu cầu tính toán (PySpark cần chuẩn bị):**
    * `Delivery Lead Time (Days)`: AVG(order_delivered_customer_date - order_purchase_timestamp)
    * `Late Delivery Rate`: Tỉ lệ phần trăm các đơn có `is_late > 0`
    * `Average Shipping Fee`: AVG(freight_value)
    * `Average Shipping Distance (km)`: AVG(shipping_distance_km) - Dựa trên hàm haversine do DE chuẩn bị.
* **Biểu đồ dự kiến (Tableau/PowerBI):**
    * Map Visual: Bản đồ nhiệt (Heatmap) thể hiện thời gian giao hàng theo bang (State) tại Brazil.
    * Scatter Plot: Mối tương quan giữa Khoảng cách vận chuyển (km) và Phí vận chuyển.

### Chủ điểm 3: Nền tảng Dữ liệu Khách hàng & Trải nghiệm (CDP & Customer Experience)
* **Nguồn dữ liệu:** `Fact_Reviews`, `Fact_Sale`, `Dim_Customer`
* **Metrics Yêu cầu tính toán (PySpark cần chuẩn bị):**
    * `Average Review Score`: AVG(review_score)
    * `Review Response Time (Days)`: AVG(response_time)
    * `New vs Returning Customer`: Đếm số khách hàng có hóa đơn thứ 2 trở lên.
* **Biểu đồ dự kiến (Tableau/PowerBI):**
    * Gauge Chart: Hiển thị điểm đánh giá trung bình.
    * Bar Chart: Số lượng bình luận tiêu cực (score 1,2) được phân loại theo chiều dài comment (`comment_length`).



# Tài liệu Tích hợp: Boto3, Parquet và PySpark với S3 (MinIO)

## 1. Tổng quan Kiến trúc
Hệ thống này thực hiện quy trình xử lý dữ liệu (ETL) cơ bản:
1.  **Ingestion:** Tạo và tải dữ liệu định dạng Parquet từ Local lên S3 (sử dụng Boto3).
2.  **Storage:** Lưu trữ dữ liệu thô (Raw Data) trên S3/MinIO.
3.  **Processing:** PySpark đọc dữ liệu trực tiếp từ S3, thực hiện lọc các mô hình AI có độ chính xác cao (`accuracy_score > 0.90`).
4.  **Sink:** PySpark ghi dữ liệu đã xử lý (Processed Data) phân mảnh trở lại S3 dưới định dạng Parquet.

## 2. Yêu cầu Hệ thống (Prerequisites)
*   Python 3.10+
*   Java (OpenJDK 17)
*   Thư viện Python: `boto3`, `pyspark`, `pandas`, `pyarrow`, `fastparquet`
*   Hạ tầng: MinIO chạy qua Docker (hoặc AWS S3 thực tế).

## 3. Cấu hình Môi trường
Hệ thống yêu cầu các biến môi trường sau để xác thực:
*   `AWS_ACCESS_KEY_ID`: [Access Key]
*   `AWS_SECRET_ACCESS_KEY`: [Secret Key]
*   `ENDPOINT_URL`: http://localhost:9000 (Dùng cho MinIO)

Đối với PySpark, cần thiết lập biến môi trường trỏ đến Java:
`export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"`

## 4. Hướng dẫn Vận hành
**Bước 1: Tải dữ liệu lên S3**
Thực thi script `connect_db.py`. Script này sử dụng Boto3 client kết nối đến S3 và dùng hàm `upload_file` để đẩy file `sample_tsfm_data.parquet` lên thư mục `raw_data/`.

**Bước 2: Đọc và Xử lý dữ liệu bằng PySpark**
Thực thi script `spark_s3.py`.
*   PySpark sử dụng giao thức `s3a://` với các gói Java `hadoop-aws` và `aws-java-sdk-bundle`.
*   Hệ thống đọc file từ `s3a://[bucket]/raw_data/`.
*   Tiến hành filter dữ liệu.
*   Ghi đè (overwrite) kết quả xuống `s3a://[bucket]/processed_data/`.

## 5. Xử lý sự cố (Troubleshooting)
*   **Lỗi `No module named pyarrow`:** Chạy lệnh `pip install pyarrow`.
*   **Lỗi Java Gateway Exited:** Đảm bảo `JAVA_HOME` được cấu hình cứng trong script Python hoặc biến môi trường hệ điều hành.
*   **Lỗi `For input string: "60s"`:** Cấu hình ép buộc Spark sử dụng `SimpleAWSCredentialsProvider` và truyền tham số timeout dưới dạng số nguyên (mili-giây).