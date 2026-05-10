import pandas as pd
import numpy as np

# Giả định hàm tính khoảng cách địa lý (Haversine)
def haversine(lat1, lon1, lat2, lon2):
    # Khung logic tính toán khoảng cách (sẽ cần thư viện math hoặc numpy)
    # Tạm thời trả về giá trị mô phỏng
    return np.abs(lat1 - lat2) + np.abs(lon1 - lon2)

def process_bronze_to_silver(customers_df, orders_df, reviews_df, products_df):
    """Thực thi các quy tắc làm sạch dữ liệu (Silver Layer)"""
    print("[*] Đang xử lý Bronze -> Silver...")
    
    # 1. Xử lý Null và Trùng lặp
    orders_df = orders_df.dropna(subset=['order_id', 'customer_id']).drop_duplicates()
    
    # 2. Biến đổi kiểu dữ liệu Datetime
    date_columns = [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    for col in date_columns:
        if col in orders_df.columns:
            orders_df[col] = pd.to_datetime(orders_df[col])
            
    if 'review_creation_date' in reviews_df.columns:
        reviews_df['review_creation_date'] = pd.to_datetime(reviews_df['review_creation_date'])
        reviews_df['review_answer_timestamp'] = pd.to_datetime(reviews_df['review_answer_timestamp'])

    # 3. Chuẩn hóa chuỗi địa lý (Loại bỏ dấu)
    if 'customer_city' in customers_df.columns:
        customers_df['customer_city'] = customers_df['customer_city'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
        # Lọc bỏ dữ liệu lỗi (city là số)
        customers_df = customers_df[~customers_df['customer_city'].str.isnumeric()]

    # 4. Sửa lỗi chính tả và ép kiểu INT cho Product
    products_df = products_df.rename(columns={
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    })
    products_df['product_photos_qty'] = products_df['product_photos_qty'].fillna(0).astype(int)

    return customers_df, orders_df, reviews_df, products_df

def process_silver_to_gold(customers_df, orders_df, reviews_df, products_df, order_items_df):
    """Thực thi các logic nghiệp vụ và Star Schema (Gold Layer)"""
    print("[*] Đang xử lý Silver -> Gold...")
    
    # 1. Bảng Fact Reviews
    fact_reviews = reviews_df.copy()
    fact_reviews['comment_length'] = fact_reviews['review_comment_message'].str.len().fillna(0)
    fact_reviews['response_time_days'] = (fact_reviews['review_answer_timestamp'] - fact_reviews['review_creation_date']).dt.days

    # 2. Bảng Fact Shipping
    fact_shipping = orders_df.copy()
    fact_shipping['delivery_lead_time_days'] = (fact_shipping['order_delivered_customer_date'] - fact_shipping['order_purchase_timestamp']).dt.days
    fact_shipping['is_late'] = (fact_shipping['order_delivered_customer_date'] - fact_shipping['order_estimated_delivery_date']).dt.days
    # Ghi chú: Khoảng cách shipping_distance_km sẽ được join thêm từ bảng Geolocation

    # 3. Denormalization (Gộp bảng phẳng cho Power BI Web như đã làm)
    # Bạn sẽ merge fact_shipping, order_items, fact_reviews và customers lại thành 1 DataFrame duy nhất tại đây.
    
    print("[+] Hoàn tất tính toán chỉ số Gold.")
    return fact_shipping # Trả về dataframe gộp cuối cùng

# Thực thi (Bạn cần thay đường dẫn bằng file CSV thật của Olist)
if __name__ == "__main__":
    print("Vui lòng nạp file CSV thực tế để chạy pipeline.")