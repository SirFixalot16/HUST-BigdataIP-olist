import pandas as pd
import os
from datetime import datetime

def generate_mock_silver_data():
    """Giả lập dữ liệu đã được làm sạch từ tầng Silver."""
    customers = pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003'],
        'city': ['Hanoi', 'HCM', 'Da Nang'],
        'signup_date': pd.to_datetime(['2025-01-10', '2025-02-15', '2025-03-20'])
    })
    
    products = pd.DataFrame({
        'product_id': ['P101', 'P102', 'P103'],
        'category': ['Electronics', 'Clothing', 'Home'],
        'price': [1500.0, 50.0, 200.0]
    })
    
    orders = pd.DataFrame({
        'order_id': ['O001', 'O002', 'O003', 'O004'],
        'customer_id': ['C001', 'C001', 'C002', 'C003'],
        'product_id': ['P101', 'P102', 'P101', 'P103'],
        'order_date': pd.to_datetime(['2026-04-01', '2026-04-15', '2026-05-01', '2026-05-05']),
        'quantity': [1, 3, 2, 1]
    })
    
    return customers, products, orders

def build_gold_dimensions(customers_df, products_df, orders_df):
    """
    Xây dựng các bảng Dimension.
    Đối với CDP, Dim_Customer thường chứa sẵn các chỉ số tổng hợp (RFM).
    """
    print("[*] Đang xử lý bảng Dimension...")
    
    # 1. Bảng Dim_Product (Giữ nguyên hoặc thêm meta-data)
    dim_product = products_df.copy()
    
    # 2. Bảng Dim_Customer (Tính toán thêm các chỉ số hành vi cơ bản)
    # Tính tổng giá trị đơn hàng để làm metrics Monetary
    merged_orders = orders_df.merge(products_df, on='product_id')
    merged_orders['total_value'] = merged_orders['quantity'] * merged_orders['price']
    
    customer_metrics = merged_orders.groupby('customer_id').agg(
        total_orders=('order_id', 'count'),
        total_spent=('total_value', 'sum'),
        last_order_date=('order_date', 'max')
    ).reset_index()
    
    dim_customer = customers_df.merge(customer_metrics, on='customer_id', how='left')
    dim_customer['total_orders'] = dim_customer['total_orders'].fillna(0)
    dim_customer['total_spent'] = dim_customer['total_spent'].fillna(0)
    
    return dim_customer, dim_product

def build_gold_fact(orders_df, products_df):
    """
    Xây dựng bảng Fact.
    Bảng Fact chứa các khóa ngoại (Foreign Keys) trỏ tới Dimension và các chỉ số (Measures).
    """
    print("[*] Đang xử lý bảng Fact...")
    fact_sales = orders_df.merge(products_df[['product_id', 'price']], on='product_id', how='left')
    fact_sales['revenue'] = fact_sales['quantity'] * fact_sales['price']
    
    # Chỉ định các cột cần thiết cho bảng Fact
    columns_to_keep = ['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'revenue']
    fact_sales = fact_sales[columns_to_keep]
    
    return fact_sales

def export_to_parquet(df_dict, output_dir="gold_data"):
    """Xuất DataFrame ra định dạng Parquet."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for name, df in df_dict.items():
        file_path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(file_path, engine='pyarrow')
        print(f"[*] Đã xuất file: {file_path}")

if __name__ == "__main__":
    # 1. Tải dữ liệu Silver (Mock)
    silver_customers, silver_products, silver_orders = generate_mock_silver_data()
    
    # 2. Transform sang Gold
    dim_customer, dim_product = build_gold_dimensions(silver_customers, silver_products, silver_orders)
    fact_sales = build_gold_fact(silver_orders, silver_products)
    
    # 3. Xem trước dữ liệu Dim_Customer (Rất quan trọng cho CDP)
    print("\n--- Bản xem trước Dim_Customer (CDP Metrics) ---")
    print(dim_customer.to_string(index=False))
    
    # 4. Lưu trữ
    gold_tables = {
        'dim_customer': dim_customer,
        'dim_product': dim_product,
        'fact_sales': fact_sales
    }
    
    print("\n")
    export_to_parquet(gold_tables)
    print("\n[+] Hoàn tất xử lý tầng Gold.")