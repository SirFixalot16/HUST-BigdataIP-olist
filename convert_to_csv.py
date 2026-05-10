import pandas as pd
import os

def convert_parquet_to_csv(directory="gold_data"):
    files = ['dim_customer', 'dim_product', 'fact_sales']
    
    for file_name in files:
        parquet_path = os.path.join(directory, f"{file_name}.parquet")
        csv_path = os.path.join(directory, f"{file_name}.csv")
        
        if os.path.exists(parquet_path):
            df = pd.read_parquet(parquet_path)
            df.to_csv(csv_path, index=False)
            print(f"[*] Đã chuyển đổi thành công: {csv_path}")
        else:
            print(f"[!] Không tìm thấy tệp: {parquet_path}")

if __name__ == "__main__":
    convert_parquet_to_csv()