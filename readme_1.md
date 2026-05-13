Xoá các gói java
rm -rf ~/.ivy2/cache ~/.ivy2/jars

Cài java
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

Chạy airflow 

airflow db migrate
airflow api-server -p 8080

airflow standalone


# 1. Khai báo thư mục gốc
export AIRFLOW_HOME=$(pwd)

# 2. Ép Airflow dùng cơ chế 'spawn' thay vì 'fork' (Sửa lỗi SIGSEGV)
export AIRFLOW__CORE__MP_START_METHOD=spawn

# 3. Tắt các kiểm tra an toàn gây crash trên Mac
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export NO_PROXY="*"

# 4. Bật trình gỡ lỗi Python nếu có crash (để xem log chi tiết hơn)
export PYTHONFAULTHANDLER=true

olist_orders_ingestion_manager