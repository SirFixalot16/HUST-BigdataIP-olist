import streamlit as st
import pandas as pd
import json
import plotly.express as px
import os

# 1. Cấu hình trang
st.set_page_config(page_title="Lakehouse Performance", layout="wide")
st.title("🚀 Data Lakehouse Performance Monitor")
st.markdown("Dashboard giám sát luồng ETL tự động đọc từ log file hệ thống.")

# 2. Hàm đọc và xử lý file JSONL
@st.cache_data(ttl=5) # Cache data trong 5 giây để tối ưu khi reload
def load_data(log_file="logs/pipeline_metrics.jsonl"):
    if not os.path.exists(log_file):
        return pd.DataFrame()
    
    data = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
                
    df = pd.DataFrame(data)
    if not df.empty:
        # Chuyển đổi định dạng thời gian để vẽ biểu đồ
        df['execution_time'] = pd.to_datetime(df['execution_time'])
    return df

# 3. Trực quan hóa dữ liệu
df = load_data()

if df.empty:
    st.warning("⚠️ Chưa có dữ liệu log. Hãy chạy file `run_test.py` để tạo dữ liệu.")
else:
    # --- SECTION 1: Các chỉ số KPI (Overview) ---
    st.subheader("1. System Overview (Chỉ số Tổng quan)")
    col1, col2, col3, col4 = st.columns(4)
    
    total_records = df['processed_records'].sum()
    avg_throughput = df['throughput_rec_per_sec'].mean()
    total_jobs = len(df)
    success_rate = (len(df[df['status'] == 'SUCCESS']) / total_jobs) * 100 if total_jobs > 0 else 0

    col1.metric("Tổng Records Đã Xử Lý", f"{total_records:,}")
    col2.metric("Thông Lượng TB (rec/s)", f"{avg_throughput:,.2f}")
    col3.metric("Tổng Số Luồng Đã Chạy", total_jobs)
    col4.metric("Tỉ lệ Thành Công", f"{success_rate:.1f}%")

    st.divider()

    # --- SECTION 2: Biểu đồ theo dõi (Performance Charts) ---
    st.subheader("2. Performance Charts (Đánh giá Hiệu năng)")
    fig_col1, fig_col2 = st.columns(2)

    with fig_col1:
        st.markdown("**Biến động Thông lượng (Throughput Trend)**")
        fig1 = px.line(df, x='execution_time', y='throughput_rec_per_sec', 
                       color='job_name', markers=True, 
                       labels={'execution_time': 'Thời gian', 'throughput_rec_per_sec': 'Dòng / Giây'})
        st.plotly_chart(fig1, use_container_width=True)

    with fig_col2:
        st.markdown("**Thời gian Thực thi (Execution Duration)**")
        fig2 = px.bar(df, x='execution_time', y='duration_seconds', 
                      color='job_name',
                      labels={'execution_time': 'Thời gian', 'duration_seconds': 'Giây'})
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # --- SECTION 3: Bảng dữ liệu gốc (Raw Logs) ---
    st.subheader("3. Raw Logs (Bảng kiểm tra sự cố)")
    # Sắp xếp để log mới nhất hiện lên đầu
    st.dataframe(df.sort_values(by='execution_time', ascending=False), use_container_width=True)