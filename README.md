# 🎮 Steam Big Data Pipeline & Hybrid Recommendation System

## 📌 Tổng quan

Đây là **dự án Big Data theo nhóm**, xây dựng pipeline xử lý dữ liệu
end-to-end kết hợp với **hệ thống gợi ý (Recommendation System)**.

Dự án sử dụng: - PySpark - Google Cloud Dataproc (Serverless) -
BigQuery - Looker Studio - Google Colab (train mô hình)

Kiến trúc chính: **Bronze → Silver → Gold + Hybrid Recommendation**

------------------------------------------------------------------------

## 👥 Thành viên nhóm

-   B22DCKH127 - Đào Thanh Trường
-   B22DCKH119 - Bùi Văn Thắng
-   B22DCKH065 - Vũ Gia Khải
-   B22DCKH063 - Phạm Văn Kiên 
-   B22DCKH107 - Nguyễn Nhật Tân

------------------------------------------------------------------------

## 🏗 Kiến trúc tổng thể

Raw Data\
→ Dataproc (PySpark)\
→ BigQuery\
→ Google Colab (Model Training)\
→ BigQuery (Predictions)\
→ Looker Studio (Dashboard)

------------------------------------------------------------------------

## 📂 Cấu trúc thư mục

steam_bigdata/\
├── common/ \# Module dùng chung\
├── jobs/\
│ ├── bronze/ \# Profiling\
│ ├── silver/ \# Cleaning\
│ └── gold/ \# Aggregation & recommendation

notebooks/\
├── eda/ \# Khám phá dữ liệu\
└── modeling/ \# Train model (Colab)

docs/ \# Báo cáo PDF\
scripts/ \# Script submit job\
sql/ \# Query BigQuery\
dashboards/ \# Dashboard\
tests/ \# Unit test

------------------------------------------------------------------------

## ⚙️ Công nghệ sử dụng

-   PySpark\
-   Google Cloud Dataproc\
-   BigQuery\
-   Looker Studio\
-   Google Colab

------------------------------------------------------------------------

## 🥉 Bronze Layer -- Data Profiling

-   Tính null ratio\
-   Distinct count\
-   Thống kê cơ bản\
-   Phục vụ phân tích và thiết kế cleaning

------------------------------------------------------------------------

## 📊 Phân tích dữ liệu

-   Notebook: `notebooks/eda/`\
-   Báo cáo: `docs/`

------------------------------------------------------------------------

## 🥈 Silver Layer -- Data Cleaning

-   Xử lý missing values\
-   Chuẩn hóa dữ liệu\
-   Loại bỏ dữ liệu không hợp lệ

------------------------------------------------------------------------

## 🥇 Gold Layer -- Recommendation System

### 🔹 Collaborative Filtering (ALS)

-   Sử dụng PySpark ALS\
-   Dựa trên hành vi user

### 🔹 Content-based

-   Dựa trên genre, tags\
-   Tính similarity giữa các game

### 🔹 Hybrid Recommendation

Kết hợp 2 phương pháp:

final_score = α \* CF + (1 - α) \* Content

Output: user_id \| recommended_game_id \| score

------------------------------------------------------------------------

## 🤖 Huấn luyện mô hình (Google Colab)

-   Sử dụng Google Colab để:
    -   Train mô hình recommendation\
    -   Evaluate model\
    -   Thử nghiệm hybrid
-   Input:
    -   Dữ liệu từ BigQuery (Silver/Gold)
-   Output:
    -   Top-N recommendation\
    -   Lưu lại BigQuery hoặc GCS

------------------------------------------------------------------------

## ▶️ Cách chạy pipeline

``` bash
bash scripts/submit_bronze.sh
```

------------------------------------------------------------------------

## 📊 Dashboard

-   Xây dựng bằng Looker Studio\
-   Hiển thị:
    -   Data profiling\
    -   Data quality\
    -   Distribution\
    -   Recommendation (future)

------------------------------------------------------------------------

## 🎯 Mục tiêu dự án

-   Xây dựng pipeline Big Data hoàn chỉnh\
-   Áp dụng recommendation system\
-   Kết hợp Data Engineering + Machine Learning

------------------------------------------------------------------------

## 📈 Highlights

-   Pipeline Big Data trên GCP với PySpark\
-   Hybrid Recommendation System\
-   Kết hợp Dataproc + BigQuery + Colab

------------------------------------------------------------------------

## 🚀 Định hướng phát triển

-   Hoàn thiện Silver & Gold layer\
-   Tối ưu hybrid recommendation\
-   Tune tham số (α)\
-   Mở rộng real-time pipeline
