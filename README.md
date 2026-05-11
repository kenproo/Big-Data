# Hệ thống gợi ý game Steam sử dụng Big Data, ALS và MiniLM

Đây là dự án xây dựng hệ thống gợi ý game Steam theo hướng **Hybrid Recommendation** trên nền tảng Big Data. Hệ thống kết hợp dữ liệu hành vi người dùng, nội dung văn bản, metadata game và tín hiệu ngôn ngữ để tạo đề xuất cho cả người dùng cũ và người dùng mới.

Dự án sử dụng pipeline xử lý dữ liệu theo kiến trúc **Bronze - Silver - Gold** trên Google Cloud, trong đó dữ liệu được lưu trữ trên Google Cloud Storage, xử lý và phân tích bằng BigQuery, Spark/Dataproc và Python.

Mô hình gợi ý chính gồm hai phần: **ALS Collaborative Filtering** để khai phá hành vi tương tác giữa user và game, và **MiniLM multilingual embedding** để khai phá ngữ nghĩa từ mô tả game và nội dung review. Các tín hiệu này được kết hợp với quality score, popularity score, recency score, price score và language score để tạo công thức hybrid scoring.

Kết quả cuối cùng là bảng recommendation dạng **Steam-like homepage**, gồm nhiều section đề xuất như Recommended For You, Because Of Your Play History, Community Review Match, Popular & Highly Rated, New & Trending, Free / Budget Friendly, Action & Adventure và Strategy & Simulation.

Dự án hướng đến mục tiêu chứng minh khả năng ứng dụng Big Data và Data Mining trong bài toán recommendation system, đồng thời xử lý được cả hai tình huống: cá nhân hóa cho người dùng cũ và cold-start recommendation cho người dùng mới.

## Công nghệ sử dụng

- Google Cloud Storage
- BigQuery
- Spark / Dataproc
- Python
- SQL
- ALS Collaborative Filtering
- MiniLM multilingual embeddings
- Looker Studio / Data Studio
- Streamlit

## Kết quả chính

- Tạo được Top-30 recommendation cho 10,000 người dùng bằng ALS.
- Tạo được MiniLM embedding cho game text và review text.
- Xây dựng được các bảng hybrid recommendation cho old user và new user.
- Tạo được bảng demo cuối cùng phục vụ giao diện Steam-like homepage.
- Hỗ trợ đánh giá bằng các chỉ số Precision@K, Recall@K, HitRate@K, NDCG@K, Catalog Coverage và Popularity Bias.

## Demo

Dữ liệu demo cuối cùng được dùng để hiển thị giao diện gợi ý game dạng Steam homepage, trong đó người dùng có thể chọn loại user, user ID và section đề xuất để xem danh sách game phù hợp.

## Ghi chú

Repository này chỉ lưu mã nguồn, SQL scripts, tài liệu, ảnh kết quả và dữ liệu mẫu nhỏ phục vụ demo. Dataset đầy đủ, file embedding lớn và credential Google Cloud không được đưa lên GitHub.
