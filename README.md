# Steam Big Data Pipeline và Hệ Gợi Ý Hybrid

## Tổng quan dự án

Đây là dự án Big Data theo nhóm, tập trung xây dựng pipeline xử lý dữ liệu end-to-end cho hệ sinh thái Steam, đồng thời mở rộng sang bài toán gợi ý game bằng mô hình hybrid recommendation.

Dự án bao gồm toàn bộ quy trình từ xử lý dữ liệu thô, profiling, làm sạch, chuẩn hóa, tổng hợp dữ liệu, xây dựng dashboard, tạo feature cho machine learning, đến thử nghiệm mô hình gợi ý ở giai đoạn mở rộng.

Kiến trúc chính của dự án:

**Bronze → Silver → Gold → Hybrid Recommendation**

Công nghệ sử dụng gồm:

- PySpark
- Google Cloud Dataproc Serverless
- Google Cloud Storage (GCS)
- BigQuery
- Looker Studio
- Google Colab

---

## Thành viên nhóm

- B22DCKH127 - Đào Thanh Trường
- B22DCKH119 - Bùi Văn Thắng
- B22DCKH065 - Vũ Gia Khải
- B22DCKH063 - Phạm Văn Kiên
- B22DCKH107 - Nguyễn Nhật Tân

---

## Mục tiêu dự án

Dự án hướng tới các mục tiêu sau:

- Xây dựng pipeline Big Data hoàn chỉnh trên nền tảng Google Cloud
- Xử lý dữ liệu Steam đa nguồn với khối lượng lớn
- Làm sạch và chuẩn hóa dữ liệu để phục vụ phân tích
- Xây dựng các bảng Gold phục vụ:
  - phân tích nghiệp vụ
  - trực quan hóa dashboard
  - feature engineering
  - dữ liệu huấn luyện cho hệ gợi ý
- Mở rộng sang hệ gợi ý lai (Hybrid Recommendation System)
- Thử nghiệm hướng nâng cao với dữ liệu đa ngôn ngữ

---

## Kiến trúc tổng thể

```text
Nguồn dữ liệu thô
    ↓
Bronze Layer (Raw ingestion + profiling)
    ↓
Silver Layer (Cleaning + validation + rejected split)
    ↓
Gold Layer (Analytics + features + training datasets)
    ↓
Modeling trên Google Colab
    ↓
Recommendation output / prediction results
    ↓
BigQuery + Looker Studio
```

Luồng xử lý chính:

- **Bronze** lưu dữ liệu gốc hoặc dữ liệu mới chỉ được chuẩn hóa ở mức tối thiểu
- **Silver** làm sạch dữ liệu, chuẩn hóa schema, kiểm tra chất lượng và tách dữ liệu hợp lệ với dữ liệu bị loại
- **Gold** tạo các bảng phục vụ phân tích, dashboard và feature engineering
- **Google Colab** được dùng để thử nghiệm, huấn luyện và đánh giá mô hình gợi ý
- **BigQuery + Looker Studio** phục vụ truy vấn, dashboard và báo cáo

---

## Cấu trúc thư mục

```text
steam_bigdata/
├── common/                  # Module dùng chung: config, io, transforms, quality, spark config
├── jobs/
│   ├── bronze/              # Job profiling và xử lý dữ liệu raw sang parquet
│   ├── silver/              # Job cleaning, validation, split rejected, quality jobs
│   └── gold/                # Job tổng hợp analytics, feature engineering, training datasets
│
├── notebooks/
│   ├── eda/                 # Notebook khám phá dữ liệu
│   └── modeling/            # Notebook thử nghiệm recommendation / Colab
│
├── docs/                    # Báo cáo, tài liệu mô tả dự án, EDA PDF
├── scripts/                 # Script submit Dataproc batch jobs
├── sql/                     # Query BigQuery
├── dashboards/              # Ảnh chụp / mô tả dashboard
├── tests/                   # File kiểm thử
└── README.md
```

---

## Công nghệ sử dụng

- **PySpark** để xử lý dữ liệu phân tán
- **Google Cloud Dataproc Serverless** để chạy Spark jobs trên cloud
- **Google Cloud Storage (GCS)** để lưu trữ dữ liệu Bronze, Silver và Gold
- **BigQuery** để phục vụ truy vấn phân tích và BI
- **Looker Studio** để xây dựng dashboard trực quan
- **Google Colab** để huấn luyện và thử nghiệm mô hình gợi ý

---

## Đặc điểm dữ liệu

Bộ dữ liệu Steam được tổng hợp từ nhiều nguồn, bao gồm:

- thông tin game
- thông tin người dùng
- dữ liệu review
- dữ liệu recommendation
- tags
- genres
- categories
- mô tả game và các bảng hỗ trợ khác

Một số đặc điểm nổi bật của dữ liệu:

- dữ liệu đến từ nhiều nguồn, schema chưa hoàn toàn đồng nhất
- khóa nối giữa các bảng chưa thống nhất, ví dụ `app_id` và `appid`
- có cả dữ liệu có cấu trúc và bán cấu trúc
- tồn tại missing values, duplicate records, sai kiểu dữ liệu và outlier
- review có tính đa ngôn ngữ, gây khó khăn cho các bước NLP
- dữ liệu có phân phối long-tail, chỉ một số ít game có rất nhiều tương tác

Những đặc điểm này khiến bài toán phù hợp với hướng xử lý dữ liệu lớn và xây dựng hệ gợi ý.

---

## Bronze Layer - Raw Data và Profiling

Bronze là tầng lưu trữ dữ liệu gốc hoặc dữ liệu chỉ được chuẩn hóa nhẹ.

Mục tiêu chính của Bronze:

- lưu trữ dữ liệu đầu vào từ nhiều nguồn
- giữ gần nguyên trạng dữ liệu gốc để dễ truy vết
- profiling dữ liệu ban đầu
- chuẩn bị dữ liệu cho các bước xử lý sâu hơn ở Silver

Một số công việc chính ở Bronze:

- đọc dữ liệu CSV, JSON và các bảng hỗ trợ từ GCS
- chuyển đổi sang định dạng Parquet để tối ưu xử lý
- tính các chỉ số profiling như:
  - row count
  - null ratio
  - distinct count
  - schema overview
  - phân phối giá trị cơ bản

Bronze giúp nhóm hiểu dữ liệu trước khi thiết kế rule cleaning cho Silver.

---

## Silver Layer - Data Cleaning và Data Quality

Silver là tầng quan trọng nhất trong pipeline. Đây là nơi dữ liệu được làm sạch, chuẩn hóa và kiểm soát chất lượng.

### Mục tiêu của Silver

- chuẩn hóa schema giữa các nguồn dữ liệu
- làm sạch dữ liệu text, số và thời gian
- phát hiện bản ghi lỗi hoặc không hợp lệ
- tách dữ liệu thành **valid data** và **rejected data**
- bổ sung metadata xử lý như `batch_date`, `silver_processed_at`
- tạo nền dữ liệu sạch cho Gold

### Các bảng Silver chính

- `silver_reviews`
- `silver_games`
- `silver_users`
- `silver_game_text`
- `silver_game_tags`
- `silver_game_genres`
- `silver_game_categories`

### Các bảng chất lượng dữ liệu

- `silver_quality_issues`
- `silver_quality_columns`
- `silver_quality_outliers`
- `silver_quality_histograms`
- `silver_quality_overview`

### Các bước xử lý chính ở Silver

- chuẩn hóa khóa dữ liệu như `app_id` và `appid`
- ép kiểu dữ liệu phù hợp cho numeric, boolean và timestamp
- làm sạch text: trim, loại bỏ ký tự bất thường, chuẩn hóa chuỗi
- kiểm tra missing keys, duplicate records và invalid values
- phát hiện outlier để phục vụ phân tích chất lượng dữ liệu
- tách dữ liệu hợp lệ và dữ liệu bị loại sang thư mục `_rejected`
- ghi dữ liệu dưới dạng Parquet và phân vùng theo `batch_date`

### Vai trò của Silver

Silver giúp đảm bảo:

- dashboard phản ánh dữ liệu chính xác hơn
- feature engineering ít bị nhiễu
- dữ liệu đầu vào cho recommendation đáng tin cậy hơn
- có thể so sánh rõ giữa dữ liệu sạch và dữ liệu bị loại

---

## Gold Layer - Analytics và Feature Engineering

Gold là tầng dữ liệu phục vụ trực tiếp cho phân tích, dashboard và recommendation.

Khác với Silver tập trung vào làm sạch, Gold tập trung vào:

- tổng hợp dữ liệu
- tạo insight
- tạo feature cho machine learning
- chuẩn bị dữ liệu training

### Gold cho phân tích và dashboard

Một số bảng Gold định hướng xây dựng:

- `gold_game_kpis`
- `gold_user_kpis`
- `gold_genre_tag_trends`
- `gold_platform_summary`

Các bảng này phục vụ các câu hỏi như:

- game nào có nhiều review nhất
- tỷ lệ recommend theo game là bao nhiêu
- số giờ chơi trung bình theo nhóm game
- tag, genre, category nào phổ biến nhất
- khác biệt giữa game free và paid
- hành vi người dùng theo mức độ hoạt động

### Gold cho feature engineering

Các nhóm feature chính bao gồm:

#### Game features

- tags
- genres
- categories
- text mô tả game đã được làm sạch
- giá
- trạng thái free/paid
- ngày phát hành
- positive ratio
- popularity score

#### User features

- số lượng tương tác
- số review
- số game sở hữu
- mức độ hoạt động
- sở thích thể loại hoặc tag nổi bật
- thống kê hành vi recommendation

#### Interaction features

- user-game interaction
- `is_recommended`
- `hours`
- `helpful`
- `funny`
- `review_length`
- `interaction_weight`

---

## Dữ liệu đa ngôn ngữ và hướng xử lý mở rộng

Một đặc điểm quan trọng của bộ dữ liệu Steam là review có nhiều ngôn ngữ khác nhau.

Vì vậy, trong giai đoạn hiện tại, dự án chưa sử dụng review text thô làm feature lõi cho toàn bộ pipeline Gold để tránh nhiễu do khác biệt ngôn ngữ.

Thay vào đó, dự án ưu tiên:

- sử dụng các tín hiệu hành vi ít phụ thuộc ngôn ngữ như:
  - `is_recommended`
  - `hours`
  - `helpful`
  - `funny`
  - độ dài review
- sử dụng phần mô tả game làm nguồn đặc trưng nội dung chính cho content-based recommendation

### Hướng mở rộng

Ở giai đoạn sau, dự án có thể thử nghiệm các mô hình đa ngôn ngữ để khai thác sâu hơn dữ liệu văn bản, ví dụ:

- multilingual sentence embeddings
- multilingual transformer models
- semantic similarity giữa game và review

Một số bảng mở rộng tiềm năng:

- `gold_game_text_embeddings`
- `gold_review_embeddings`
- `gold_user_embedding_profiles`

Hướng này có thể hỗ trợ tốt hơn cho semantic recommendation trên dữ liệu review đa ngôn ngữ.

---

## Hệ gợi ý Hybrid

Dự án định hướng phát triển hệ gợi ý lai bằng cách kết hợp nhiều loại tín hiệu khác nhau.

### 1. Collaborative Filtering

Dựa trên hành vi tương tác giữa user và game:

- recommendation flag
- hours played
- helpful / funny
- interaction weight

Có thể triển khai thử nghiệm bằng:

- **PySpark ALS**
- hoặc các mô hình collaborative khác trong Colab

### 2. Content-based Recommendation

Dựa trên nội dung và thuộc tính của game:

- tags
- genres
- categories
- mô tả game
- giá và các thuộc tính sản phẩm

Mục tiêu là tìm ra các game tương tự nhau dựa trên nội dung.

### 3. Hybrid Recommendation

Điểm gợi ý cuối cùng được kết hợp từ nhiều thành phần:

```text
final_score = w1 * collaborative_score + w2 * content_score + w3 * popularity_score
```

Cách tiếp cận này giúp:

- tận dụng dữ liệu hành vi người dùng
- giảm vấn đề cold-start
- hỗ trợ game mới hoặc game ít tương tác
- khai thác metadata phong phú của Steam

---

## Dashboard và BI

Dự án sử dụng **BigQuery** và **Looker Studio** để trực quan hóa dữ liệu sau xử lý.

### Mục tiêu dashboard

- theo dõi chất lượng dữ liệu sau cleaning
- so sánh dữ liệu hợp lệ và rejected data
- phân tích phân phối review, game, user, tags, genres, categories
- phục vụ báo cáo tiến độ và minh họa giá trị của pipeline

### Các nhóm dashboard chính

- **Overview**: tổng số game, user, review, số bảng
- **Data Quality**: null ratio, duplicate ratio, outlier metrics, rejected volume
- **Distribution Analysis**: phân phối review, tags, genres, categories
- **Game Analysis**: top game theo review, recommendation, playtime
- **User Analysis**: activity level, số review, hành vi recommendation
- **Comparison Charts**: cleaned vs rejected, free vs paid, tag/genre popularity

---

## Huấn luyện mô hình trên Google Colab

Google Colab được sử dụng trong giai đoạn modeling để:

- đọc dữ liệu từ BigQuery hoặc GCS
- huấn luyện mô hình recommendation
- đánh giá chất lượng gợi ý
- thử nghiệm hybrid recommendation
- mở rộng sang semantic recommendation với multilingual embeddings

### Input modeling

- dữ liệu Silver đã làm sạch
- dữ liệu Gold analytics / features
- training interactions

### Output modeling

- top-N recommendation
- bảng dự đoán recommendation
- feature importance hoặc similarity outputs
- kết quả có thể được lưu lại BigQuery hoặc GCS

---

## Cách chạy pipeline

Ví dụ submit job bằng script:

```bash
bash scripts/submit_bronze.sh
bash scripts/submit_silver.sh
bash scripts/submit_gold.sh
```

Hoặc có thể submit từng Dataproc batch job riêng cho từng layer.

---

## Điểm nổi bật của dự án

- Xây dựng pipeline Big Data trên GCP với PySpark
- Tổ chức dữ liệu theo mô hình Bronze - Silver - Gold
- Có lớp data quality riêng để theo dõi dữ liệu sạch và rejected data
- Kết hợp Data Engineering, BI và Recommendation System
- Có hướng mở rộng sang semantic recommendation với dữ liệu đa ngôn ngữ

---

## Định hướng phát triển tiếp theo

Trong giai đoạn tiếp theo, nhóm tập trung vào:

- hoàn thiện các bảng Gold analytics
- hoàn thiện `gold_game_features`, `gold_user_features`, `gold_training_interactions`
- đẩy dữ liệu Gold lên BigQuery để xây dựng dashboard đầy đủ hơn
- thử nghiệm và đánh giá mô hình hybrid recommendation
- tối ưu trọng số kết hợp giữa collaborative và content-based
- mở rộng sang semantic recommendation bằng multilingual embeddings nếu tài nguyên cho phép

---

## Kết luận

Dự án hướng tới xây dựng một pipeline xử lý dữ liệu lớn tương đối hoàn chỉnh cho bài toán phân tích và gợi ý game trên dữ liệu Steam.

Việc tổ chức pipeline theo mô hình **Bronze - Silver - Gold** giúp dữ liệu được xử lý tuần tự, rõ ràng, dễ mở rộng và dễ giải thích.

- **Silver** đảm bảo dữ liệu sạch, nhất quán và có thể kiểm soát chất lượng
- **Gold** tạo ra giá trị cho phân tích, dashboard và feature engineering
- **Hybrid Recommendation** là hướng mở rộng quan trọng, kết hợp hành vi người dùng, nội dung game và tín hiệu ngữ nghĩa đa ngôn ngữ
