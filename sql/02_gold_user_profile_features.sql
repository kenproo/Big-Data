-- Tạo user profile features từ silver_users
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_profile_features` AS
WITH b AS (
  SELECT user_id,
    SAFE_CAST(products AS FLOAT64) AS products_raw,
    SAFE_CAST(reviews AS FLOAT64) AS reviews_raw,
    SAFE_CAST(products_capped AS FLOAT64) AS products_capped,
    SAFE_CAST(reviews_capped AS FLOAT64) AS reviews_capped,
    batch_date, bronze_ingested_at, source_name, source_file,
    quality_issue, outlier_reasons, IFNULL(is_outlier,FALSE) AS is_outlier, silver_processed_at
  FROM `project-79499e5c-69d7-42b8-864.steam_silver.silver_users`
  WHERE user_id IS NOT NULL
), f AS (
  SELECT *,
    IFNULL(products_capped,IFNULL(products_raw,0)) AS products_owned,
    IFNULL(reviews_capped,IFNULL(reviews_raw,0)) AS reviews_count,
    LOG(1+IFNULL(products_capped,IFNULL(products_raw,0))) AS log_products_owned,
    LOG(1+IFNULL(reviews_capped,IFNULL(reviews_raw,0))) AS log_reviews_count,
    SAFE_DIVIDE(IFNULL(reviews_capped,IFNULL(reviews_raw,0)),NULLIF(IFNULL(products_capped,IFNULL(products_raw,0)),0)) AS review_density
  FROM b
)
SELECT *,
  LEAST(1.0,0.60*SAFE_DIVIDE(log_products_owned,LOG(1+1000))+0.40*SAFE_DIVIDE(log_reviews_count,LOG(1+500))) AS activity_score,
  CASE WHEN products_owned>=5 OR reviews_count>=2 THEN 'old_user' ELSE 'new_user' END AS user_type,
  CASE WHEN products_owned>=100 OR reviews_count>=50 THEN 'high_activity' WHEN products_owned>=20 OR reviews_count>=10 THEN 'medium_activity' WHEN products_owned>=5 OR reviews_count>=2 THEN 'low_activity' ELSE 'cold_start' END AS user_activity_level,
  CASE WHEN reviews_count=0 THEN 'no_review' WHEN reviews_count<2 THEN 'light_reviewer' WHEN reviews_count<10 THEN 'normal_reviewer' ELSE 'active_reviewer' END AS user_reviewer_type,
  IF(products_owned>=5 OR reviews_count>=2,TRUE,FALSE) AS has_enough_signal,
  CASE WHEN is_outlier THEN 0.40 WHEN products_owned<2 THEN 0.30 WHEN reviews_count=0 THEN 0.50 WHEN products_owned<10 THEN 0.70 ELSE LEAST(1.5,0.8+0.1*LOG(1+reviews_count)) END AS user_weight,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM f;
