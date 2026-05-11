-- Tạo game profile features từ silver_games
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features` AS
WITH base AS (
  SELECT
    app_id,
    COALESCE(NULLIF(TRIM(title),''),NULLIF(TRIM(steam_name),''),NULLIF(TRIM(game_name),'')) AS game_title,
    NULLIF(TRIM(developer),'') AS developer,
    NULLIF(TRIM(publisher),'') AS publisher,
    release_date_final, date_release, steam_release_date,
    IFNULL(win,FALSE) AS win, IFNULL(mac,FALSE) AS mac, IFNULL(linux,FALSE) AS linux,
    rating,
    SAFE_CAST(positive_ratio AS FLOAT64) AS positive_ratio,
    SAFE_CAST(user_reviews_capped AS FLOAT64) AS user_reviews,
    SAFE_CAST(review_count_all_reviews_capped AS FLOAT64) AS review_count_all_reviews,
    SAFE_CAST(positive_review_count AS FLOAT64) AS positive_review_count,
    SAFE_CAST(negative_review_count AS FLOAT64) AS negative_review_count,
    SAFE_CAST(recommendation_event_count_capped AS FLOAT64) AS recommendation_event_count,
    SAFE_CAST(recommended_count AS FLOAT64) AS recommended_count,
    SAFE_CAST(avg_recommendation_hours AS FLOAT64) AS avg_recommendation_hours,
    SAFE_CAST(avg_helpful AS FLOAT64) AS avg_helpful,
    SAFE_CAST(avg_funny AS FLOAT64) AS avg_funny,
    SAFE_CAST(price_final_capped AS FLOAT64) AS price_final,
    SAFE_CAST(price_original_capped AS FLOAT64) AS price_original,
    SAFE_CAST(discount AS FLOAT64) AS discount,
    IFNULL(steam_is_free,FALSE) AS steam_is_free,
    IFNULL(is_free_clean,FALSE) AS is_free_clean,
    IFNULL(steam_deck,FALSE) AS steam_deck,
    quality_issue, outlier_reasons, IFNULL(is_outlier,FALSE) AS is_outlier
  FROM `project-79499e5c-69d7-42b8-864.steam_silver.silver_games`
  WHERE app_id IS NOT NULL AND app_id > 0
), f AS (
  SELECT *,
    IF(win,1,0)+IF(mac,1,0)+IF(linux,1,0) AS platform_count,
    IF(IF(win,1,0)+IF(mac,1,0)+IF(linux,1,0)>=2,TRUE,FALSE) AS is_cross_platform,
    SAFE_DIVIDE(positive_review_count,NULLIF(positive_review_count+negative_review_count,0)) AS positive_review_share,
    LEAST(1.0, SAFE_DIVIDE(LOG(1+IFNULL(review_count_all_reviews,IFNULL(user_reviews,0))), LOG(1+1000000))) AS popularity_score,
    LEAST(1.0, 0.50*SAFE_DIVIDE(LOG(1+IFNULL(avg_recommendation_hours,0)),LOG(1+1000)) + 0.30*SAFE_DIVIDE(LOG(1+IFNULL(avg_helpful,0)),LOG(1+1000)) + 0.20*SAFE_DIVIDE(LOG(1+IFNULL(avg_funny,0)),LOG(1+1000))) AS engagement_score,
    CASE WHEN steam_is_free OR is_free_clean THEN 1.0 WHEN price_final IS NULL THEN 0.50 WHEN price_final <= 0 THEN 1.0 WHEN price_final <= 5 THEN 0.90 WHEN price_final <= 10 THEN 0.75 WHEN price_final <= 20 THEN 0.55 WHEN price_final <= 40 THEN 0.35 ELSE 0.20 END AS base_price_score,
    CASE WHEN release_date_final IS NULL THEN 0.30 ELSE EXP(-GREATEST(DATE_DIFF(CURRENT_DATE(),DATE(release_date_final),DAY),0)/1825.0) END AS recency_score
  FROM base
), s2 AS (
  SELECT *,
    LEAST(1.0,GREATEST(0.0,0.45*IFNULL(SAFE_DIVIDE(positive_ratio,100.0),0.50)+0.35*IFNULL(positive_review_share,0.50)+0.20*CASE WHEN LOWER(IFNULL(rating,'')) LIKE '%overwhelmingly positive%' THEN 1.00 WHEN LOWER(IFNULL(rating,'')) LIKE '%very positive%' THEN 0.90 WHEN LOWER(IFNULL(rating,'')) LIKE '%positive%' THEN 0.75 WHEN LOWER(IFNULL(rating,'')) LIKE '%mixed%' THEN 0.50 WHEN LOWER(IFNULL(rating,'')) LIKE '%negative%' THEN 0.20 ELSE 0.50 END - IF(is_outlier,0.10,0.0))) AS quality_score,
    LEAST(1.0,GREATEST(0.0,base_price_score + CASE WHEN IFNULL(discount,0) >= 50 THEN 0.15 WHEN IFNULL(discount,0) >= 20 THEN 0.08 ELSE 0.0 END)) AS price_attractiveness_score
  FROM f
)
SELECT *,
  LEAST(1.0,GREATEST(0.0,0.35*quality_score+0.25*popularity_score+0.15*engagement_score+0.15*recency_score+0.10*price_attractiveness_score)) AS game_profile_score,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM s2;
