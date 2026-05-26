
  CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_labels` AS
SELECT
  user_id,
  app_id,
  1 AS label
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_train_split`
WHERE split = 'test'
  AND rating > 0
GROUP BY user_id, app_id;

CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidates_labeled` AS
WITH candidates_ranked AS (
  SELECT
    SAFE_CAST(user_id AS INT64) AS user_id,
    SAFE_CAST(app_id AS INT64) AS app_id,
    user_idx,
    app_idx,
    als_score,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY als_score DESC
    ) AS als_rank
  FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidates_mapped`
)

SELECT
  c.user_id,
  c.app_id,
  c.user_idx,
  c.app_idx,
  c.als_score,
  c.als_rank,
  IF(l.label IS NULL, 0, 1) AS label
FROM candidates_ranked c
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_labels` l
  ON c.user_id = l.user_id
 AND c.app_id = l.app_id;


 CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_train_features_v1` AS
SELECT
  c.user_id,
  c.app_id,

  -- Label
  c.label,

  -- ALS features
  c.als_score,
  c.als_rank,

  -- Index từ ALS
  c.user_idx,
  c.app_idx,

  -- User features
  u.* EXCEPT(user_id),

  -- Game features
  g.* EXCEPT(app_id),

  -- Language score
  lang.* EXCEPT(user_id, app_id)

FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidates_labeled` c

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_profile_features` u
  ON c.user_id = u.user_id

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features` g
  ON c.app_id = g.app_id

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score` lang
  ON c.user_id = lang.user_id
 AND c.app_id = lang.app_id;


 CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score_full` AS
SELECT
  c.user_id,
  c.app_id,

  COALESCE(l.language_score, 0) AS language_score,
  COALESCE(l.has_language_match, 0) AS has_language_match,
  COALESCE(l.matched_language_count, 0) AS matched_language_count,

  COALESCE(l.matched_user_language_review_count, 0) AS matched_user_language_review_count,
  COALESCE(l.matched_game_language_review_count, 0) AS matched_game_language_review_count,

  l.top_matched_language,
  l.top_matched_language_market,

  COALESCE(l.max_single_language_match_score, 0) AS max_single_language_match_score

FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidates_labeled` c

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score` l
  ON c.user_id = l.user_id
 AND c.app_id = l.app_id;


 CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_train_features_v1` AS
SELECT
  c.user_id,
  c.app_id,

  -- Label
  c.label,

  -- ALS features
  c.als_score,
  c.als_rank,
  c.user_idx,
  c.app_idx,

  -- User profile features
  u.* EXCEPT(user_id),

  -- Game profile features
  g.* EXCEPT(app_id),

  -- Language features
  lang.language_score,
  lang.has_language_match,
  lang.matched_language_count,
  lang.matched_user_language_review_count,
  lang.matched_game_language_review_count,
  lang.max_single_language_match_score

FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidates_labeled` c

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_profile_features` u
  ON c.user_id = u.user_id

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features` g
  ON c.app_id = g.app_id

LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score_full` lang
  ON c.user_id = lang.user_id
 AND c.app_id = lang.app_id;


 CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_train_features_v1`
PARTITION BY batch_date
CLUSTER BY user_id, app_id
AS

WITH als_candidates AS (
  SELECT
    c.user_id,
    c.app_id,
    c.user_idx,
    c.app_idx,
    c.als_score,

    ROW_NUMBER() OVER (
      PARTITION BY c.user_id
      ORDER BY c.als_score DESC
    ) AS als_rank,

    c.label
  FROM
    `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidates_labeled` c
),

user_features AS (
  SELECT
    user_id,

    products_raw,
    reviews_raw,
    products_owned,
    reviews_count,
    products_log,
    reviews_log,
    review_density,
    activity_score,

    user_activity_level,
    user_reviewer_type,
    has_enough_signal,
    user_type,

    user_weight,
    is_outlier

  FROM
    `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_profile_features`
),

game_features AS (
  SELECT
    app_id,

    game_title,
    title,
    steam_name,
    game_name,

    days_since_release,

    win,
    mac,
    linux,
    platform_count,
    is_cross_platform,
    platform_score,

    steam_deck,
    steam_deck_score,

    rating,
    positive_ratio,
    positive_review_share,

    user_reviews,
    user_reviews_final,
    review_count_all_reviews,
    review_count_final,
    positive_review_count,
    negative_review_count,

    recommendation_event_count,
    recommendation_event_count_final,
    avg_recommendation_hours,
    avg_helpful,
    avg_funny,
    recommended_count,

    price_final,
    price_original,
    price_final_clean,
    price_original_clean,
    discount,
    is_free,
    price_group,

    quality_score,
    popularity_score,
    engagement_score,
    price_attractiveness_score,
    recency_score,

    quality_group,
    popularity_group,
    recency_group,

    game_profile_score,

    developer

  FROM
    `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features`
),

language_features AS (
  SELECT
    user_id,
    app_id,

    language_score,
    has_language_match,
    matched_language_count,
    matched_user_review_count,
    matched_game_review_count,
    top_matched_language,
    top_matched_language_market,
    max_single_language_match_score

  FROM
    `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score_full`
)

SELECT
  -- =========================
  -- Keys
  -- =========================
  a.user_id,
  a.app_id,
  a.user_idx,
  a.app_idx,

  -- =========================
  -- Label
  -- =========================
  CAST(a.label AS INT64) AS label,

  -- =========================
  -- ALS candidate features
  -- =========================
  CAST(a.als_score AS FLOAT64) AS als_score,
  CAST(a.als_rank AS INT64) AS als_rank,

  -- score phụ để LightGBM dễ học hơn
  SAFE_DIVIDE(1.0, a.als_rank) AS als_inverse_rank,
  LOG(1 + a.als_rank) AS als_rank_log,

  -- =========================
  -- User profile features
  -- =========================
  COALESCE(u.products_raw, 0.0) AS user_products_raw,
  COALESCE(u.reviews_raw, 0.0) AS user_reviews_raw,
  COALESCE(u.products_owned, 0.0) AS user_products_owned,
  COALESCE(u.reviews_count, 0.0) AS user_reviews_count,

  COALESCE(u.products_log, 0.0) AS user_products_log,
  COALESCE(u.reviews_log, 0.0) AS user_reviews_log,
  COALESCE(u.review_density, 0.0) AS user_review_density,
  COALESCE(u.activity_score, 0.0) AS user_activity_score,

  COALESCE(u.user_weight, 1.0) AS user_weight,
  COALESCE(u.has_enough_signal, FALSE) AS user_has_enough_signal,
  COALESCE(u.is_outlier, FALSE) AS user_is_outlier,

  COALESCE(u.user_activity_level, 'unknown') AS user_activity_level,
  COALESCE(u.user_reviewer_type, 'unknown') AS user_reviewer_type,
  COALESCE(u.user_type, 'unknown') AS user_type,

  -- =========================
  -- Game metadata
  -- =========================
  COALESCE(g.game_title, g.title, g.steam_name, g.game_name) AS game_title,
  COALESCE(g.developer, 'unknown') AS developer,

  -- =========================
  -- Game platform features
  -- =========================
  COALESCE(g.days_since_release, 0) AS days_since_release,

  COALESCE(g.win, FALSE) AS game_win,
  COALESCE(g.mac, FALSE) AS game_mac,
  COALESCE(g.linux, FALSE) AS game_linux,
  COALESCE(g.platform_count, 0) AS platform_count,
  COALESCE(g.is_cross_platform, FALSE) AS is_cross_platform,
  COALESCE(g.platform_score, 0.0) AS platform_score,

  COALESCE(g.steam_deck, FALSE) AS steam_deck,
  COALESCE(g.steam_deck_score, 0.0) AS steam_deck_score,

  -- =========================
  -- Game review / popularity features
  -- =========================
  COALESCE(g.rating, 0.0) AS game_rating,
  COALESCE(g.positive_ratio, 0.0) AS positive_ratio,
  COALESCE(g.positive_review_share, 0.0) AS positive_review_share,

  COALESCE(g.user_reviews, 0.0) AS user_reviews,
  COALESCE(g.user_reviews_final, 0.0) AS user_reviews_final,
  COALESCE(g.review_count_all_reviews, 0.0) AS review_count_all_reviews,
  COALESCE(g.review_count_final, 0.0) AS review_count_final,

  COALESCE(g.positive_review_count, 0.0) AS positive_review_count,
  COALESCE(g.negative_review_count, 0.0) AS negative_review_count,

  COALESCE(g.recommendation_event_count, 0.0) AS recommendation_event_count,
  COALESCE(g.recommendation_event_count_final, 0.0) AS recommendation_event_count_final,
  COALESCE(g.avg_recommendation_hours, 0.0) AS avg_recommendation_hours,
  COALESCE(g.avg_helpful, 0.0) AS avg_helpful,
  COALESCE(g.avg_funny, 0.0) AS avg_funny,
  COALESCE(g.recommended_count, 0.0) AS recommended_count,

  -- =========================
  -- Game price features
  -- =========================
  COALESCE(g.price_final, 0.0) AS price_final,
  COALESCE(g.price_original, 0.0) AS price_original,
  COALESCE(g.price_final_clean, 0.0) AS price_final_clean,
  COALESCE(g.price_original_clean, 0.0) AS price_original_clean,
  COALESCE(g.discount, 0.0) AS discount,
  COALESCE(g.is_free, FALSE) AS is_free,
  COALESCE(g.price_group, 'unknown') AS price_group,

  -- =========================
  -- Game aggregated scores
  -- =========================
  COALESCE(g.quality_score, 0.0) AS quality_score,
  COALESCE(g.popularity_score, 0.0) AS popularity_score,
  COALESCE(g.engagement_score, 0.0) AS engagement_score,
  COALESCE(g.price_attractiveness_score, 0.0) AS price_attractiveness_score,
  COALESCE(g.recency_score, 0.0) AS recency_score,
  COALESCE(g.game_profile_score, 0.0) AS game_profile_score,

  COALESCE(g.quality_group, 'unknown') AS quality_group,
  COALESCE(g.popularity_group, 'unknown') AS popularity_group,
  COALESCE(g.recency_group, 'unknown') AS recency_group,

  -- =========================
  -- Language matching features
  -- =========================
  COALESCE(l.language_score, 0.0) AS language_score,
  COALESCE(l.has_language_match, FALSE) AS has_language_match,
  COALESCE(l.matched_language_count, 0) AS matched_language_count,
  COALESCE(l.matched_user_review_count, 0) AS matched_user_review_count,
  COALESCE(l.matched_game_review_count, 0) AS matched_game_review_count,
  COALESCE(l.top_matched_language, 'unknown') AS top_matched_language,
  COALESCE(l.top_matched_language_market, 'unknown') AS top_matched_language_market,
  COALESCE(l.max_single_language_match_score, 0.0) AS max_single_language_match_score,

  -- =========================
  -- Extra interaction features
  -- =========================
  COALESCE(a.als_score, 0.0) * COALESCE(l.language_score, 0.0) AS als_x_language_score,
  COALESCE(a.als_score, 0.0) * COALESCE(g.popularity_score, 0.0) AS als_x_popularity_score,
  COALESCE(a.als_score, 0.0) * COALESCE(g.quality_score, 0.0) AS als_x_quality_score,
  COALESCE(u.activity_score, 0.0) * COALESCE(g.engagement_score, 0.0) AS user_activity_x_game_engagement,

  -- =========================
  -- Processing info
  -- =========================
  CURRENT_DATE() AS batch_date,
  CURRENT_TIMESTAMP() AS gold_processed_at

FROM als_candidates a
LEFT JOIN user_features u
  ON a.user_id = u.user_id
LEFT JOIN game_features g
  ON a.app_id = g.app_id
LEFT JOIN language_features l
  ON a.user_id = l.user_id
 AND a.app_id = l.app_id;


 CREATE OR REPLACE TABLE
`project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_train_features_v1_sample`
AS

WITH positives AS (
  SELECT *
  FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_train_features_v1`
  WHERE label = 1
),

negatives AS (
  SELECT *
  FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_lgbm_train_features_v1`
  WHERE label = 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id
    ORDER BY RAND()
  ) <= 5
)

SELECT * FROM positives
UNION ALL
SELECT * FROM negatives;


