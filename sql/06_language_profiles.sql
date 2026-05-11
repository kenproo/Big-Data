-- Tạo user_language_profile và game_language_profile
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_language_profile` AS
WITH ul AS (SELECT user_id, language, COUNT(*) AS review_count, SUM(CASE WHEN rating>=0.5 THEN 1 ELSE 0 END) AS positive_count, SUM(CASE WHEN rating<0.5 THEN 1 ELSE 0 END) AS negative_count FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_reviews_top4_languages` WHERE user_id IS NOT NULL AND language IN ('english','schinese','spanish','russian') GROUP BY user_id, language),
tu AS (SELECT user_id, SUM(review_count) AS total_reviews FROM ul GROUP BY user_id)
SELECT u.user_id, u.language,
  CASE WHEN u.language='english' THEN 'english_market' WHEN u.language='schinese' THEN 'simplified_chinese_market' WHEN u.language='spanish' THEN 'spanish_market' WHEN u.language='russian' THEN 'russian_market' ELSE 'other_market' END AS language_market,
  u.review_count, u.positive_count, u.negative_count, SAFE_DIVIDE(u.review_count,t.total_reviews) AS user_language_weight,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM ul u JOIN tu t USING(user_id);

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_language_profile` AS
WITH gl AS (SELECT app_id, language, COUNT(*) AS review_count, SUM(CASE WHEN rating>=0.5 THEN 1 ELSE 0 END) AS positive_count, SUM(CASE WHEN rating<0.5 THEN 1 ELSE 0 END) AS negative_count FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_reviews_top4_languages` WHERE app_id IS NOT NULL AND language IN ('english','schinese','spanish','russian') GROUP BY app_id, language),
tg AS (SELECT app_id, SUM(review_count) AS total_reviews FROM gl GROUP BY app_id)
SELECT g.app_id, g.language,
  CASE WHEN g.language='english' THEN 'english_market' WHEN g.language='schinese' THEN 'simplified_chinese_market' WHEN g.language='spanish' THEN 'spanish_market' WHEN g.language='russian' THEN 'russian_market' ELSE 'other_market' END AS language_market,
  g.review_count, g.positive_count, g.negative_count, SAFE_DIVIDE(g.review_count,t.total_reviews) AS game_language_review_share,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM gl g JOIN tg t USING(app_id);
