-- Tạo language score và content similarity cho ALS candidates
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score` AS
SELECT a.user_id, a.app_id, SUM(IFNULL(u.user_language_weight,0)*IFNULL(g.game_language_review_share,0)) AS language_country_score, CURRENT_TIMESTAMP() AS gold_processed_at
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo` a
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_language_profile` u ON a.user_id=u.user_id
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_language_profile` g ON a.app_id=g.app_id AND u.language=g.language
GROUP BY a.user_id, a.app_id;

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_content_similarity` AS
WITH b AS (
  SELECT a.user_id,a.app_id,ug.user_game_text_preference_embedding,ur.user_review_preference_embedding,g.game_text_embedding,g.review_embedding
  FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo` a
  LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_preference_game_text_embedding` ug USING(user_id)
  LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_preference_review_embedding` ur USING(user_id)
  LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_hybrid_features` g USING(app_id)
), gt AS (
  SELECT user_id, app_id, SUM(u_val*g_val) AS game_text_similarity
  FROM b, UNNEST(user_game_text_preference_embedding) AS u_val WITH OFFSET p1 JOIN UNNEST(game_text_embedding) AS g_val WITH OFFSET p2 ON p1=p2
  WHERE user_game_text_preference_embedding IS NOT NULL AND game_text_embedding IS NOT NULL GROUP BY user_id, app_id
), rs AS (
  SELECT user_id, app_id, SUM(u_val*g_val) AS review_content_similarity
  FROM b, UNNEST(user_review_preference_embedding) AS u_val WITH OFFSET p1 JOIN UNNEST(review_embedding) AS g_val WITH OFFSET p2 ON p1=p2
  WHERE user_review_preference_embedding IS NOT NULL AND review_embedding IS NOT NULL GROUP BY user_id, app_id
)
SELECT b.user_id,b.app_id,gt.game_text_similarity,rs.review_content_similarity,
  CASE WHEN rs.review_content_similarity IS NOT NULL AND gt.game_text_similarity IS NOT NULL THEN 0.65*rs.review_content_similarity+0.35*gt.game_text_similarity WHEN rs.review_content_similarity IS NOT NULL THEN rs.review_content_similarity WHEN gt.game_text_similarity IS NOT NULL THEN gt.game_text_similarity ELSE 0.0 END AS content_similarity,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM b LEFT JOIN gt USING(user_id,app_id) LEFT JOIN rs USING(user_id,app_id);
