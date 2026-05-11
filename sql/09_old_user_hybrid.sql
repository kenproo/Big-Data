-- Tạo old user hybrid top30
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_old_user_hybrid_candidates` AS
SELECT a.user_id,a.app_id,a.rank AS als_rank,IFNULL(a.als_score,0.0) AS collaborative_score,
  g.game_title,g.developer,g.publisher,g.genres_text,g.categories_text,g.tags_text,
  IFNULL(c.content_similarity,0.0) AS content_similarity,IFNULL(c.review_content_similarity,0.0) AS review_content_similarity,IFNULL(c.game_text_similarity,0.0) AS game_text_similarity,
  IFNULL(l.language_country_score,0.0) AS language_country_score,
  IFNULL(g.quality_score,0.0) AS quality_score,IFNULL(g.popularity_score,0.0) AS popularity_score,IFNULL(g.recency_score,0.0) AS recency_score,IFNULL(g.price_attractiveness_score,0.0) AS price_score,
  0.30*IFNULL(c.content_similarity,0.0)+0.25*IFNULL(a.als_score,0.0)+0.15*IFNULL(l.language_country_score,0.0)+0.15*IFNULL(g.quality_score,0.0)+0.10*IFNULL(g.popularity_score,0.0)+0.05*IFNULL(g.recency_score,0.0) AS final_score_old,
  CASE WHEN c.review_content_similarity IS NOT NULL AND c.game_text_similarity IS NOT NULL THEN 'ALS + Review MiniLM + Game Text MiniLM + Metadata' WHEN c.review_content_similarity IS NOT NULL THEN 'ALS + Review MiniLM + Metadata' WHEN c.game_text_similarity IS NOT NULL THEN 'ALS + Game Text MiniLM + Metadata' ELSE 'ALS + Metadata' END AS hybrid_reason,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo` a
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_hybrid_features` g USING(app_id)
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_content_similarity` c USING(user_id,app_id)
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_candidate_language_score` l USING(user_id,app_id);

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_old_user_hybrid_top30` AS
WITH r AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY final_score_old DESC) AS hybrid_rank FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_old_user_hybrid_candidates`)
SELECT user_id,'old_user' AS user_type,hybrid_rank,app_id,game_title,final_score_old AS final_score,collaborative_score,content_similarity,review_content_similarity,game_text_similarity,language_country_score,quality_score,popularity_score,recency_score,price_score,hybrid_reason,genres_text,categories_text,tags_text,CURRENT_TIMESTAMP() AS gold_processed_at
FROM r WHERE hybrid_rank<=30;
