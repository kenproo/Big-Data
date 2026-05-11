-- Tạo review embedding cấp game, game_hybrid_features và user preference embeddings
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_review_embedding_game_features` AS
WITH e AS (SELECT app_id, pos AS dim_index, CAST(value AS FLOAT64) AS value FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_review_chunk_embeddings`, UNNEST(embedding) AS value WITH OFFSET pos WHERE embedding IS NOT NULL AND ARRAY_LENGTH(embedding)=384),
a AS (SELECT app_id, dim_index, AVG(value) AS avg_value FROM e GROUP BY app_id, dim_index),
n AS (SELECT app_id, SQRT(SUM(avg_value*avg_value)) AS norm FROM a GROUP BY app_id)
SELECT a.app_id, 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2' AS embedding_model, 384 AS embedding_dim, ARRAY_AGG(SAFE_DIVIDE(a.avg_value,NULLIF(n.norm,0)) ORDER BY dim_index) AS review_embedding, CURRENT_TIMESTAMP() AS gold_processed_at
FROM a JOIN n USING(app_id) GROUP BY a.app_id;

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_hybrid_features` AS
SELECT gp.app_id, gp.game_title, gp.developer, gp.publisher, gp.release_date_final,
  gp.quality_score, gp.popularity_score, gp.engagement_score, gp.recency_score, gp.price_attractiveness_score, gp.game_profile_score,
  tx.genres, tx.categories, tx.tags, tx.genres_text, tx.categories_text, tx.tags_text, tx.taxonomy_document, tx.taxonomy_completeness_score,
  gt.summary_final, gt.about_final, gt.extensive_final, gt.game_text_document, gt.game_text_document_len, gt.text_completeness_score, gt.text_quality_group,
  IF(gt.game_text_document IS NOT NULL,TRUE,FALSE) AS has_game_text,
  IF(tx.taxonomy_document IS NOT NULL AND TRIM(tx.taxonomy_document)!='',TRUE,FALSE) AS has_taxonomy,
  IF(ge.embedding IS NOT NULL,TRUE,FALSE) AS has_game_text_embedding,
  IF(re.review_embedding IS NOT NULL,TRUE,FALSE) AS has_review_embedding,
  ge.embedding AS game_text_embedding, re.review_embedding,
  CASE WHEN ge.embedding IS NOT NULL AND re.review_embedding IS NOT NULL THEN 'both_game_text_and_review' WHEN ge.embedding IS NOT NULL THEN 'game_text_only' WHEN re.review_embedding IS NOT NULL THEN 'review_only' ELSE 'no_embedding' END AS embedding_coverage_group,
  LEAST(1.0,GREATEST(0.0,0.30*IF(gt.game_text_document IS NOT NULL,1.0,0.0)+0.25*IF(tx.taxonomy_document IS NOT NULL AND TRIM(tx.taxonomy_document)!='',1.0,0.0)+0.25*IF(ge.embedding IS NOT NULL,1.0,0.0)+0.20*IF(re.review_embedding IS NOT NULL,1.0,0.0))) AS content_readiness_score,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features` gp
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_taxonomy_features` tx USING(app_id)
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_text_features` gt USING(app_id)
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed` ge USING(app_id)
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_review_embedding_game_features` re USING(app_id);

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_preference_game_text_embedding` AS
WITH p AS (SELECT user_id, app_id, 1.0+0.15*LOG(1+IFNULL(hours,0))+0.10*LOG(1+IFNULL(helpful_score,0)) AS interaction_weight FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_reviews_top4_languages` WHERE user_id IS NOT NULL AND app_id IS NOT NULL AND rating>=0.5),
e AS (SELECT p.user_id, pos AS dim_index, CAST(value AS FLOAT64)*interaction_weight AS weighted_value, interaction_weight FROM p JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed` ge USING(app_id), UNNEST(ge.embedding) AS value WITH OFFSET pos WHERE ge.embedding IS NOT NULL AND ARRAY_LENGTH(ge.embedding)=384),
a AS (SELECT user_id, dim_index, SAFE_DIVIDE(SUM(weighted_value),SUM(interaction_weight)) AS avg_value FROM e GROUP BY user_id, dim_index),
n AS (SELECT user_id, SQRT(SUM(avg_value*avg_value)) AS norm FROM a GROUP BY user_id)
SELECT a.user_id, 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2' AS embedding_model, 384 AS embedding_dim, ARRAY_AGG(SAFE_DIVIDE(a.avg_value,NULLIF(n.norm,0)) ORDER BY dim_index) AS user_game_text_preference_embedding, CURRENT_TIMESTAMP() AS gold_processed_at
FROM a JOIN n USING(user_id) GROUP BY a.user_id;

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_user_preference_review_embedding` AS
WITH p AS (SELECT user_id, app_id, 1.0+0.15*LOG(1+IFNULL(hours,0))+0.10*LOG(1+IFNULL(helpful_score,0)) AS interaction_weight FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_reviews_top4_languages` WHERE user_id IS NOT NULL AND app_id IS NOT NULL AND rating>=0.5),
e AS (SELECT p.user_id, pos AS dim_index, CAST(value AS FLOAT64)*interaction_weight AS weighted_value, interaction_weight FROM p JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_review_embedding_game_features` re USING(app_id), UNNEST(re.review_embedding) AS value WITH OFFSET pos WHERE re.review_embedding IS NOT NULL AND ARRAY_LENGTH(re.review_embedding)=384),
a AS (SELECT user_id, dim_index, SAFE_DIVIDE(SUM(weighted_value),SUM(interaction_weight)) AS avg_value FROM e GROUP BY user_id, dim_index),
n AS (SELECT user_id, SQRT(SUM(avg_value*avg_value)) AS norm FROM a GROUP BY user_id)
SELECT a.user_id, 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2' AS embedding_model, 384 AS embedding_dim, ARRAY_AGG(SAFE_DIVIDE(a.avg_value,NULLIF(n.norm,0)) ORDER BY dim_index) AS user_review_preference_embedding, CURRENT_TIMESTAMP() AS gold_processed_at
FROM a JOIN n USING(user_id) GROUP BY a.user_id;
