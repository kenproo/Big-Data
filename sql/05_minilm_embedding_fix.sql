-- Fix bảng game text embedding nếu embedding đang nested dạng game_text_embedding.list.element
SELECT field_path, data_type
FROM `project-79499e5c-69d7-42b8-864.steam_gold.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE table_name='gold_minilm_game_text_embeddings'
ORDER BY field_path;

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed` AS
SELECT
  app_id, embedding_model, embedding_dim,
  ARRAY(SELECT CAST(x.element AS FLOAT64) FROM UNNEST(game_text_embedding.list) AS x) AS embedding,
  game_text_document_len, text_completeness_score, text_quality_group, embedded_at
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings`
WHERE app_id IS NOT NULL;

SELECT COUNT(*) AS total_rows, MIN(ARRAY_LENGTH(embedding)) AS min_dim, MAX(ARRAY_LENGTH(embedding)) AS max_dim, AVG(ARRAY_LENGTH(embedding)) AS avg_dim, COUNTIF(ARRAY_LENGTH(embedding)!=384) AS bad_dim_rows
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed`;

SELECT COUNT(*) AS total_rows, AVG(norm) AS avg_norm, MIN(norm) AS min_norm, MAX(norm) AS max_norm, COUNTIF(norm IS NULL) AS null_norm_rows, COUNTIF(norm=0) AS zero_norm_rows
FROM (SELECT app_id, SQRT((SELECT SUM(v*v) FROM UNNEST(embedding) AS v)) AS norm FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed`);
