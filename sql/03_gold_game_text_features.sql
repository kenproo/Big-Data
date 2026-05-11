-- Làm sạch game text để tạo game_text_document cho MiniLM
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_text_features` AS
WITH valid_games AS (
  SELECT DISTINCT app_id FROM `project-79499e5c-69d7-42b8-864.steam_silver.silver_games` WHERE app_id IS NOT NULL AND app_id > 0
), r AS (
  SELECT t.* FROM `project-79499e5c-69d7-42b8-864.steam_silver.silver_game_text` t JOIN valid_games g USING(app_id)
), c AS (
  SELECT *,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(TRIM(COALESCE(summary_clean,summary,''))),'&amp;','&'),'&quot;','"'),'&#39;',"'"),'&nbsp;',' '),'&lt;','<'),'&gt;','>'),r'<[^>]+>',' '),r'https?://\S+|www\.\S+',' '),r'\\n|\\r|\\t',' '),r'[\x00-\x1F\x7F]',' '),r'\\',' ') AS summary_tmp,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(TRIM(COALESCE(about_clean,about,''))),'&amp;','&'),'&quot;','"'),'&#39;',"'"),'&nbsp;',' '),'&lt;','<'),'&gt;','>'),r'<[^>]+>',' '),r'https?://\S+|www\.\S+',' '),r'\\n|\\r|\\t',' '),r'[\x00-\x1F\x7F]',' '),r'\\',' ') AS about_tmp,
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(TRIM(COALESCE(extensive_clean,extensive,''))),'&amp;','&'),'&quot;','"'),'&#39;',"'"),'&nbsp;',' '),'&lt;','<'),'&gt;','>'),r'<[^>]+>',' '),r'https?://\S+|www\.\S+',' '),r'\\n|\\r|\\t',' '),r'[\x00-\x1F\x7F]',' '),r'\\',' ') AS extensive_tmp
  FROM r
), n AS (
  SELECT *,
    NULLIF(REGEXP_REPLACE(TRIM(summary_tmp),r'\s+',' '),'') AS summary_norm,
    NULLIF(REGEXP_REPLACE(TRIM(about_tmp),r'\s+',' '),'') AS about_norm,
    NULLIF(REGEXP_REPLACE(TRIM(extensive_tmp),r'\s+',' '),'') AS extensive_norm
  FROM c
), f AS (
  SELECT *,
    CASE WHEN summary_norm IS NULL OR summary_norm IN ('n','null','none','nan') OR LENGTH(summary_norm)<20 THEN NULL ELSE summary_norm END AS summary_final,
    CASE WHEN about_norm IS NULL OR about_norm IN ('n','null','none','nan') OR LENGTH(about_norm)<30 THEN NULL ELSE about_norm END AS about_final,
    CASE WHEN extensive_norm IS NULL OR extensive_norm IN ('n','null','none','nan') OR LENGTH(extensive_norm)<30 THEN NULL ELSE extensive_norm END AS extensive_final
  FROM n
), d AS (
  SELECT *,
    ARRAY_TO_STRING(ARRAY_CONCAT(IF(summary_final IS NOT NULL,[CONCAT('[SUMMARY] ',summary_final)],[]),IF(about_final IS NOT NULL,[CONCAT('[ABOUT] ',about_final)],[]),IF(extensive_final IS NOT NULL,[CONCAT('[EXTENSIVE] ',extensive_final)],[])),' ') AS game_text_document
  FROM f
)
SELECT
  app_id, summary_final, about_final, extensive_final,
  LENGTH(IFNULL(summary_final,'')) AS summary_final_len,
  LENGTH(IFNULL(about_final,'')) AS about_final_len,
  LENGTH(IFNULL(extensive_final,'')) AS extensive_final_len,
  IF(summary_final IS NOT NULL,TRUE,FALSE) AS has_useful_summary,
  IF(about_final IS NOT NULL,TRUE,FALSE) AS has_useful_about,
  IF(extensive_final IS NOT NULL,TRUE,FALSE) AS has_useful_extensive,
  LENGTH(IFNULL(summary_final,''))+LENGTH(IFNULL(about_final,''))+LENGTH(IFNULL(extensive_final,'')) AS actual_content_len,
  (IF(summary_final IS NOT NULL,1.0,0.0)+IF(about_final IS NOT NULL,1.0,0.0)+IF(extensive_final IS NOT NULL,1.0,0.0))/3.0 AS text_completeness_score,
  CASE WHEN LENGTH(IFNULL(summary_final,''))+LENGTH(IFNULL(about_final,''))+LENGTH(IFNULL(extensive_final,''))>=1000 THEN 'rich_text' WHEN LENGTH(IFNULL(summary_final,''))+LENGTH(IFNULL(about_final,''))+LENGTH(IFNULL(extensive_final,''))>=300 THEN 'medium_text' WHEN LENGTH(IFNULL(summary_final,''))+LENGTH(IFNULL(about_final,''))+LENGTH(IFNULL(extensive_final,''))>=80 THEN 'short_text' ELSE 'weak_or_empty_text' END AS text_quality_group,
  NULLIF(TRIM(game_text_document),'') AS game_text_document,
  LENGTH(NULLIF(TRIM(game_text_document),'')) AS game_text_document_len,
  quality_issue, outlier_reasons, IFNULL(is_outlier,FALSE) AS is_outlier, silver_processed_at,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM d
WHERE NULLIF(TRIM(game_text_document),'') IS NOT NULL;
