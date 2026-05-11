-- Tạo taxonomy features từ gold_game_genres_en, gold_game_categories_en, gold_game_tags_en_only
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_taxonomy_features` AS
WITH valid_games AS (SELECT DISTINCT app_id FROM `project-79499e5c-69d7-42b8-864.steam_silver.silver_games` WHERE app_id IS NOT NULL AND app_id>0),
g AS (SELECT app_id, ARRAY_AGG(DISTINCT LOWER(TRIM(genre)) IGNORE NULLS ORDER BY LOWER(TRIM(genre))) AS genres FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_genres_en` WHERE app_id IN (SELECT app_id FROM valid_games) AND genre IS NOT NULL AND TRIM(genre)!='' GROUP BY app_id),
c AS (SELECT app_id, ARRAY_AGG(DISTINCT LOWER(TRIM(category)) IGNORE NULLS ORDER BY LOWER(TRIM(category))) AS categories FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_categories_en` WHERE app_id IN (SELECT app_id FROM valid_games) AND category IS NOT NULL AND TRIM(category)!='' GROUP BY app_id),
t AS (SELECT app_id, ARRAY_AGG(DISTINCT LOWER(TRIM(tag)) IGNORE NULLS ORDER BY LOWER(TRIM(tag)) LIMIT 50) AS tags FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_tags_en_only` WHERE app_id IN (SELECT app_id FROM valid_games) AND tag IS NOT NULL AND TRIM(tag)!='' AND REGEXP_CONTAINS(TRIM(tag), r'^[A-Za-z0-9 /+\-:&,.()''!?#]+$') GROUP BY app_id),
j AS (SELECT v.app_id, IFNULL(g.genres,[]) AS genres, IFNULL(c.categories,[]) AS categories, IFNULL(t.tags,[]) AS tags FROM valid_games v LEFT JOIN g USING(app_id) LEFT JOIN c USING(app_id) LEFT JOIN t USING(app_id))
SELECT app_id, genres, categories, tags,
  ARRAY_LENGTH(genres) AS genre_count, ARRAY_LENGTH(categories) AS category_count, ARRAY_LENGTH(tags) AS tag_count,
  ARRAY_TO_STRING(genres,', ') AS genres_text, ARRAY_TO_STRING(categories,', ') AS categories_text, ARRAY_TO_STRING(tags,', ') AS tags_text,
  ARRAY_TO_STRING(ARRAY_CONCAT(IF(ARRAY_LENGTH(genres)>0,[CONCAT('[GENRES] ',ARRAY_TO_STRING(genres,', '))],[]),IF(ARRAY_LENGTH(categories)>0,[CONCAT('[CATEGORIES] ',ARRAY_TO_STRING(categories,', '))],[]),IF(ARRAY_LENGTH(tags)>0,[CONCAT('[TAGS] ',ARRAY_TO_STRING(tags,', '))],[])),' ') AS taxonomy_document,
  (IF(ARRAY_LENGTH(genres)>0,1.0,0.0)+IF(ARRAY_LENGTH(categories)>0,1.0,0.0)+IF(ARRAY_LENGTH(tags)>0,1.0,0.0))/3.0 AS taxonomy_completeness_score,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM j;
