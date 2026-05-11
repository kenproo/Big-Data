-- Tạo old user homepage sections và demo final
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_old_user_homepage_sections` AS
WITH src AS (SELECT * FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_old_user_hybrid_top30`), sec AS (
  SELECT *,'Recommended For You' AS section_name, final_score AS section_score FROM src
  UNION ALL SELECT *,'Because Of Your Play History',content_similarity FROM src WHERE content_similarity>0
  UNION ALL SELECT *,'Community Review Match',review_content_similarity FROM src WHERE review_content_similarity>0
  UNION ALL SELECT *,'Popular In Your Taste Area',0.50*popularity_score+0.50*final_score FROM src WHERE popularity_score>0
  UNION ALL SELECT *,'Fresh Picks For You',0.50*recency_score+0.50*final_score FROM src WHERE recency_score>0
), r AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id,section_name ORDER BY section_score DESC) AS item_rank FROM sec)
SELECT CAST(user_id AS STRING) AS demo_user_id,user_type,'learned_from_history' AS preferred_language,section_name,
  CASE WHEN section_name='Recommended For You' THEN 1 WHEN section_name='Because Of Your Play History' THEN 2 WHEN section_name='Community Review Match' THEN 3 WHEN section_name='Popular In Your Taste Area' THEN 4 WHEN section_name='Fresh Picks For You' THEN 5 ELSE 99 END AS section_rank,
  item_rank,app_id,game_title,section_score AS final_score,content_similarity,review_content_similarity,game_text_similarity,language_country_score,quality_score,popularity_score,recency_score,hybrid_reason AS reason,genres_text,categories_text,tags_text,CURRENT_TIMESTAMP() AS gold_processed_at
FROM r WHERE item_rank<=30;

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.demo_steam_homepage_final` AS
SELECT demo_user_id,user_type,preferred_language,section_name,section_rank,item_rank,app_id,game_title,final_score,reason,genres_text,categories_text,tags_text,CURRENT_TIMESTAMP() AS demo_created_at FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_new_user_homepage_sections`
UNION ALL
SELECT demo_user_id,user_type,preferred_language,section_name,section_rank,item_rank,app_id,game_title,final_score,reason,genres_text,categories_text,tags_text,CURRENT_TIMESTAMP() AS demo_created_at FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_old_user_homepage_sections`;

SELECT user_type,COUNT(*) AS total_rows,COUNT(DISTINCT demo_user_id) AS user_count,COUNT(DISTINCT section_name) AS section_count,COUNT(DISTINCT app_id) AS unique_games
FROM `project-79499e5c-69d7-42b8-864.steam_gold.demo_steam_homepage_final` GROUP BY user_type;

EXPORT DATA OPTIONS (uri='gs://truong_bigdata_24032026_init/demo/demo_steam_homepage_final_*.csv', format='CSV', overwrite=true, header=true, field_delimiter=',') AS
SELECT * FROM `project-79499e5c-69d7-42b8-864.steam_gold.demo_steam_homepage_final`;
