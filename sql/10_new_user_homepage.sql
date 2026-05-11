-- Tạo new user cold-start homepage sections
CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_new_user_homepage_candidates` AS
WITH du AS (SELECT 'new_user_english' AS demo_user_id,'english' AS preferred_language,'english_market' AS preferred_market UNION ALL SELECT 'new_user_schinese','schinese','simplified_chinese_market' UNION ALL SELECT 'new_user_spanish','spanish','spanish_market' UNION ALL SELECT 'new_user_russian','russian','russian_market')
SELECT du.demo_user_id,'new_user' AS user_type,du.preferred_language,du.preferred_market,g.app_id,g.game_title,g.developer,g.publisher,g.genres_text,g.categories_text,g.tags_text,
  IFNULL(gl.game_language_review_share,0.0) AS language_country_score,IFNULL(g.quality_score,0.0) AS quality_score,IFNULL(g.popularity_score,0.0) AS popularity_score,IFNULL(g.recency_score,0.0) AS recency_score,IFNULL(g.price_attractiveness_score,0.0) AS price_score,
  0.30*IFNULL(gl.game_language_review_share,0.0)+0.25*IFNULL(g.quality_score,0.0)+0.25*IFNULL(g.popularity_score,0.0)+0.10*IFNULL(g.recency_score,0.0)+0.10*IFNULL(g.price_attractiveness_score,0.0) AS final_score_new,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM du CROSS JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_hybrid_features` g
LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_language_profile` gl ON g.app_id=gl.app_id AND du.preferred_language=gl.language
WHERE g.game_title IS NOT NULL AND g.quality_score IS NOT NULL AND g.popularity_score IS NOT NULL;

CREATE OR REPLACE TABLE `project-79499e5c-69d7-42b8-864.steam_gold.gold_new_user_homepage_sections` AS
WITH s AS (SELECT * FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_new_user_homepage_candidates`), sec AS (
  SELECT *,'Popular & Highly Rated' AS section_name, final_score_new AS section_score FROM s WHERE quality_score>=0.65 AND popularity_score>=0.50
  UNION ALL SELECT *,'New & Trending',0.50*recency_score+0.50*popularity_score FROM s WHERE recency_score>=0.50
  UNION ALL SELECT *,'Free / Budget Friendly',0.50*price_score+0.30*quality_score+0.20*popularity_score FROM s WHERE price_score>=0.60
  UNION ALL SELECT *,'Top Indie Games',0.40*quality_score+0.30*popularity_score+0.30*final_score_new FROM s WHERE LOWER(IFNULL(genres_text,'')) LIKE '%indie%'
  UNION ALL SELECT *,'Action & Adventure',0.35*quality_score+0.35*popularity_score+0.30*final_score_new FROM s WHERE LOWER(IFNULL(genres_text,'')) LIKE '%action%' OR LOWER(IFNULL(genres_text,'')) LIKE '%adventure%'
  UNION ALL SELECT *,'Strategy & Simulation',0.35*quality_score+0.35*popularity_score+0.30*final_score_new FROM s WHERE LOWER(IFNULL(genres_text,'')) LIKE '%strategy%' OR LOWER(IFNULL(genres_text,'')) LIKE '%simulation%'
  UNION ALL SELECT *,'Language Market Picks',0.50*language_country_score+0.25*quality_score+0.25*popularity_score FROM s WHERE language_country_score>0
), r AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY demo_user_id,section_name ORDER BY section_score DESC) AS item_rank FROM sec)
SELECT demo_user_id,user_type,preferred_language,section_name,
  CASE WHEN section_name='Popular & Highly Rated' THEN 1 WHEN section_name='New & Trending' THEN 2 WHEN section_name='Free / Budget Friendly' THEN 3 WHEN section_name='Top Indie Games' THEN 4 WHEN section_name='Action & Adventure' THEN 5 WHEN section_name='Strategy & Simulation' THEN 6 WHEN section_name='Language Market Picks' THEN 7 ELSE 99 END AS section_rank,
  item_rank,app_id,game_title,section_score AS final_score,language_country_score,quality_score,popularity_score,recency_score,price_score,
  CASE WHEN section_name='Popular & Highly Rated' THEN 'High quality and popular among Steam users' WHEN section_name='New & Trending' THEN 'Recently released or currently trending' WHEN section_name='Free / Budget Friendly' THEN 'Good price attractiveness for cold-start users' WHEN section_name='Top Indie Games' THEN 'Indie game matched by taxonomy and quality signals' WHEN section_name='Action & Adventure' THEN 'Action/adventure genre match' WHEN section_name='Strategy & Simulation' THEN 'Strategy/simulation genre match' WHEN section_name='Language Market Picks' THEN 'Strong match with preferred language market' ELSE 'Hybrid recommendation' END AS reason,
  genres_text,categories_text,tags_text,CURRENT_TIMESTAMP() AS gold_processed_at
FROM r WHERE item_rank<=30;
