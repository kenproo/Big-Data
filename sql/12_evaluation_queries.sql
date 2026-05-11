-- Các query đánh giá ALS, MiniLM và demo final
-- 1. ALS output check
SELECT COUNT(*) AS total_rows,COUNT(DISTINCT user_id) AS user_count,COUNT(DISTINCT app_id) AS recommended_game_count,MIN(rank) AS min_rank,MAX(rank) AS max_rank,AVG(als_score) AS avg_als_score,MIN(als_score) AS min_als_score,MAX(als_score) AS max_als_score
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo`;

-- 2. Game text embedding dim/norm
SELECT COUNT(*) AS total_rows,MIN(ARRAY_LENGTH(embedding)) AS min_dim,MAX(ARRAY_LENGTH(embedding)) AS max_dim,AVG(ARRAY_LENGTH(embedding)) AS avg_dim,COUNTIF(ARRAY_LENGTH(embedding)!=384) AS bad_dim_rows FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed`;
SELECT COUNT(*) AS total_rows,AVG(norm) AS avg_norm,MIN(norm) AS min_norm,MAX(norm) AS max_norm,COUNTIF(norm IS NULL) AS null_norm_rows,COUNTIF(norm=0) AS zero_norm_rows FROM (SELECT app_id,SQRT((SELECT SUM(v*v) FROM UNNEST(embedding) AS v)) AS norm FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_minilm_game_text_embeddings_fixed`);

-- 3. Review embedding coverage
SELECT total_games,games_with_review_embedding,SAFE_DIVIDE(games_with_review_embedding,total_games) AS review_embedding_coverage
FROM (SELECT (SELECT COUNT(DISTINCT app_id) FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features`) AS total_games,(SELECT COUNT(DISTINCT app_id) FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_review_embedding_game_features`) AS games_with_review_embedding);

-- 4. Catalog coverage
WITH rec_games AS (SELECT DISTINCT app_id FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo`), catalog AS (SELECT DISTINCT app_id FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features`)
SELECT (SELECT COUNT(*) FROM rec_games) AS recommended_unique_games,(SELECT COUNT(*) FROM catalog) AS catalog_games,SAFE_DIVIDE((SELECT COUNT(*) FROM rec_games),(SELECT COUNT(*) FROM catalog)) AS catalog_coverage;

-- 5. Popularity bias
WITH rec_games AS (SELECT DISTINCT app_id FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo`), catalog AS (SELECT app_id,popularity_score,quality_score,recency_score,price_attractiveness_score FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features`)
SELECT 'recommended_games' AS group_name,COUNT(*) AS game_count,AVG(popularity_score) AS avg_popularity_score,AVG(quality_score) AS avg_quality_score,AVG(recency_score) AS avg_recency_score,AVG(price_attractiveness_score) AS avg_price_score FROM catalog c JOIN rec_games r USING(app_id)
UNION ALL
SELECT 'all_catalog',COUNT(*),AVG(popularity_score),AVG(quality_score),AVG(recency_score),AVG(price_attractiveness_score) FROM catalog;

-- 6. Top recommended games
SELECT r.app_id,ANY_VALUE(g.game_title) AS game_title,COUNT(*) AS recommended_count,AVG(r.als_score) AS avg_als_score,ANY_VALUE(g.popularity_score) AS popularity_score,ANY_VALUE(g.quality_score) AS quality_score
FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo` r LEFT JOIN `project-79499e5c-69d7-42b8-864.steam_gold.gold_game_profile_features` g USING(app_id)
GROUP BY r.app_id ORDER BY recommended_count DESC LIMIT 30;

-- 7. Demo final summary
SELECT user_type,COUNT(*) AS total_rows,COUNT(DISTINCT demo_user_id) AS user_count,COUNT(DISTINCT section_name) AS section_count,COUNT(DISTINCT app_id) AS unique_games
FROM `project-79499e5c-69d7-42b8-864.steam_gold.demo_steam_homepage_final` GROUP BY user_type;

-- 8. Section distribution
SELECT user_type,section_name,COUNT(*) AS rows,COUNT(DISTINCT demo_user_id) AS user_count,COUNT(DISTINCT app_id) AS unique_games
FROM `project-79499e5c-69d7-42b8-864.steam_gold.demo_steam_homepage_final` GROUP BY user_type,section_name ORDER BY user_type,section_name;

-- 9. Ranking metrics @10 và @30
-- Nếu bảng split không có split/label, sửa điều kiện WHERE split='test' AND label=1.
-- Thay 10 bằng 30 để tính @30.
WITH rec AS (SELECT CAST(user_id AS STRING) AS user_id,CAST(app_id AS STRING) AS app_id,CAST(rank AS INT64) AS rank_position FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_recommendations_top30_10000users_demo` WHERE CAST(rank AS INT64)<=10),
test_pos AS (SELECT DISTINCT CAST(user_id AS STRING) AS user_id,CAST(app_id AS STRING) AS app_id FROM `project-79499e5c-69d7-42b8-864.steam_gold.gold_als_interaction_split` WHERE split='test' AND label=1),
tu AS (SELECT user_id,COUNT(DISTINCT app_id) AS test_positive_count FROM test_pos GROUP BY user_id),
j AS (SELECT r.user_id,r.app_id,r.rank_position,IF(t.app_id IS NOT NULL,1,0) AS is_hit FROM rec r LEFT JOIN test_pos t ON r.user_id=t.user_id AND r.app_id=t.app_id),
pu AS (SELECT u.user_id,IFNULL(SUM(j.is_hit),0) AS hits_at_k,u.test_positive_count,IFNULL(SUM(CASE WHEN j.is_hit=1 THEN 1.0/(LOG(j.rank_position+1)/LOG(2)) ELSE 0.0 END),0.0) AS dcg_at_k FROM tu u LEFT JOIN j ON u.user_id=j.user_id GROUP BY u.user_id,u.test_positive_count),
idcg AS (SELECT user_id,SUM(1.0/(LOG(pos+1)/LOG(2))) AS idcg_at_k FROM tu,UNNEST(GENERATE_ARRAY(1,LEAST(10,test_positive_count))) AS pos GROUP BY user_id)
SELECT 10 AS k,COUNT(*) AS evaluated_users,AVG(SAFE_DIVIDE(hits_at_k,10)) AS precision_at_k,AVG(SAFE_DIVIDE(hits_at_k,test_positive_count)) AS recall_at_k,AVG(IF(hits_at_k>0,1.0,0.0)) AS hitrate_at_k,AVG(SAFE_DIVIDE(dcg_at_k,idcg_at_k)) AS ndcg_at_k,AVG(hits_at_k) AS avg_hits_at_k,AVG(test_positive_count) AS avg_test_positives_per_user
FROM pu LEFT JOIN idcg USING(user_id);
