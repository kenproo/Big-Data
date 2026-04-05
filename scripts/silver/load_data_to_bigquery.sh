#!/bin/bash
set -e

PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
REGION="${REGION:-YOUR_REGION}"
BUCKET="${BUCKET:-gs://YOUR_BUCKET_NAME}"
BATCH_DATE="2026-03-24"

SILVER_DATASET="steam_silver"
ANALYTICS_DATASET="steam_analytics"

# =========================
# PATHS
# =========================

REVIEWS_BI_PATH="${BUCKET}/gold/reviews_bi/batch_date=${BATCH_DATE}/*"

SILVER_GAMES_PATH="${BUCKET}/silver/games/batch_date=${BATCH_DATE}/*"
SILVER_USERS_PATH="${BUCKET}/silver/users/batch_date=${BATCH_DATE}/*"
SILVER_GAME_TEXT_PATH="${BUCKET}/silver/game_text/batch_date=${BATCH_DATE}/*"
SILVER_GAME_TAGS_PATH="${BUCKET}/silver/game_tags/batch_date=${BATCH_DATE}/*"
SILVER_GAME_CATEGORIES_PATH="${BUCKET}/silver/game_categories/batch_date=${BATCH_DATE}/*"
SILVER_GAME_GENRES_PATH="${BUCKET}/silver/game_genres/batch_date=${BATCH_DATE}/*"

# QUALITY nay nam trong silver/quality
QUALITY_ISSUES_PATH="${BUCKET}/silver/quality/issues/*"
QUALITY_COLUMNS_PATH="${BUCKET}/silver/quality/columns/*"
QUALITY_OUTLIERS_PATH="${BUCKET}/silver/quality/outliers/*"
QUALITY_HISTOGRAMS_PATH="${BUCKET}/silver/quality/histograms/*"
QUALITY_OVERVIEW_PATH="${BUCKET}/silver/quality/overview/*"

REJECTED_REVIEWS_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/reviews/*"
REJECTED_GAMES_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/games/*"
REJECTED_USERS_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/users/*"
REJECTED_GAME_TEXT_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/game_text/*"
REJECTED_GAME_TAGS_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/game_tags/*"
REJECTED_GAME_CATEGORIES_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/game_categories/*"
REJECTED_GAME_GENRES_PATH="${BUCKET}/silver/_rejected/batch_date=${BATCH_DATE}/game_genres/*"

# =========================
# HELPERS
# =========================

load_parquet_table() {
  local table_fq="$1"
  local gcs_path="$2"

  echo "--------------------------------------"
  echo "LOADING: ${table_fq}"
  echo "FROM:    ${gcs_path}"
  echo "--------------------------------------"

  bq load \
    --location="${LOCATION}" \
    --source_format=PARQUET \
    --replace \
    "${table_fq}" \
    "${gcs_path}"
}

# =========================
# CREATE DATASETS
# =========================

echo "======================================"
echo "CREATE BIGQUERY DATASETS IF NOT EXISTS"
echo "======================================"

bq --location="${LOCATION}" mk -d "${PROJECT_ID}:${SILVER_DATASET}" || true
bq --location="${LOCATION}" mk -d "${PROJECT_ID}:${ANALYTICS_DATASET}" || true

# =========================
# LOAD BI-SAFE REVIEWS
# =========================

echo "===================="
echo "LOAD BI SAFE REVIEWS"
echo "===================="

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.silver_reviews_bi" \
  "${REVIEWS_BI_PATH}"

# =========================
# LOAD SILVER TABLES
# =========================

echo "=================="
echo "LOAD SILVER TABLES"
echo "=================="

load_parquet_table \
  "${PROJECT_ID}:${SILVER_DATASET}.silver_games" \
  "${SILVER_GAMES_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${SILVER_DATASET}.silver_users" \
  "${SILVER_USERS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${SILVER_DATASET}.silver_game_text" \
  "${SILVER_GAME_TEXT_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${SILVER_DATASET}.silver_game_tags" \
  "${SILVER_GAME_TAGS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${SILVER_DATASET}.silver_game_categories" \
  "${SILVER_GAME_CATEGORIES_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${SILVER_DATASET}.silver_game_genres" \
  "${SILVER_GAME_GENRES_PATH}"

# =========================
# LOAD QUALITY TABLES
# =========================

echo "==================="
echo "LOAD QUALITY TABLES"
echo "==================="

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.silver_quality_issues" \
  "${QUALITY_ISSUES_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.silver_quality_columns" \
  "${QUALITY_COLUMNS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.silver_quality_outliers" \
  "${QUALITY_OUTLIERS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.silver_quality_histograms" \
  "${QUALITY_HISTOGRAMS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.silver_quality_overview" \
  "${QUALITY_OVERVIEW_PATH}"

# =========================
# LOAD REJECTED TABLES
# =========================

echo "===================="
echo "LOAD REJECTED TABLES"
echo "===================="

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_reviews" \
  "${REJECTED_REVIEWS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_games" \
  "${REJECTED_GAMES_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_users" \
  "${REJECTED_USERS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_game_text" \
  "${REJECTED_GAME_TEXT_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_game_tags" \
  "${REJECTED_GAME_TAGS_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_game_categories" \
  "${REJECTED_GAME_CATEGORIES_PATH}"

load_parquet_table \
  "${PROJECT_ID}:${ANALYTICS_DATASET}.rejected_game_genres" \
  "${REJECTED_GAME_GENRES_PATH}"

# =========================
# VERIFY COUNTS
# =========================

echo "==================="
echo "VERIFY QUICK COUNTS"
echo "==================="

bq query --location="${LOCATION}" --use_legacy_sql=false "
SELECT 'silver_reviews_bi' AS table_name, COUNT(*) AS row_count
FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.silver_reviews_bi\`
UNION ALL
SELECT 'silver_games', COUNT(*)
FROM \`${PROJECT_ID}.${SILVER_DATASET}.silver_games\`
UNION ALL
SELECT 'silver_users', COUNT(*)
FROM \`${PROJECT_ID}.${SILVER_DATASET}.silver_users\`
UNION ALL
SELECT 'silver_game_text', COUNT(*)
FROM \`${PROJECT_ID}.${SILVER_DATASET}.silver_game_text\`
UNION ALL
SELECT 'silver_game_tags', COUNT(*)
FROM \`${PROJECT_ID}.${SILVER_DATASET}.silver_game_tags\`
UNION ALL
SELECT 'silver_game_categories', COUNT(*)
FROM \`${PROJECT_ID}.${SILVER_DATASET}.silver_game_categories\`
UNION ALL
SELECT 'silver_game_genres', COUNT(*)
FROM \`${PROJECT_ID}.${SILVER_DATASET}.silver_game_genres\`
UNION ALL
SELECT 'silver_quality_overview', COUNT(*)
FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.silver_quality_overview\`
UNION ALL
SELECT 'rejected_reviews', COUNT(*)
FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.rejected_reviews\`
UNION ALL
SELECT 'rejected_games', COUNT(*)
FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.rejected_games\`
UNION ALL
SELECT 'rejected_users', COUNT(*)
FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.rejected_users\`
"

echo "===================="
echo "LOAD TO BIGQUERY DONE"
echo "===================="