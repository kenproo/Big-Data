PROJECT_ID = "project-79499e5c-69d7-42b8-864"
REGION = "asia-southeast1"

BUCKET = "gs://truong_bigdata_24032026_init"
BATCH_DATE = "2026-03-24"

SILVER_ROOT = f"{BUCKET}/silver"

# =========================
# RAW BRONZE
# =========================
RAW_BRONZE_ALL_REVIEWS = f"{BUCKET}/bronze/all_reviews/batch_date={BATCH_DATE}/*.csv.gz"

RAW_BRONZE_REC_GAMES = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/games.csv"
RAW_BRONZE_REC_GAMES_META = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/games_metadata.json"
RAW_BRONZE_REC_RECOMMENDATIONS = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/recommendations.csv"
RAW_BRONZE_REC_USERS = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/users.csv"

RAW_BRONZE_STEAM_INSIGHTS_GAMES = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/games/*"
RAW_BRONZE_STEAM_INSIGHTS_CATEGORIES = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/categories/*"
RAW_BRONZE_STEAM_INSIGHTS_DESCRIPTIONS = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/descriptions/*"
RAW_BRONZE_STEAM_INSIGHTS_GENRES = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/genres/*"
RAW_BRONZE_STEAM_INSIGHTS_PROMOTIONAL = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/promotional/*"
RAW_BRONZE_STEAM_INSIGHTS_REVIEWS = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/reviews/*"
RAW_BRONZE_STEAM_INSIGHTS_STEAMSPY = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/steamspy_insights/*"
RAW_BRONZE_STEAM_INSIGHTS_TAGS = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/tags/*"

# =========================
# BRONZE PARQUET
# =========================
BRONZE_PARQUET_ALL_REVIEWS = f"{BUCKET}/bronze_parquet/all_reviews/batch_date={BATCH_DATE}"
BRONZE_PARQUET_REC_GAMES = f"{BUCKET}/bronze_parquet/recommendations/games/batch_date={BATCH_DATE}"
BRONZE_PARQUET_REC_GAMES_META = f"{BUCKET}/bronze_parquet/recommendations/games_meta/batch_date={BATCH_DATE}"
BRONZE_PARQUET_REC_RECOMMENDATIONS = f"{BUCKET}/bronze_parquet/recommendations/recommendations/batch_date={BATCH_DATE}"
BRONZE_PARQUET_REC_USERS = f"{BUCKET}/bronze_parquet/recommendations/users/batch_date={BATCH_DATE}"

BRONZE_PARQUET_STEAM_GAMES = f"{BUCKET}/bronze_parquet/steam_insights/games/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAM_GENRES = f"{BUCKET}/bronze_parquet/steam_insights/genres/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAM_TAGS = f"{BUCKET}/bronze_parquet/steam_insights/tags/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAM_CATEGORIES = f"{BUCKET}/bronze_parquet/steam_insights/categories/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAM_DESCRIPTIONS = f"{BUCKET}/bronze_parquet/steam_insights/descriptions/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAM_REVIEWS = f"{BUCKET}/bronze_parquet/steam_insights/reviews/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAM_PROMOTIONAL = f"{BUCKET}/bronze_parquet/steam_insights/promotional/batch_date={BATCH_DATE}"
BRONZE_PARQUET_STEAMSPY = f"{BUCKET}/bronze_parquet/steam_insights/steamspy_insights/batch_date={BATCH_DATE}"

# =========================
# SILVER
# =========================
SILVER_REVIEWS = f"{SILVER_ROOT}/reviews/batch_date={BATCH_DATE}"
SILVER_GAMES = f"{SILVER_ROOT}/games/batch_date={BATCH_DATE}"
SILVER_USERS = f"{SILVER_ROOT}/users/batch_date={BATCH_DATE}"
SILVER_GAME_TAGS = f"{SILVER_ROOT}/game_tags/batch_date={BATCH_DATE}"
SILVER_GAME_CATEGORIES = f"{SILVER_ROOT}/game_categories/batch_date={BATCH_DATE}"
SILVER_GAME_GENRES = f"{SILVER_ROOT}/game_genres/batch_date={BATCH_DATE}"
SILVER_GAME_TEXT = f"{SILVER_ROOT}/game_text/batch_date={BATCH_DATE}"

SILVER_REJECTED_ROOT = f"{SILVER_ROOT}/_rejected/batch_date={BATCH_DATE}"
SILVER_QUALITY_METRICS = f"{SILVER_ROOT}/_quality_metrics/batch_date={BATCH_DATE}"
SILVER_QUALITY_ROOT = f"{SILVER_ROOT}/quality"

SILVER_QUALITY_OVERVIEW = f"{SILVER_QUALITY_ROOT}/overview/batch_date={BATCH_DATE}"
SILVER_QUALITY_ISSUES = f"{SILVER_QUALITY_ROOT}/issues/batch_date={BATCH_DATE}"
SILVER_QUALITY_COLUMNS = f"{SILVER_QUALITY_ROOT}/columns/batch_date={BATCH_DATE}"
SILVER_QUALITY_OUTLIERS = f"{SILVER_QUALITY_ROOT}/outliers/batch_date={BATCH_DATE}"
SILVER_QUALITY_HISTOGRAMS = f"{SILVER_QUALITY_ROOT}/histograms/batch_date={BATCH_DATE}"