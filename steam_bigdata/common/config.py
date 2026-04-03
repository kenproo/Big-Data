# common/config.py

PROJECT_ID = "YOUR_PROJECT_ID"
REGION = "YOUR_REGION"

BUCKET = "YOUR_NAME_BUCKET"
BATCH_DATE = ""

# =========================
# BRONZE
# =========================
BRONZE_ALL_REVIEWS = f"{BUCKET}/bronze/all_reviews/batch_date={BATCH_DATE}/*.csv.gz"

BRONZE_REC_GAMES = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/games.csv"
BRONZE_REC_GAMES_META = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/games_metadata.json"
BRONZE_REC_RECOMMENDATIONS = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/recommendations.csv"
BRONZE_REC_USERS = f"{BUCKET}/bronze/recommendations/batch_date={BATCH_DATE}/users.csv"

BRONZE_STEAM_INSIGHTS_GAMES = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/games/*"
BRONZE_STEAM_INSIGHTS_CATEGORIES = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/categories/*"
BRONZE_STEAM_INSIGHTS_DESCRIPTIONS = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/descriptions/*"
BRONZE_STEAM_INSIGHTS_GENRES = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/genres/*"
BRONZE_STEAM_INSIGHTS_PROMOTIONAL = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/promotional/*"
BRONZE_STEAM_INSIGHTS_REVIEWS = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/reviews/*"
BRONZE_STEAM_INSIGHTS_STEAMSPY = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/steamspy_insights/*"
BRONZE_STEAM_INSIGHTS_TAGS = f"{BUCKET}/bronze/steam_insights/batch_date={BATCH_DATE}/tags/*"

# =========================
# SILVER
# =========================
SILVER_REVIEWS_ALL = f"{BUCKET}/silver/reviews_all"
SILVER_REC_GAMES = f"{BUCKET}/silver/rec_games"
SILVER_REC_GAMES_META = f"{BUCKET}/silver/rec_games_meta"
SILVER_REC_USERS = f"{BUCKET}/silver/rec_users"
SILVER_REC_INTERACTIONS = f"{BUCKET}/silver/rec_interactions"

SILVER_INSIGHT_GAMES = f"{BUCKET}/silver/insight_games"
SILVER_INSIGHT_CATEGORIES = f"{BUCKET}/silver/insight_categories"
SILVER_INSIGHT_DESCRIPTIONS = f"{BUCKET}/silver/insight_descriptions"
SILVER_INSIGHT_GENRES = f"{BUCKET}/silver/insight_genres"
SILVER_INSIGHT_PROMOTIONAL = f"{BUCKET}/silver/insight_promotional"
SILVER_INSIGHT_REVIEWS = f"{BUCKET}/silver/insight_reviews"
SILVER_INSIGHT_STEAMSPY = f"{BUCKET}/silver/insight_steamspy"
SILVER_INSIGHT_TAGS = f"{BUCKET}/silver/insight_tags"

# =========================
# GOLD
# =========================
GOLD_DATA_QUALITY = f"{BUCKET}/gold/data_quality"
GOLD_GAME_DIM = f"{BUCKET}/gold/game_dim"
GOLD_USER_DIM = f"{BUCKET}/gold/user_dim"
GOLD_INTERACTION_FACT = f"{BUCKET}/gold/interaction_fact"
GOLD_GAME_FEATURES = f"{BUCKET}/gold/game_features"
GOLD_HYBRID_BASE = f"{BUCKET}/gold/hybrid_base"
GOLD_KPI = f"{BUCKET}/gold/kpi"