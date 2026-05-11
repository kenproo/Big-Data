from collections import Counter

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_GAMES_META,
    BRONZE_PARQUET_STEAM_GAMES,
    BRONZE_PARQUET_STEAM_REVIEWS,
    BRONZE_PARQUET_STEAMSPY,
    BRONZE_PARQUET_STEAM_PROMOTIONAL,
    SILVER_GAMES,
    SILVER_REJECTED_ROOT,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.transforms import clean_string_col, safe_to_long, safe_to_double
from steam_bigdata.common.spark_config import apply_spark_tuning
from steam_bigdata.common.outliers import (
    ensure_reason_cols,
    add_issue,
    add_hard_range_issue,
    flag_upper_quantile_outliers,
    cap_upper_quantile,
    add_is_outlier_flag,
)


def clean_strings(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, clean_string_col(c))
    return df


def drop_metadata_cols(df):
    metadata_cols = [
        "batch_date",
        "bronze_ingested_at",
        "ingested_at",
        "silver_ingested_at",
        "_rescued_data",
        "_corrupt_record",
    ]
    cols_to_drop = [c for c in metadata_cols if c in df.columns]
    return df.drop(*cols_to_drop) if cols_to_drop else df


def normalize_app_id(df, source_app_col="app_id"):
    if source_app_col not in df.columns:
        return df
    if source_app_col != "app_id":
        df = df.withColumnRenamed(source_app_col, "app_id")
    df = df.withColumn("app_id", safe_to_long("app_id"))
    return df


def debug_duplicate_columns(df, label="df"):
    dup_cols = [c for c, n in Counter(df.columns).items() if n > 1]
    print(f"[DEBUG] {label} total_cols = {len(df.columns)}")
    print(f"[DEBUG] {label} duplicate_cols = {dup_cols}")


def dedup_one_row_per_app(df):
    if "app_id" not in df.columns:
        return df
    w = Window.partitionBy("app_id").orderBy(F.lit(1))
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def prepare_base_table(df, source_app_col="app_id"):
    df = clean_strings(df)
    df = drop_metadata_cols(df)
    df = normalize_app_id(df, source_app_col=source_app_col)
    df = df.filter(F.col("app_id").isNotNull())
    return df


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_games ===")
    print(f"OUTPUT VALID = {SILVER_GAMES}")
    print(f"OUTPUT INVALID = {SILVER_REJECTED_ROOT}/games")

    # ----------------------------
    # Load bronze sources
    # ----------------------------
    rec_games_raw = prepare_base_table(read_parquet(spark, BRONZE_PARQUET_REC_GAMES), "app_id")
    rec_meta_raw = prepare_base_table(read_parquet(spark, BRONZE_PARQUET_REC_GAMES_META), "app_id")
    steam_games_raw = prepare_base_table(read_parquet(spark, BRONZE_PARQUET_STEAM_GAMES), "app_id")
    steam_reviews_raw = prepare_base_table(read_parquet(spark, BRONZE_PARQUET_STEAM_REVIEWS), "appid")
    steamspy_raw = prepare_base_table(read_parquet(spark, BRONZE_PARQUET_STEAMSPY), "app_id")
    promotional_raw = prepare_base_table(read_parquet(spark, BRONZE_PARQUET_STEAM_PROMOTIONAL), "app_id")

    print("READ + NORMALIZE DONE")

    # ----------------------------
    # Recommendation games main = BASE TABLE
    # ----------------------------
    rec_games_main = rec_games_raw.select(
        *[c for c in [
            "app_id",
            "title",
            "date_release",
            "win",
            "mac",
            "linux",
            "rating",
            "positive_ratio",
            "user_reviews",
            "price_final",
            "price_original",
            "discount",
            "steam_deck",
        ] if c in rec_games_raw.columns]
    )

    if "date_release" in rec_games_main.columns:
        rec_games_main = rec_games_main.withColumn("date_release", F.to_date("date_release"))

    rec_games_main = dedup_one_row_per_app(rec_games_main)

    # ----------------------------
    # Steam games main (rename to avoid duplicate column names)
    # ----------------------------
    steam_games_main = steam_games_raw.select(
        *[
            x for x in [
                F.col("app_id"),
                F.col("name").alias("steam_name") if "name" in steam_games_raw.columns else None,
                F.col("release_date").alias("steam_release_date") if "release_date" in steam_games_raw.columns else None,
                F.col("is_free").alias("steam_is_free") if "is_free" in steam_games_raw.columns else None,
                F.col("price_overview").alias("steam_price_overview") if "price_overview" in steam_games_raw.columns else None,
                F.col("languages").alias("steam_languages") if "languages" in steam_games_raw.columns else None,
                F.col("type").alias("steam_type") if "type" in steam_games_raw.columns else None,
            ] if x is not None
        ]
    )

    if "steam_release_date" in steam_games_main.columns:
        steam_games_main = steam_games_main.withColumn(
            "steam_release_date",
            F.to_date("steam_release_date")
        )

    steam_games_main = dedup_one_row_per_app(steam_games_main)

    # ----------------------------
    # Metadata json
    # ----------------------------
    rec_meta_main = rec_meta_raw.select(
        *[c for c in ["app_id", "description", "tags"] if c in rec_meta_raw.columns]
    )
    rec_meta_main = dedup_one_row_per_app(rec_meta_main)

    # ----------------------------
    # Promotional / long text fields
    # ----------------------------
    promotional_main = promotional_raw.select(
        *[c for c in ["app_id", "summary", "extensive", "about"] if c in promotional_raw.columns]
    )
    promotional_main = dedup_one_row_per_app(promotional_main)

    # ----------------------------
    # Steamspy: only keep non-conflicting columns
    # ----------------------------
    steamspy_keep = [c for c in [
        "app_id",
        "developer",
        "publisher",
        "owners",
        "average_forever",
        "average_2weeks",
        "median_forever",
        "median_2weeks",
        "ccu",
    ] if c in steamspy_raw.columns]

    steamspy_main = steamspy_raw.select(*steamspy_keep)
    steamspy_main = dedup_one_row_per_app(steamspy_main)

    # ----------------------------
    # Supporting arrays
    # ----------------------------
    genre_agg = None
    if "genre" in steam_games_raw.columns:
        genre_agg = (
            steam_games_raw
            .filter(F.col("genre").isNotNull())
            .groupBy("app_id")
            .agg(F.sort_array(F.collect_set("genre")).alias("genres"))
        )

    tag_agg = None
    if "tag" in steam_games_raw.columns:
        tag_agg = (
            steam_games_raw
            .filter(F.col("tag").isNotNull())
            .groupBy("app_id")
            .agg(F.sort_array(F.collect_set("tag")).alias("tags_insight"))
        )

    category_agg = None
    if "category" in steam_games_raw.columns:
        category_agg = (
            steam_games_raw
            .filter(F.col("category").isNotNull())
            .groupBy("app_id")
            .agg(F.sort_array(F.collect_set("category")).alias("categories"))
        )

    # ----------------------------
    # All reviews aggregate
    # ----------------------------
    reviews_agg = (
        steam_reviews_raw
        .withColumn(
            "votes_up_num",
            safe_to_long("votes_up") if "votes_up" in steam_reviews_raw.columns else F.lit(None)
        )
        .withColumn(
            "votes_funny_num",
            safe_to_long("votes_funny") if "votes_funny" in steam_reviews_raw.columns else F.lit(None)
        )
        .withColumn(
            "voted_up_bool",
            F.when(F.lower(F.col("voted_up").cast("string")).isin("true", "1", "yes"), F.lit(True))
             .when(F.lower(F.col("voted_up").cast("string")).isin("false", "0", "no"), F.lit(False))
             .otherwise(F.lit(None))
            if "voted_up" in steam_reviews_raw.columns else F.lit(None)
        )
        .groupBy("app_id")
        .agg(
            F.count("*").alias("review_count_all_reviews"),
            F.sum(F.when(F.col("voted_up_bool") == True, 1).otherwise(0)).alias("positive_review_count"),
            F.sum(F.when(F.col("voted_up_bool") == False, 1).otherwise(0)).alias("negative_review_count"),
            F.avg("votes_up_num").alias("avg_votes_up"),
            F.avg("votes_funny_num").alias("avg_votes_funny"),
            F.first("game", ignorenulls=True).alias("game_from_reviews")
            if "game" in steam_reviews_raw.columns else F.lit(None).alias("game_from_reviews"),
        )
    )

    # ----------------------------
    # Recommendation events aggregate
    # ----------------------------
    rec_event_agg = (
        rec_games_raw
        .groupBy("app_id")
        .agg(
            F.count("*").alias("recommendation_event_count"),
            F.avg("hours").alias("avg_recommendation_hours")
            if "hours" in rec_games_raw.columns else F.lit(None).alias("avg_recommendation_hours"),
            F.avg("helpful").alias("avg_helpful")
            if "helpful" in rec_games_raw.columns else F.lit(None).alias("avg_helpful"),
            F.avg("funny").alias("avg_funny")
            if "funny" in rec_games_raw.columns else F.lit(None).alias("avg_funny"),
            F.sum(F.when(F.col("is_recommended") == True, 1).otherwise(0)).alias("recommended_count")
            if "is_recommended" in rec_games_raw.columns else F.lit(None).alias("recommended_count"),
        )
    )

    # ----------------------------
    # Build master games table
    # ----------------------------
    games = rec_games_main

    for df_joined in [
        steam_games_main,
        rec_meta_main,
        promotional_main,
        steamspy_main,
        reviews_agg,
        rec_event_agg,
        genre_agg,
        tag_agg,
        category_agg,
    ]:
        if df_joined is not None:
            games = games.join(df_joined, on="app_id", how="left")

    debug_duplicate_columns(games, "games_after_join")

    # ----------------------------
    # Standardize business columns
    # ----------------------------
    candidate_name_cols = [c for c in ["title", "steam_name", "game_from_reviews"] if c in games.columns]
    if candidate_name_cols:
        games = games.withColumn(
            "game_name",
            F.coalesce(*[F.col(c) for c in candidate_name_cols])
        )

    if "steam_is_free" in games.columns:
        games = games.withColumn(
            "is_free_clean",
            F.when(F.col("steam_is_free").cast("int") == 1, F.lit(True))
             .when(F.col("steam_is_free").cast("int") == 0, F.lit(False))
             .otherwise(F.col("steam_is_free").cast("boolean"))
        )

    if "steam_release_date" in games.columns and "date_release" in games.columns:
        games = games.withColumn(
            "release_date_final",
            F.coalesce(F.col("date_release"), F.col("steam_release_date"))
        )
    elif "date_release" in games.columns:
        games = games.withColumn("release_date_final", F.col("date_release"))
    elif "steam_release_date" in games.columns:
        games = games.withColumn("release_date_final", F.col("steam_release_date"))

    # Final select to lock schema and avoid duplicate columns
    final_cols = [c for c in [
        "app_id",
        "title",
        "steam_name",
        "game_name",
        "date_release",
        "steam_release_date",
        "release_date_final",
        "win",
        "mac",
        "linux",
        "rating",
        "positive_ratio",
        "user_reviews",
        "price_final",
        "price_original",
        "discount",
        "steam_deck",
        "steam_is_free",
        "is_free_clean",
        "steam_price_overview",
        "steam_languages",
        "steam_type",
        "description",
        "tags",
        "summary",
        "extensive",
        "about",
        "genres",
        "tags_insight",
        "categories",
        "review_count_all_reviews",
        "positive_review_count",
        "negative_review_count",
        "avg_votes_up",
        "avg_votes_funny",
        "recommendation_event_count",
        "avg_recommendation_hours",
        "avg_helpful",
        "avg_funny",
        "recommended_count",
        "developer",
        "publisher",
        "owners",
        "average_forever",
        "average_2weeks",
        "median_forever",
        "median_2weeks",
        "ccu",
    ] if c in games.columns]

    games = games.select(*final_cols)
    debug_duplicate_columns(games, "games_after_final_select")

    # ----------------------------
    # Numeric casts
    # ----------------------------
    for c in [
        "positive_ratio",
        "user_reviews",
        "price_final",
        "price_original",
        "discount",
        "review_count_all_reviews",
        "positive_review_count",
        "negative_review_count",
        "avg_votes_up",
        "avg_votes_funny",
        "recommendation_event_count",
        "avg_recommendation_hours",
        "avg_helpful",
        "avg_funny",
        "recommended_count",
        "average_forever",
        "average_2weeks",
        "median_forever",
        "median_2weeks",
        "ccu",
    ]:
        if c in games.columns:
            games = games.withColumn(c, safe_to_double(c))

    # ----------------------------
    # Quality checks
    # ----------------------------
    games = ensure_reason_cols(games)

    games = add_issue(games, F.col("app_id").isNull(), "missing_app_id")

    if "game_name" in games.columns:
        games = add_issue(
            games,
            F.col("game_name").isNull() | (F.length(F.trim(F.col("game_name"))) == 0),
            "missing_game_name",
        )

    for c in [
        "positive_ratio",
        "user_reviews",
        "price_final",
        "price_original",
        "discount",
        "review_count_all_reviews",
        "positive_review_count",
        "negative_review_count",
        "recommendation_event_count",
    ]:
        if c in games.columns:
            games = add_issue(
                games,
                F.col(c).isNotNull() & (F.col(c) < 0),
                f"negative_{c}",
            )

    if "positive_ratio" in games.columns:
        games = add_hard_range_issue(
            games,
            "positive_ratio",
            min_value=0.0,
            max_value=100.0,
            issue_name="invalid_positive_ratio",
        )

    if "discount" in games.columns:
        games = add_hard_range_issue(
            games,
            "discount",
            min_value=0.0,
            max_value=100.0,
            issue_name="invalid_discount",
        )

    if "release_date_final" in games.columns:
        games = add_issue(
            games,
            F.col("release_date_final").isNotNull() &
            (F.col("release_date_final") > F.current_date() + F.expr("INTERVAL 30 DAYS")),
            "release_date_too_far_in_future",
        )

    games = games.filter(F.col("app_id").isNotNull()).dropDuplicates(["app_id"])

    print("RULES + DEDUP DONE")

    # ----------------------------
    # Split valid / invalid
    # ----------------------------
    invalid_df = games.filter(F.col("quality_issue").isNotNull())
    valid_df = games.filter(F.col("quality_issue").isNull())

    outlier_cols = [
        c for c in [
            "price_final",
            "price_original",
            "user_reviews",
            "review_count_all_reviews",
            "recommendation_event_count",
        ]
        if c in valid_df.columns
    ]

    if outlier_cols:
        valid_df = flag_upper_quantile_outliers(
            valid_df,
            outlier_cols,
            quantile=0.999,
            rel_error=0.01,
        )
        valid_df = cap_upper_quantile(
            valid_df,
            outlier_cols,
            quantile=0.999,
            rel_error=0.01,
            suffix="_capped",
        )

    if "game_name" in valid_df.columns:
        valid_df = valid_df.withColumn("game_name_length", F.length("game_name"))
        valid_df = flag_upper_quantile_outliers(
            valid_df,
            ["game_name_length"],
            quantile=0.999,
            rel_error=0.01,
        )

    valid_df = add_is_outlier_flag(valid_df)

    debug_duplicate_columns(valid_df, "valid_df_before_write")
    debug_duplicate_columns(invalid_df, "invalid_df_before_write")

    print("WRITE START")

    write_parquet(
        valid_df,
        SILVER_GAMES,
        mode="overwrite",
        num_partitions=80,
    )

    write_parquet(
        invalid_df,
        f"{SILVER_REJECTED_ROOT}/games",
        mode="overwrite",
        num_partitions=20,
    )

    print("WRITE VALID DONE")
    print("WRITE INVALID DONE")
    print("=== END silver_games ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-games").getOrCreate()
    main(spark)
    spark.stop()