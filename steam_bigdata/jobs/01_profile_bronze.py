from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BRONZE_ALL_REVIEWS,
    BRONZE_REC_GAMES,
    BRONZE_REC_GAMES_META,
    BRONZE_REC_RECOMMENDATIONS,
    BRONZE_REC_USERS,
    BRONZE_STEAM_INSIGHTS_GAMES,
    BRONZE_STEAM_INSIGHTS_CATEGORIES,
    BRONZE_STEAM_INSIGHTS_DESCRIPTIONS,
    BRONZE_STEAM_INSIGHTS_GENRES,
    BRONZE_STEAM_INSIGHTS_PROMOTIONAL,
    BRONZE_STEAM_INSIGHTS_REVIEWS,
    BRONZE_STEAM_INSIGHTS_STEAMSPY,
    BRONZE_STEAM_INSIGHTS_TAGS,
    GOLD_DATA_QUALITY,
)

from steam_bigdata.common.io import read_csv, read_json, write_parquet
from steam_bigdata.common.spark_session import get_spark
from steam_bigdata.common.utils import (
    normalize_columns,
    add_ingestion_metadata,
    profile_dataframe,
    profile_numeric_columns,
)

def prepare_df(df: DataFrame, source_name: str) -> DataFrame:
    df = normalize_columns(df)
    df = add_ingestion_metadata(df, source_name)
    return df

def build_profile(df: DataFrame, table_name: str) -> DataFrame:
    base_profile = profile_dataframe(df, table_name)
    numeric_profile = profile_numeric_columns(df, table_name)

    result = (
        base_profile.alias("b")
        .join(
            numeric_profile.alias("n"),
            on=["table_name", "column_name"],
            how="left"
        )
        .select(
            "table_name",
            "column_name",
            "row_count",
            "null_count",
            "null_ratio",
            "data_type",
            "min_value",
            "max_value",
            "avg_value",
            "stddev_value"
        )
    )
    return result

def main():
    spark = get_spark("steam-bronze-profile")

    datasets = []

    # 1. All reviews
    try:
        df = prepare_df(read_csv(spark, BRONZE_ALL_REVIEWS), "all_reviews")
        datasets.append(build_profile(df, "bronze_all_reviews"))
    except Exception as e:
        print(f"[WARN] Could not profile BRONZE_ALL_REVIEWS: {e}")

    # 2. Recommendation datasets
    try:
        df = prepare_df(read_csv(spark, BRONZE_REC_GAMES), "rec_games")
        datasets.append(build_profile(df, "bronze_rec_games"))
    except Exception as e:
        print(f"[WARN] Could not profile BRONZE_REC_GAMES: {e}")

    try:
        df = prepare_df(read_json(spark, BRONZE_REC_GAMES_META), "rec_games_meta")
        datasets.append(build_profile(df, "bronze_rec_games_meta"))
    except Exception as e:
        print(f"[WARN] Could not profile BRONZE_REC_GAMES_META: {e}")

    try:
        df = prepare_df(read_csv(spark, BRONZE_REC_RECOMMENDATIONS), "rec_recommendations")
        datasets.append(build_profile(df, "bronze_rec_recommendations"))
    except Exception as e:
        print(f"[WARN] Could not profile BRONZE_REC_RECOMMENDATIONS: {e}")

    try:
        df = prepare_df(read_csv(spark, BRONZE_REC_USERS), "rec_users")
        datasets.append(build_profile(df, "bronze_rec_users"))
    except Exception as e:
        print(f"[WARN] Could not profile BRONZE_REC_USERS: {e}")

    # 3. Steam insights datasets
    steam_insight_sources = [
        (BRONZE_STEAM_INSIGHTS_GAMES, "insight_games", "bronze_insight_games"),
        (BRONZE_STEAM_INSIGHTS_CATEGORIES, "insight_categories", "bronze_insight_categories"),
        (BRONZE_STEAM_INSIGHTS_DESCRIPTIONS, "insight_descriptions", "bronze_insight_descriptions"),
        (BRONZE_STEAM_INSIGHTS_GENRES, "insight_genres", "bronze_insight_genres"),
        (BRONZE_STEAM_INSIGHTS_PROMOTIONAL, "insight_promotional", "bronze_insight_promotional"),
        (BRONZE_STEAM_INSIGHTS_REVIEWS, "insight_reviews", "bronze_insight_reviews"),
        (BRONZE_STEAM_INSIGHTS_STEAMSPY, "insight_steamspy", "bronze_insight_steamspy"),
        (BRONZE_STEAM_INSIGHTS_TAGS, "insight_tags", "bronze_insight_tags"),
    ]

    for path, source_name, table_name in steam_insight_sources:
        try:
            df = prepare_df(read_csv(spark, path), source_name)
            datasets.append(build_profile(df, table_name))
        except Exception as e:
            print(f"[WARN] Could not profile {table_name}: {e}")

    if not datasets:
        raise ValueError("No bronze datasets were successfully profiled.")

    final_profile = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), datasets)

    final_profile = final_profile.withColumn("profile_ts", F.current_timestamp())

    write_parquet(final_profile, GOLD_DATA_QUALITY, mode="overwrite")

    print(f"[INFO] Bronze profiling completed. Output written to: {GOLD_DATA_QUALITY}")

    spark.stop()

if __name__ == "__main__":
    main()