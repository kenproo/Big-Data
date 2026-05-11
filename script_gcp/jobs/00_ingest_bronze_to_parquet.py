from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BATCH_DATE,
    BRONZE_ALL_REVIEWS,
    BRONZE_REC_GAMES,
    BRONZE_REC_GAMES_META,
    BRONZE_REC_RECOMMENDATIONS,
    BRONZE_REC_USERS,
    BRONZE_STEAM_INSIGHTS_GAMES,
    BRONZE_STEAM_INSIGHTS_GENRES,
    BRONZE_STEAM_INSIGHTS_TAGS,
    BRONZE_STEAM_INSIGHTS_CATEGORIES,
    BRONZE_STEAM_INSIGHTS_DESCRIPTIONS,
    BRONZE_STEAM_INSIGHTS_REVIEWS,
    BRONZE_STEAM_INSIGHTS_PROMOTIONAL,
    BRONZE_STEAM_INSIGHTS_STEAMSPY,
    BRONZE_PARQUET_ALL_REVIEWS,
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_GAMES_META,
    BRONZE_PARQUET_REC_RECOMMENDATIONS,
    BRONZE_PARQUET_REC_USERS,
    BRONZE_PARQUET_STEAM_GAMES,
    BRONZE_PARQUET_STEAM_GENRES,
    BRONZE_PARQUET_STEAM_TAGS,
    BRONZE_PARQUET_STEAM_CATEGORIES,
    BRONZE_PARQUET_STEAM_DESCRIPTIONS,
    BRONZE_PARQUET_STEAM_REVIEWS,
    BRONZE_PARQUET_STEAM_PROMOTIONAL,
    BRONZE_PARQUET_STEAMSPY,
)
from steam_bigdata.common.io import read_csv, read_json, write_parquet
from steam_bigdata.common.spark_session import get_spark


def add_bronze_metadata(df, source_name):
    return (
        df.withColumn("batch_date", F.lit(BATCH_DATE))
          .withColumn("bronze_ingested_at", F.current_timestamp())
          .withColumn("source_name", F.lit(source_name))
          .withColumn("source_file", F.input_file_name())
    )


def main():
    spark = get_spark("bronze-to-parquet")

    # all_reviews
    df = read_csv(spark, BRONZE_ALL_REVIEWS, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "all_reviews")
    write_parquet(df, BRONZE_PARQUET_ALL_REVIEWS, mode="overwrite")

    # recommendations
    df = read_csv(spark, BRONZE_REC_GAMES, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "rec_games")
    write_parquet(df, BRONZE_PARQUET_REC_GAMES, mode="overwrite")

    df = read_csv(spark, BRONZE_REC_USERS, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "rec_users")
    write_parquet(df, BRONZE_PARQUET_REC_USERS, mode="overwrite")

    df = read_csv(spark, BRONZE_REC_RECOMMENDATIONS, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "rec_recommendations")
    write_parquet(df, BRONZE_PARQUET_REC_RECOMMENDATIONS, mode="overwrite")

    df = read_json(spark, BRONZE_REC_GAMES_META)
    df = add_bronze_metadata(df, "rec_games_meta")
    write_parquet(df, BRONZE_PARQUET_REC_GAMES_META, mode="overwrite")

    # steam insights
    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_GAMES, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_games")
    write_parquet(df, BRONZE_PARQUET_STEAM_GAMES, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_GENRES, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_genres")
    write_parquet(df, BRONZE_PARQUET_STEAM_GENRES, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_TAGS, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_tags")
    write_parquet(df, BRONZE_PARQUET_STEAM_TAGS, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_CATEGORIES, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_categories")
    write_parquet(df, BRONZE_PARQUET_STEAM_CATEGORIES, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_DESCRIPTIONS, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_descriptions")
    write_parquet(df, BRONZE_PARQUET_STEAM_DESCRIPTIONS, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_REVIEWS, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_reviews")
    write_parquet(df, BRONZE_PARQUET_STEAM_REVIEWS, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_PROMOTIONAL, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steam_promotional")
    write_parquet(df, BRONZE_PARQUET_STEAM_PROMOTIONAL, mode="overwrite")

    df = read_csv(spark, BRONZE_STEAM_INSIGHTS_STEAMSPY, header=True, infer_schema=True)
    df = add_bronze_metadata(df, "steamspy_insights")
    write_parquet(df, BRONZE_PARQUET_STEAMSPY, mode="overwrite")

    print("Bronze CSV/JSON -> Bronze Parquet completed successfully.")


if __name__ == "__main__":
    main()