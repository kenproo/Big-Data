from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    SILVER_REVIEWS,
    SILVER_GAMES,
    SILVER_USERS,
    SILVER_QUALITY_HISTOGRAMS,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


def build_histogram(df, table_name, column_name, bucket_size, max_value=None):
    if column_name not in df.columns:
        return None

    work_df = df.filter(F.col(column_name).isNotNull())

    if max_value is not None:
        work_df = work_df.filter(F.col(column_name) <= F.lit(max_value))

    bucket_col = (
        F.floor(F.col(column_name) / F.lit(bucket_size)) * F.lit(bucket_size)
    ).cast("double")

    hist_df = (
        work_df
        .withColumn("bucket_start", bucket_col)
        .groupBy("bucket_start")
        .agg(F.count("*").alias("bin_count"))
        .withColumn("bucket_end", F.col("bucket_start") + F.lit(bucket_size))
        .withColumn("table_name", F.lit(table_name))
        .withColumn("column_name", F.lit(column_name))
        .withColumn("bucket_size", F.lit(float(bucket_size)))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "table_name",
            "column_name",
            "bucket_start",
            "bucket_end",
            "bucket_size",
            "bin_count",
            "created_at",
        )
        .orderBy("bucket_start")
    )

    return hist_df


def build_dataset_histograms(spark, table_name, path, specs):
    print(f"=== START HISTOGRAMS: {table_name} ===")

    df = read_parquet(spark, path)
    frames = []

    for spec in specs:
        col_name = spec["column_name"]
        bucket_size = spec["bucket_size"]
        max_value = spec.get("max_value")

        hist_df = build_histogram(
            df=df,
            table_name=table_name,
            column_name=col_name,
            bucket_size=bucket_size,
            max_value=max_value,
        )

        if hist_df is not None:
            frames.append(hist_df)

    if not frames:
        return spark.createDataFrame(
            [],
            schema="""
                table_name string,
                column_name string,
                bucket_start double,
                bucket_end double,
                bucket_size double,
                bin_count long,
                created_at timestamp
            """,
        )

    result = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    print(f"=== END HISTOGRAMS: {table_name} ===")
    return result


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_quality_histograms ===")

    frames = [
        build_dataset_histograms(
            spark,
            "reviews",
            SILVER_REVIEWS,
            [
                {"column_name": "author_num_games_owned", "bucket_size": 10, "max_value": 500},
                {"column_name": "author_num_reviews", "bucket_size": 5, "max_value": 200},
                {"column_name": "author_playtime_forever", "bucket_size": 1000, "max_value": 50000},
                {"column_name": "author_playtime_last_two_weeks", "bucket_size": 200, "max_value": 10000},
                {"column_name": "author_playtime_at_review", "bucket_size": 500, "max_value": 30000},
                {"column_name": "votes_up", "bucket_size": 10, "max_value": 500},
                {"column_name": "votes_funny", "bucket_size": 5, "max_value": 200},
                {"column_name": "comment_count", "bucket_size": 5, "max_value": 200},
                {"column_name": "review_text_length", "bucket_size": 50, "max_value": 3000},
                {"column_name": "weighted_vote_score", "bucket_size": 0.05, "max_value": 1.0},
            ],
        ),
        build_dataset_histograms(
            spark,
            "games",
            SILVER_GAMES,
            [
                {"column_name": "price_final", "bucket_size": 5, "max_value": 200},
                {"column_name": "price_original", "bucket_size": 5, "max_value": 200},
                {"column_name": "discount_percent", "bucket_size": 5, "max_value": 100},
                {"column_name": "metacritic_score", "bucket_size": 5, "max_value": 100},
                {"column_name": "recommendation_count", "bucket_size": 1000, "max_value": 50000},
                {"column_name": "required_age", "bucket_size": 2, "max_value": 30},
            ],
        ),
        build_dataset_histograms(
            spark,
            "users",
            SILVER_USERS,
            [
                {"column_name": "products", "bucket_size": 10, "max_value": 500},
                {"column_name": "reviews", "bucket_size": 5, "max_value": 200},
            ],
        ),
    ]

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    write_parquet(
        result,
        SILVER_QUALITY_HISTOGRAMS,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_histograms ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-histograms").getOrCreate()
    main(spark)