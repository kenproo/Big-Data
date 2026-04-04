from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BRONZE_PARQUET_ALL_REVIEWS,
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_USERS,
    SILVER_REVIEWS,
    SILVER_GAMES,
    SILVER_USERS,
    SILVER_QUALITY_COLUMNS,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


# Chỉ profile các cột quan trọng để tránh job quá nặng
PROFILE_COLUMNS = {
    "reviews": [
        "review_id",
        "app_id",
        "author_steamid",
        "review",
        "voted_up",
        "weighted_vote_score",
        "timestamp_created",
        "timestamp_updated",
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "votes_up",
        "votes_funny",
        "comment_count",
        "review_text_length",
        "quality_issue",
        "is_outlier",
    ],
    "games": [
        "app_id",
        "name",
        "price_final",
        "price_original",
        "discount_percent",
        "required_age",
        "is_free",
        "metacritic_score",
        "recommendation_count",
        "quality_issue",
        "is_outlier",
    ],
    "users": [
        "user_id",
        "products",
        "reviews",
        "quality_issue",
        "is_outlier",
    ],
}


def profile_columns(df, table_name, layer_name, use_approx_distinct=True):
    existing_cols = [c for c in PROFILE_COLUMNS.get(table_name, []) if c in df.columns]

    if not existing_cols:
        return df.sparkSession.createDataFrame(
            [],
            schema="""
                table_name string,
                layer_name string,
                column_name string,
                row_count long,
                null_count long,
                null_rate double,
                distinct_count long
            """,
        )

    # 1 lần count cho cả bảng
    row_count = df.count()

    frames = []
    for c in existing_cols:
        col_expr = F.col(c)

        if use_approx_distinct:
            distinct_expr = F.approx_count_distinct(col_expr).alias("distinct_count")
        else:
            distinct_expr = F.countDistinct(col_expr).alias("distinct_count")

        metric_df = (
            df.agg(
                F.sum(F.when(col_expr.isNull(), 1).otherwise(0)).cast("long").alias("null_count"),
                distinct_expr,
            )
            .withColumn("table_name", F.lit(table_name))
            .withColumn("layer_name", F.lit(layer_name))
            .withColumn("column_name", F.lit(c))
            .withColumn("row_count", F.lit(row_count))
            .withColumn(
                "null_rate",
                F.when(F.lit(row_count) > 0, F.col("null_count") / F.lit(row_count)).otherwise(F.lit(0.0)),
            )
            .select(
                "table_name",
                "layer_name",
                "column_name",
                "row_count",
                "null_count",
                "null_rate",
                "distinct_count",
            )
        )
        frames.append(metric_df)

    return reduce(lambda a, b: a.unionByName(b), frames)


def compare_columns(spark, table_name, bronze_path, silver_path):
    print(f"=== START COLUMN PROFILE: {table_name} ===")

    bronze_df = read_parquet(spark, bronze_path)
    silver_df = read_parquet(spark, silver_path)

    # Bronze giữ approx distinct để nhẹ hơn
    bronze_profile = profile_columns(
        bronze_df,
        table_name=table_name,
        layer_name="bronze",
        use_approx_distinct=True,
    )

    # Silver cũng approx distinct cho ổn định chi phí
    silver_profile = profile_columns(
        silver_df,
        table_name=table_name,
        layer_name="silver",
        use_approx_distinct=True,
    )

    bronze_renamed = bronze_profile.select(
        "table_name",
        "column_name",
        F.col("row_count").alias("bronze_row_count"),
        F.col("null_count").alias("bronze_null_count"),
        F.col("null_rate").alias("bronze_null_rate"),
        F.col("distinct_count").alias("bronze_distinct_count"),
    )

    silver_renamed = silver_profile.select(
        "table_name",
        "column_name",
        F.col("row_count").alias("silver_row_count"),
        F.col("null_count").alias("silver_null_count"),
        F.col("null_rate").alias("silver_null_rate"),
        F.col("distinct_count").alias("silver_distinct_count"),
    )

    result = (
        bronze_renamed.join(
            silver_renamed,
            on=["table_name", "column_name"],
            how="full_outer",
        )
        .withColumn(
            "null_rate_improvement",
            F.coalesce(F.col("bronze_null_rate"), F.lit(0.0)) - F.coalesce(F.col("silver_null_rate"), F.lit(0.0))
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "table_name",
            "column_name",
            "bronze_row_count",
            "silver_row_count",
            "bronze_null_count",
            "silver_null_count",
            "bronze_null_rate",
            "silver_null_rate",
            "null_rate_improvement",
            "bronze_distinct_count",
            "silver_distinct_count",
            "created_at",
        )
    )

    print(f"=== END COLUMN PROFILE: {table_name} ===")
    return result


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_quality_columns ===")

    datasets = [
        ("reviews", BRONZE_PARQUET_ALL_REVIEWS, SILVER_REVIEWS),
        ("games", BRONZE_PARQUET_REC_GAMES, SILVER_GAMES),
        ("users", BRONZE_PARQUET_REC_USERS, SILVER_USERS),
    ]

    frames = [
        compare_columns(spark, table_name, bronze_path, silver_path)
        for table_name, bronze_path, silver_path in datasets
    ]

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    write_parquet(
        result,
        SILVER_QUALITY_COLUMNS,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_columns ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-columns").getOrCreate()
    main(spark)