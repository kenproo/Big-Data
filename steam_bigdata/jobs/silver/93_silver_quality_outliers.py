from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    SILVER_REVIEWS,
    SILVER_GAMES,
    SILVER_USERS,
    SILVER_QUALITY_OUTLIERS,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


OUTLIER_COLUMNS = {
    "reviews": [
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "author_playtime_at_review",
        "votes_up",
        "votes_funny",
        "comment_count",
        "weighted_vote_score",
        "review_text_length",
    ],
    "games": [
        "price_final",
        "price_original",
        "discount_percent",
        "metacritic_score",
        "recommendation_count",
        "required_age",
    ],
    "users": [
        "products",
        "reviews",
    ],
}


def build_outlier_metrics_for_column(df, table_name, column_name, row_count, rel_error=0.01):
    # approxQuantile cho từng cột, nhưng chỉ trên vài cột quan trọng
    q = df.approxQuantile(column_name, [0.95, 0.99, 0.999], rel_error)

    p95 = float(q[0]) if len(q) > 0 and q[0] is not None else None
    p99 = float(q[1]) if len(q) > 1 and q[1] is not None else None
    p999 = float(q[2]) if len(q) > 2 and q[2] is not None else None

    outlier_expr = (
        F.sum(F.when(F.col(column_name) > F.lit(p999), 1).otherwise(0)).cast("long").alias("outlier_count")
        if p999 is not None
        else F.lit(0).cast("long").alias("outlier_count")
    )

    return (
        df.agg(
            F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).cast("long").alias("null_count"),
            F.min(F.col(column_name)).cast("double").alias("min_value"),
            F.max(F.col(column_name)).cast("double").alias("max_value"),
            F.avg(F.col(column_name)).cast("double").alias("avg_value"),
            F.stddev(F.col(column_name)).cast("double").alias("stddev_value"),
            outlier_expr,
        )
        .withColumn("table_name", F.lit(table_name))
        .withColumn("column_name", F.lit(column_name))
        .withColumn("row_count", F.lit(row_count))
        .withColumn("p95", F.lit(p95))
        .withColumn("p99", F.lit(p99))
        .withColumn("p999", F.lit(p999))
        .withColumn(
            "outlier_rate",
            F.when(F.lit(row_count) > 0, F.col("outlier_count") / F.lit(row_count)).otherwise(F.lit(0.0))
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "table_name",
            "column_name",
            "row_count",
            "null_count",
            "min_value",
            "max_value",
            "avg_value",
            "stddev_value",
            "p95",
            "p99",
            "p999",
            "outlier_count",
            "outlier_rate",
            "created_at",
        )
    )


def build_outlier_metrics(spark, table_name, path):
    print(f"=== START OUTLIER PROFILE: {table_name} ===")

    df = read_parquet(spark, path)
    existing_cols = [c for c in OUTLIER_COLUMNS.get(table_name, []) if c in df.columns]

    if not existing_cols:
        return spark.createDataFrame(
            [],
            schema="""
                table_name string,
                column_name string,
                row_count long,
                null_count long,
                min_value double,
                max_value double,
                avg_value double,
                stddev_value double,
                p95 double,
                p99 double,
                p999 double,
                outlier_count long,
                outlier_rate double,
                created_at timestamp
            """,
        )

    # 1 lần count cho cả bảng
    row_count = df.count()

    frames = [
        build_outlier_metrics_for_column(df, table_name, c, row_count)
        for c in existing_cols
    ]

    result = reduce(lambda a, b: a.unionByName(b), frames)

    print(f"=== END OUTLIER PROFILE: {table_name} ===")
    return result


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_quality_outliers ===")

    frames = [
        build_outlier_metrics(spark, "reviews", SILVER_REVIEWS),
        build_outlier_metrics(spark, "games", SILVER_GAMES),
        build_outlier_metrics(spark, "users", SILVER_USERS),
    ]

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    write_parquet(
        result,
        SILVER_QUALITY_OUTLIERS,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_outliers ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-outliers").getOrCreate()
    main(spark)