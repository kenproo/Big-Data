from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from steam_bigdata.common.config import (
    SILVER_REVIEWS,
    SILVER_GAMES,
    SILVER_USERS,
    SILVER_QUALITY_OUTLIERS,   # add this in config.py if not exists
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


OUTLIER_COLUMNS = {
    "reviews": [
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "hours",
        "helpful",
        "funny",
        "comment_count",
        "review_text_length",
    ],
    "games": [
        "price_final",
        "price_original",
        "discount_percent",
        "required_age",
        "metacritic_score",
        "recommendation_count",
    ],
    "users": [
        "products",
        "reviews",
    ],
}


OUTLIER_SCHEMA = StructType([
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("non_null_count", LongType(), True),
    StructField("outlier_count", LongType(), True),
    StructField("outlier_rate", DoubleType(), True),
    StructField("q1", DoubleType(), True),
    StructField("q3", DoubleType(), True),
    StructField("iqr", DoubleType(), True),
    StructField("lower_bound", DoubleType(), True),
    StructField("upper_bound", DoubleType(), True),
    StructField("min_value", DoubleType(), True),
    StructField("max_value", DoubleType(), True),
    StructField("mean_value", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
])


def empty_outlier_df(spark):
    return spark.createDataFrame([], OUTLIER_SCHEMA)


def safe_read_parquet(spark, path):
    try:
        return read_parquet(spark, path)
    except Exception as e:
        print(f"[WARN] Cannot read path: {path}")
        print(f"[WARN] {e}")
        return None


def profile_outliers_for_column(df, table_name, column_name):
    if column_name not in df.columns:
        return df.sparkSession.createDataFrame([], OUTLIER_SCHEMA)

    numeric_df = df.select(F.col(column_name).cast("double").alias(column_name))
    row_count = df.count()

    base_stats = numeric_df.agg(
        F.count(F.col(column_name)).cast("long").alias("non_null_count"),
        F.min(F.col(column_name)).cast("double").alias("min_value"),
        F.max(F.col(column_name)).cast("double").alias("max_value"),
        F.avg(F.col(column_name)).cast("double").alias("mean_value"),
    ).collect()[0]

    non_null_count = base_stats["non_null_count"]

    if non_null_count == 0:
        return df.sparkSession.createDataFrame(
            [(
                table_name,
                column_name,
                row_count,
                0,
                0,
                0.0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )],
            schema=OUTLIER_SCHEMA,
        )

    q1, q3 = numeric_df.approxQuantile(column_name, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outlier_count = (
        numeric_df.filter(
            F.col(column_name).isNotNull() &
            (
                (F.col(column_name) < F.lit(lower_bound)) |
                (F.col(column_name) > F.lit(upper_bound))
            )
        )
        .count()
    )

    outlier_rate = float(outlier_count / non_null_count) if non_null_count > 0 else 0.0

    return df.sparkSession.createDataFrame(
        [(
            table_name,
            column_name,
            int(row_count),
            int(non_null_count),
            int(outlier_count),
            float(outlier_rate),
            float(q1) if q1 is not None else None,
            float(q3) if q3 is not None else None,
            float(iqr) if iqr is not None else None,
            float(lower_bound) if lower_bound is not None else None,
            float(upper_bound) if upper_bound is not None else None,
            float(base_stats["min_value"]) if base_stats["min_value"] is not None else None,
            float(base_stats["max_value"]) if base_stats["max_value"] is not None else None,
            float(base_stats["mean_value"]) if base_stats["mean_value"] is not None else None,
            None,
        )],
        schema=OUTLIER_SCHEMA,
    ).withColumn("created_at", F.current_timestamp())


def profile_outliers(spark, df, table_name):
    print(f"=== START OUTLIER PROFILE: {table_name} ===")

    existing_cols = [c for c in OUTLIER_COLUMNS.get(table_name, []) if c in df.columns]

    if not existing_cols:
        print(f"[WARN] No matching outlier columns for {table_name}")
        return empty_outlier_df(spark)

    frames = [
        profile_outliers_for_column(df, table_name, c)
        for c in existing_cols
    ]

    result = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    print(f"=== END OUTLIER PROFILE: {table_name} ===")
    return result.orderBy("table_name", "column_name")


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_quality_outliers ===")

    datasets = [
        ("reviews", SILVER_REVIEWS),
        ("games", SILVER_GAMES),
        ("users", SILVER_USERS),
    ]

    frames = []
    for table_name, path in datasets:
        df = safe_read_parquet(spark, path)
        if df is None:
            print(f"[WARN] Missing silver path for {table_name}, skip.")
            continue
        frames.append(profile_outliers(spark, df, table_name))

    if not frames:
        result = empty_outlier_df(spark)
    else:
        result = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    write_parquet(
        result,
        SILVER_QUALITY_OUTLIERS,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_outliers ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-outliers").getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()