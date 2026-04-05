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
    BRONZE_PARQUET_ALL_REVIEWS,
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_USERS,
    BRONZE_PARQUET_STEAM_DESCRIPTIONS,
    BRONZE_PARQUET_STEAM_TAGS,
    BRONZE_PARQUET_STEAM_CATEGORIES,
    BRONZE_PARQUET_STEAM_GENRES,
    SILVER_REVIEWS,
    SILVER_GAMES,
    SILVER_USERS,
    SILVER_GAME_TEXT,
    SILVER_GAME_TAGS,
    SILVER_GAME_CATEGORIES,
    SILVER_GAME_GENRES,
    SILVER_REJECTED_ROOT,
    SILVER_QUALITY_OVERVIEW,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


OVERVIEW_SCHEMA = StructType([
    StructField("table_name", StringType(), True),
    StructField("bronze_row_count", LongType(), True),
    StructField("silver_row_count", LongType(), True),
    StructField("rejected_row_count", LongType(), True),
    StructField("silver_outlier_count", LongType(), True),
    StructField("rejected_rate", DoubleType(), True),
    StructField("retention_rate", DoubleType(), True),
    StructField("silver_outlier_rate", DoubleType(), True),
    StructField("row_delta", LongType(), True),
    StructField("column_count_bronze", LongType(), True),
    StructField("column_count_silver", LongType(), True),
    StructField("overview_created_at", TimestampType(), True),
])


def empty_overview_df(spark):
    return spark.createDataFrame([], OVERVIEW_SCHEMA)


def safe_read_parquet(spark, path):
    try:
        print(f"[READ] {path}")
        return read_parquet(spark, path)
    except Exception as e:
        print(f"[WARN] Cannot read path: {path}")
        print(f"[WARN] {e}")
        return None


def profile_dataset(spark, table_name, bronze_path, silver_path, rejected_path):
    print(f"=== START PROFILE: {table_name} ===")
    print(f"BRONZE   = {bronze_path}")
    print(f"SILVER   = {silver_path}")
    print(f"REJECTED = {rejected_path}")

    bronze_df = safe_read_parquet(spark, bronze_path)
    silver_df = safe_read_parquet(spark, silver_path)
    rejected_df = safe_read_parquet(spark, rejected_path)

    bronze_row_count = bronze_df.count() if bronze_df is not None else 0
    silver_row_count = silver_df.count() if silver_df is not None else 0
    rejected_row_count = rejected_df.count() if rejected_df is not None else 0

    column_count_bronze = len(bronze_df.columns) if bronze_df is not None else 0
    column_count_silver = len(silver_df.columns) if silver_df is not None else 0

    silver_outlier_count = 0
    if silver_df is not None and "is_outlier" in silver_df.columns:
        silver_outlier_count = silver_df.filter(F.col("is_outlier") == True).count()

    rejected_rate = 0.0
    retention_rate = 0.0
    silver_outlier_rate = 0.0

    if bronze_row_count > 0:
        rejected_rate = rejected_row_count / bronze_row_count
        retention_rate = silver_row_count / bronze_row_count

    if silver_row_count > 0:
        silver_outlier_rate = silver_outlier_count / silver_row_count

    row_delta = bronze_row_count - silver_row_count - rejected_row_count

    data = [(
        table_name,
        int(bronze_row_count),
        int(silver_row_count),
        int(rejected_row_count),
        int(silver_outlier_count),
        float(rejected_rate),
        float(retention_rate),
        float(silver_outlier_rate),
        int(row_delta),
        int(column_count_bronze),
        int(column_count_silver),
    )]

    result = spark.createDataFrame(
        data,
        [
            "table_name",
            "bronze_row_count",
            "silver_row_count",
            "rejected_row_count",
            "silver_outlier_count",
            "rejected_rate",
            "retention_rate",
            "silver_outlier_rate",
            "row_delta",
            "column_count_bronze",
            "column_count_silver",
        ],
    ).withColumn("overview_created_at", F.current_timestamp())

    print(f"=== END PROFILE: {table_name} ===")
    return result


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_quality_overview ===")

    datasets = [
        {
            "table_name": "reviews",
            "bronze_path": BRONZE_PARQUET_ALL_REVIEWS,
            "silver_path": SILVER_REVIEWS,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/reviews",
        },
        {
            "table_name": "games",
            "bronze_path": BRONZE_PARQUET_REC_GAMES,
            "silver_path": SILVER_GAMES,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/games",
        },
        {
            "table_name": "users",
            "bronze_path": BRONZE_PARQUET_REC_USERS,
            "silver_path": SILVER_USERS,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/users",
        },
        {
            "table_name": "game_text",
            "bronze_path": BRONZE_PARQUET_STEAM_DESCRIPTIONS,
            "silver_path": SILVER_GAME_TEXT,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/game_text",
        },
        {
            "table_name": "game_tags",
            "bronze_path": BRONZE_PARQUET_STEAM_TAGS,
            "silver_path": SILVER_GAME_TAGS,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/game_tags",
        },
        {
            "table_name": "game_categories",
            "bronze_path": BRONZE_PARQUET_STEAM_CATEGORIES,
            "silver_path": SILVER_GAME_CATEGORIES,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/game_categories",
        },
        {
            "table_name": "game_genres",
            "bronze_path": BRONZE_PARQUET_STEAM_GENRES,
            "silver_path": SILVER_GAME_GENRES,
            "rejected_path": f"{SILVER_REJECTED_ROOT}/game_genres",
        },
    ]

    frames = [
        profile_dataset(
            spark,
            ds["table_name"],
            ds["bronze_path"],
            ds["silver_path"],
            ds["rejected_path"],
        )
        for ds in datasets
    ]

    if not frames:
        result = empty_overview_df(spark)
    else:
        result = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    write_parquet(
        result,
        SILVER_QUALITY_OVERVIEW,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_overview ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-overview").getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()