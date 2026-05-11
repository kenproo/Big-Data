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
    SILVER_REJECTED_ROOT,
    SILVER_QUALITY_ISSUES,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


ISSUE_SCHEMA = StructType([
    StructField("table_name", StringType(), True),
    StructField("issue_name", StringType(), True),
    StructField("issue_count", LongType(), True),
    StructField("issue_rate_on_bronze", DoubleType(), True),
    StructField("issue_rate_on_rejected", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
])


def empty_issue_df(spark):
    return spark.createDataFrame([], ISSUE_SCHEMA)


def safe_read_parquet(spark, path):
    try:
        return read_parquet(spark, path)
    except Exception as e:
        print(f"[WARN] Cannot read path: {path}")
        print(f"[WARN] {e}")
        return None


def build_issue_metrics(spark, table_name, bronze_path, rejected_path):
    print(f"=== START ISSUES: {table_name} ===")
    print(f"BRONZE PATH   = {bronze_path}")
    print(f"REJECTED PATH = {rejected_path}")

    bronze_df = safe_read_parquet(spark, bronze_path)
    rejected_df = safe_read_parquet(spark, rejected_path)

    if bronze_df is None:
        print(f"[WARN] Missing bronze data for {table_name}, return empty.")
        return empty_issue_df(spark)

    bronze_count = bronze_df.count()

    if rejected_df is None:
        print(f"[WARN] Missing rejected data for {table_name}, return empty.")
        return empty_issue_df(spark)

    if "quality_issue" not in rejected_df.columns:
        print(f"[WARN] quality_issue column not found in rejected {table_name}, return empty.")
        return empty_issue_df(spark)

    rejected_total = rejected_df.count()
    if rejected_total == 0:
        print(f"[WARN] rejected_df is empty for {table_name}, return empty.")
        return empty_issue_df(spark)

    issue_col = F.col("quality_issue")

    issue_array = (
        F.when(issue_col.isNull(), F.array().cast("array<string>"))
        .when(F.instr(issue_col.cast("string"), ",") > 0, F.split(issue_col.cast("string"), r"\s*,\s*"))
        .otherwise(F.array(issue_col.cast("string")))
    )

    exploded = (
        rejected_df
        .withColumn("issue_name", F.explode(issue_array))
        .withColumn("issue_name", F.trim(F.col("issue_name")))
        .filter(F.col("issue_name").isNotNull() & (F.col("issue_name") != ""))
    )

    result = (
        exploded.groupBy("issue_name")
        .agg(F.count("*").cast("long").alias("issue_count"))
        .withColumn("table_name", F.lit(table_name))
        .withColumn(
            "issue_rate_on_bronze",
            F.when(F.lit(bronze_count) > 0, F.col("issue_count") / F.lit(bronze_count)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "issue_rate_on_rejected",
            F.when(F.lit(rejected_total) > 0, F.col("issue_count") / F.lit(rejected_total)).otherwise(F.lit(0.0)),
        )
        .withColumn("created_at", F.current_timestamp())
        .select(
            "table_name",
            "issue_name",
            "issue_count",
            "issue_rate_on_bronze",
            "issue_rate_on_rejected",
            "created_at",
        )
        .orderBy(F.desc("issue_count"), F.asc("issue_name"))
    )

    print(f"=== END ISSUES: {table_name} ===")
    return result


def main(spark):
    apply_spark_tuning(spark)

    print("=== START silver_quality_issues ===")

    datasets = [
        ("reviews", BRONZE_PARQUET_ALL_REVIEWS, f"{SILVER_REJECTED_ROOT}/reviews"),
        ("games", BRONZE_PARQUET_REC_GAMES, f"{SILVER_REJECTED_ROOT}/games"),
        ("users", BRONZE_PARQUET_REC_USERS, f"{SILVER_REJECTED_ROOT}/users"),
    ]

    frames = [
        build_issue_metrics(spark, table_name, bronze_path, rejected_path)
        for table_name, bronze_path, rejected_path in datasets
    ]

    non_empty_frames = [f for f in frames if f is not None]

    if not non_empty_frames:
        result = empty_issue_df(spark)
    else:
        result = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), non_empty_frames)

    write_parquet(
        result,
        SILVER_QUALITY_ISSUES,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_issues ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-issues").getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()