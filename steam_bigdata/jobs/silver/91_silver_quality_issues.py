from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BRONZE_PARQUET_ALL_REVIEWS,
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_USERS,
    SILVER_REJECTED_ROOT,
    SILVER_QUALITY_ISSUES,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


def build_issue_metrics(spark, table_name, bronze_path, rejected_path):
    print(f"=== START ISSUES: {table_name} ===")

    bronze_df = read_parquet(spark, bronze_path)
    rejected_df = read_parquet(spark, rejected_path)

    bronze_count = bronze_df.count()

    if "quality_issue" not in rejected_df.columns:
        return spark.createDataFrame(
            [],
            schema="""
                table_name string,
                issue_name string,
                issue_count long,
                issue_rate_on_bronze double,
                issue_rate_on_rejected double,
                created_at timestamp
            """,
        )

    issue_col = F.col("quality_issue")

    issue_array = (
        F.when(issue_col.isNull(), F.array().cast("array<string>"))
        .when(issue_col.cast("string").contains(","), F.split(issue_col.cast("string"), r"\s*,\s*"))
        .otherwise(F.array(issue_col.cast("string")))
    )

    exploded = (
        rejected_df
        .withColumn("issue_name", F.explode(issue_array))
        .filter(F.col("issue_name").isNotNull() & (F.trim(F.col("issue_name")) != ""))
    )

    rejected_count_df = rejected_df.agg(F.count("*").alias("rejected_total"))

    result = (
        exploded.groupBy("issue_name")
        .agg(F.count("*").alias("issue_count"))
        .crossJoin(rejected_count_df)
        .withColumn("table_name", F.lit(table_name))
        .withColumn(
            "issue_rate_on_bronze",
            F.when(F.lit(bronze_count) > 0, F.col("issue_count") / F.lit(bronze_count)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "issue_rate_on_rejected",
            F.when(F.col("rejected_total") > 0, F.col("issue_count") / F.col("rejected_total")).otherwise(F.lit(0.0)),
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
        .orderBy(F.desc("issue_count"))
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

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    write_parquet(
        result,
        SILVER_QUALITY_ISSUES,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_issues ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-issues").getOrCreate()
    main(spark)