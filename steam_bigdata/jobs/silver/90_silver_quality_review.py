from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BRONZE_PARQUET_ALL_REVIEWS,
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_USERS,
    SILVER_REVIEWS,
    SILVER_GAMES,
    SILVER_USERS,
    SILVER_REJECTED_ROOT,
    SILVER_QUALITY_OVERVIEW,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning


def profile_dataset(spark, table_name, bronze_path, silver_path, rejected_path):
    print(f"=== START PROFILE: {table_name} ===")

    bronze_df = read_parquet(spark, bronze_path)
    silver_df = read_parquet(spark, silver_path)
    rejected_df = read_parquet(spark, rejected_path)

    bronze_row_count = bronze_df.count()
    silver_row_count = silver_df.count()
    rejected_row_count = rejected_df.count()

    column_count_bronze = len(bronze_df.columns)
    column_count_silver = len(silver_df.columns)

    rejected_rate = 0.0
    retention_rate = 0.0

    if bronze_row_count and bronze_row_count > 0:
        rejected_rate = rejected_row_count / bronze_row_count
        retention_rate = silver_row_count / bronze_row_count

    data = [(
        table_name,
        bronze_row_count,
        silver_row_count,
        rejected_row_count,
        float(rejected_rate),
        float(retention_rate),
        column_count_bronze,
        column_count_silver,
    )]

    return spark.createDataFrame(
        data,
        [
            "table_name",
            "bronze_row_count",
            "silver_row_count",
            "rejected_row_count",
            "rejected_rate",
            "retention_rate",
            "column_count_bronze",
            "column_count_silver",
        ],
    )


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

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f)

    result = result.withColumn("created_at", F.current_timestamp())

    write_parquet(
        result,
        SILVER_QUALITY_OVERVIEW,
        mode="overwrite",
        num_partitions=1,
    )

    print("=== END silver_quality_overview ===")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-quality-overview").getOrCreate()
    main(spark)