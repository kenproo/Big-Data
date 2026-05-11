from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ====== CONFIG: sửa 2 path này cho đúng path của bạn ======
SOURCE_PATH = "gs://truong_bigdata_24032026_init/silver/reviews/batch_date=2026-03-24"
TARGET_PATH = "gs://truong_bigdata_24032026_init/silver/reviews_fixed/batch_date=2026-03-24"

MIN_EPOCH_SECONDS = 0
MAX_EPOCH_SECONDS = 4102444800  # 2100-01-01 00:00:00 UTC


def epoch_seconds_to_timestamp(raw_col_name):
    raw_col = F.col(raw_col_name).cast("long")

    return (
        F.when(
            raw_col.between(MIN_EPOCH_SECONDS, MAX_EPOCH_SECONDS),
            F.to_timestamp(F.from_unixtime(raw_col))
        )
        .otherwise(F.lit(None).cast("timestamp"))
    )


def fix_timestamp_columns(df):
    """
    Input columns are expected to be epoch seconds, for example:
    timestamp_created = 1583504092
    timestamp_updated = 1583504092

    Output:
    timestamp_created_raw: long
    timestamp_updated_raw: long
    timestamp_created: timestamp
    timestamp_updated: timestamp
    """

    if "timestamp_created" in df.columns:
        df = (
            df
            .withColumn("timestamp_created_raw", F.col("timestamp_created").cast("long"))
            .withColumn("timestamp_created", epoch_seconds_to_timestamp("timestamp_created_raw"))
        )

    if "timestamp_updated" in df.columns:
        df = (
            df
            .withColumn("timestamp_updated_raw", F.col("timestamp_updated").cast("long"))
            .withColumn("timestamp_updated", epoch_seconds_to_timestamp("timestamp_updated_raw"))
        )

    return df


def main():
    spark = (
        SparkSession.builder
        .appName("fix-all-reviews-timestamp-parquet")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )

    print("=== START fix timestamp parquet ===")
    print(f"SOURCE = {SOURCE_PATH}")
    print(f"TARGET = {TARGET_PATH}")

    df = spark.read.parquet(SOURCE_PATH)

    print("SOURCE SCHEMA")
    df.printSchema()

    print("SOURCE TIMESTAMP SAMPLE")
    df.select(
        *[c for c in ["timestamp_created", "timestamp_updated"] if c in df.columns]
    ).show(20, truncate=False)

    fixed_df = fix_timestamp_columns(df)

    print("FIXED SCHEMA")
    fixed_df.printSchema()

    print("FIXED TIMESTAMP SAMPLE")
    fixed_df.select(
        *[c for c in [
            "timestamp_created_raw",
            "timestamp_created",
            "timestamp_updated_raw",
            "timestamp_updated",
        ] if c in fixed_df.columns]
    ).show(20, truncate=False)

    print("TIMESTAMP QUALITY CHECK")
    fixed_df.select(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("timestamp_created_raw").isNull(), 1).otherwise(0)).alias("null_created_raw"),
        F.sum(F.when(F.col("timestamp_created").isNull(), 1).otherwise(0)).alias("null_created_ts"),
        F.sum(F.when(F.col("timestamp_updated_raw").isNull(), 1).otherwise(0)).alias("null_updated_raw"),
        F.sum(F.when(F.col("timestamp_updated").isNull(), 1).otherwise(0)).alias("null_updated_ts"),
        F.min("timestamp_created_raw").alias("min_created_raw"),
        F.max("timestamp_created_raw").alias("max_created_raw"),
        F.min("timestamp_updated_raw").alias("min_updated_raw"),
        F.max("timestamp_updated_raw").alias("max_updated_raw"),
    ).show(truncate=False)

    (
        fixed_df
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(TARGET_PATH)
    )

    print("WRITE DONE")
    print("=== END fix timestamp parquet ===")

    spark.stop()


if __name__ == "__main__":
    main()