from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from steam_bigdata.common.config import (
    BUCKET,
    BATCH_DATE,
    SILVER_REVIEWS,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.spark_config import apply_spark_tuning

GOLD_REVIEWS_BI = f"{BUCKET}/gold/reviews_bi/batch_date={BATCH_DATE}"


def main(spark):
    apply_spark_tuning(spark)

    print("=== START gold_reviews_bi ===")
    print(f"INPUT  = {SILVER_REVIEWS}")
    print(f"OUTPUT = {GOLD_REVIEWS_BI}")

    df = read_parquet(spark, SILVER_REVIEWS)
    print("READ DONE")
    print("INPUT COLUMNS =", df.columns)

    # Chỉ giữ các cột an toàn và hữu ích cho BI / dashboard
    preferred_cols = [
        "review_id",
        "app_id",
        "user_id",
        "review_text",
        "review_text_length",
        "is_recommended",
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
        "hidden_in_steam_china",
        "hours",
        "helpful",
        "funny",
        "comment_count",
        "weighted_vote_score",
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "is_outlier",
        "quality_issue",
        "quality_reason",
        "silver_processed_at",
    ]

    existing_cols = [c for c in preferred_cols if c in df.columns]
    bi_df = df.select(*existing_cols)

    # Chuẩn hóa kiểu dữ liệu để BigQuery load ổn hơn
    cast_to_string = ["review_id", "app_id", "user_id", "review_text", "quality_issue", "quality_reason"]
    for c in cast_to_string:
        if c in bi_df.columns:
            bi_df = bi_df.withColumn(c, F.col(c).cast("string"))

    cast_to_long = [
        "review_text_length",
        "helpful",
        "funny",
        "comment_count",
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
    ]
    for c in cast_to_long:
        if c in bi_df.columns:
            bi_df = bi_df.withColumn(c, F.col(c).cast("long"))

    cast_to_double = ["hours", "weighted_vote_score"]
    for c in cast_to_double:
        if c in bi_df.columns:
            bi_df = bi_df.withColumn(c, F.col(c).cast("double"))

    cast_to_bool = [
        "is_recommended",
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
        "hidden_in_steam_china",
        "is_outlier",
    ]
    for c in cast_to_bool:
        if c in bi_df.columns:
            bi_df = bi_df.withColumn(c, F.col(c).cast("boolean"))

    # Không mang các cột timestamp_created / timestamp_updated sang BI
    # để tránh lỗi load BigQuery
    bi_df = bi_df.withColumn("batch_date", F.lit(BATCH_DATE))
    bi_df = bi_df.withColumn("bi_created_at", F.current_timestamp())

    # Có thể lọc bớt bản ghi bị issue nếu muốn dashboard sạch hơn:
    # bi_df = bi_df.filter(F.col("quality_issue").isNull())

    print("BI SCHEMA READY")
    print("OUTPUT COLUMNS =", bi_df.columns)

    write_parquet(
        bi_df,
        GOLD_REVIEWS_BI,
        mode="overwrite",
        num_partitions=80,
    )

    print("WRITE DONE")
    print("=== END gold_reviews_bi ===")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("gold-reviews-bi")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    try:
        main(spark)
    finally:
        spark.stop()