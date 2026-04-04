from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from steam_bigdata.common.config import (
    BRONZE_PARQUET_ALL_REVIEWS,
    SILVER_REVIEWS,
    SILVER_REJECTED_ROOT,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.transforms import clean_string_col, safe_to_long, safe_to_double
from steam_bigdata.common.spark_config import apply_spark_tuning
from steam_bigdata.common.outliers import (
    ensure_reason_cols,
    add_issue,
    add_hard_range_issue,
    flag_upper_quantile_outliers,
    cap_upper_quantile,
    add_is_outlier_flag,
)


def clean_df(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, clean_string_col(c))

    long_cols = [
        "app_id",
        "review_id",
        "author_steamid",
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "author_playtime_at_review",
        "comment_count",
        "votes_up",
        "votes_funny",
    ]
    for c in long_cols:
        if c in df.columns:
            df = df.withColumn(c, safe_to_long(c))

    double_cols = [
        "weighted_vote_score",
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
    ]
    for c in double_cols:
        if c in df.columns:
            df = df.withColumn(c, safe_to_double(c))

    return df


def normalize_boolean_cols(df):
    bool_like_cols = [
        "voted_up",
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
    ]

    for c in bool_like_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.lower(F.col(c).cast("string")).isin("true", "1", "yes"), F.lit(True))
                 .when(F.lower(F.col(c).cast("string")).isin("false", "0", "no"), F.lit(False))
                 .otherwise(F.col(c).cast("boolean"))
            )

    return df


def normalize_date_cols(df):
    if "timestamp_created" in df.columns:
        df = df.withColumn("timestamp_created", F.to_timestamp("timestamp_created"))

    if "timestamp_updated" in df.columns:
        df = df.withColumn("timestamp_updated", F.to_timestamp("timestamp_updated"))

    return df


def main(spark):
    apply_spark_tuning(spark)

    spark.sparkContext.setCheckpointDir(
        "gs://truong_bigdata_24032026_init/tmp/checkpoints"
    )

    print("=== START silver_reviews ===")
    print(f"INPUT = {BRONZE_PARQUET_ALL_REVIEWS}")
    print(f"OUTPUT VALID = {SILVER_REVIEWS}")
    print(f"OUTPUT INVALID = {SILVER_REJECTED_ROOT}/reviews")

    df = read_parquet(spark, BRONZE_PARQUET_ALL_REVIEWS)
    print("READ DONE")

    df = clean_df(df)
    df = normalize_boolean_cols(df)
    df = normalize_date_cols(df)
    df = ensure_reason_cols(df)
    print("NORMALIZE DONE")

    if "review_id" in df.columns:
        df = add_issue(df, F.col("review_id").isNull(), "missing_review_id")
    if "app_id" in df.columns:
        df = add_issue(df, F.col("app_id").isNull(), "missing_app_id")

    if "review" in df.columns:
        df = add_issue(
            df,
            F.col("review").isNull() | (F.length(F.trim(F.col("review"))) == 0),
            "empty_review_text",
        )

    for c in [
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "author_playtime_at_review",
        "votes_up",
        "votes_funny",
        "comment_count",
    ]:
        if c in df.columns:
            df = add_issue(
                df,
                F.col(c).isNotNull() & (F.col(c) < 0),
                f"negative_{c}",
            )

    for c in ["voted_up", "steam_purchase", "received_for_free", "written_during_early_access"]:
        if c in df.columns:
            df = add_issue(
                df,
                F.col(c).isNull(),
                f"invalid_boolean_{c}",
            )
ANCIENT_TS_CUTOFF = F.lit("1900-01-01 00:00:00").cast("timestamp")

if "timestamp_created" in df.columns:
    df = add_issue(
        df,
        F.col("timestamp_created").isNotNull() & (F.col("timestamp_created") < ANCIENT_TS_CUTOFF),
        "ancient_timestamp_created",
    )
    df = df.withColumn(
        "timestamp_created",
        F.when(F.col("timestamp_created") < ANCIENT_TS_CUTOFF, F.lit(None).cast("timestamp"))
         .otherwise(F.col("timestamp_created"))
    )

if "timestamp_updated" in df.columns:
    df = add_issue(
        df,
        F.col("timestamp_updated").isNotNull() & (F.col("timestamp_updated") < ANCIENT_TS_CUTOFF),
        "ancient_timestamp_updated",
    )
    df = df.withColumn(
        "timestamp_updated",
        F.when(F.col("timestamp_updated") < ANCIENT_TS_CUTOFF, F.lit(None).cast("timestamp"))
         .otherwise(F.col("timestamp_updated"))
    )
      
    df = add_hard_range_issue(
        df,
        "weighted_vote_score",
        min_value=0.0,
        max_value=1.0,
        issue_name="invalid_weighted_vote_score",
    )

    if "app_id" in df.columns:
        df = df.filter(F.col("app_id").isNotNull())

    if "review_id" in df.columns:
        df = df.repartition(200, "review_id")
        df = df.dropDuplicates(["review_id"])
    else:
        df = df.repartition(200)

    print("HARD RULES + DEDUP DONE")

    df = df.checkpoint(eager=False)
    print("CHECKPOINT SET")

    invalid_df = df.filter(F.col("quality_issue").isNotNull()).repartition(40)
    valid_df = df.filter(F.col("quality_issue").isNull()).repartition(160)

    print("SPLIT DONE")

    outlier_cols = [
        c for c in [
            "author_num_games_owned",
            "author_num_reviews",
            "author_playtime_forever",
            "author_playtime_last_two_weeks",
            "author_playtime_at_review",
            "votes_up",
            "votes_funny",
            "comment_count",
        ]
        if c in valid_df.columns
    ]

    if outlier_cols:
        valid_df = flag_upper_quantile_outliers(
            valid_df,
            outlier_cols,
            quantile=0.999,
            rel_error=0.01,
        )
        valid_df = cap_upper_quantile(
            valid_df,
            outlier_cols,
            quantile=0.999,
            rel_error=0.01,
            suffix="_capped",
        )

    if "review" in valid_df.columns:
        valid_df = valid_df.withColumn("review_text_length", F.length("review"))
        valid_df = flag_upper_quantile_outliers(
            valid_df,
            ["review_text_length"],
            quantile=0.999,
            rel_error=0.01,
        )

    valid_df = add_is_outlier_flag(valid_df)

    print("OUTLIER PROCESS DONE")

    write_parquet(
        valid_df,
        SILVER_REVIEWS,
        mode="overwrite",
        num_partitions=160,
    )
    print("WRITE VALID DONE")

    write_parquet(
        invalid_df,
        f"{SILVER_REJECTED_ROOT}/reviews",
        mode="overwrite",
        num_partitions=40,
    )
    print("WRITE INVALID DONE")

    print("=== END silver_reviews ===")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("silver-reviews")
        .getOrCreate()
    )
    main(spark)