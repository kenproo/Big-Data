from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

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

# -------------------- CONFIG --------------------
MIN_REVIEW_TEXT_LENGTH = 10
MAX_HOURS = 50000.0

CHECKPOINT_DIR = "gs://truong_bigdata_24032026_init/tmp/checkpoints"


# -------------------- UTILS --------------------
def rename_raw_columns(df):
    rename_map = {
        "recommendationid": "review_id",
        "appid": "app_id",
        "author_steamid": "user_id",
        "author_playtime_at_review": "hours",
        "votes_up": "helpful",
        "votes_funny": "funny",
        "review": "review_text",

        # Keep timestamp/date fields as raw values for later BigQuery processing.
        # Do not parse them in Silver PySpark.
        "date": "timestamp_created",
        "created": "timestamp_created",
        "created_at": "timestamp_created",
        "review_date": "timestamp_created",
        "timestamp": "timestamp_created",
        "time_created": "timestamp_created",

        "updated": "timestamp_updated",
        "updated_at": "timestamp_updated",
        "time_updated": "timestamp_updated",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def clean_df(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, clean_string_col(c))

    long_cols = [
        "app_id",
        "review_id",
        "user_id",
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "comment_count",
        "helpful",
        "funny",
    ]

    for c in long_cols:
        if c in df.columns:
            df = df.withColumn(c, safe_to_long(c))

    double_cols = [
        "hours",
        "weighted_vote_score",
    ]

    for c in double_cols:
        if c in df.columns:
            df = df.withColumn(c, safe_to_double(c))

    return df


def normalize_boolean_cols(df):
    bool_like_cols = [
        "is_recommended",
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
        "hidden_in_steam_china",
    ]

    for c in bool_like_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    F.lower(F.col(c).cast("string")).isin("true", "1", "yes"),
                    F.lit(True),
                )
                .when(
                    F.lower(F.col(c).cast("string")).isin("false", "0", "no"),
                    F.lit(False),
                )
                .otherwise(F.col(c).cast("boolean")),
            )

    return df


def keep_timestamp_raw_for_bigquery(df):
    """
    Keep timestamp/date columns as raw string values.
    Timestamp parsing will be handled later in BigQuery.
    """

    timestamp_cols = [
        "timestamp_created",
        "timestamp_updated",
    ]

    for c in timestamp_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("string"))

    return df


def add_duplicate_review_issue(df):
    if "review_id" not in df.columns:
        return df

    w = Window.partitionBy("review_id")

    df = df.withColumn("_review_id_cnt", F.count("*").over(w))

    df = add_issue(
        df,
        F.col("review_id").isNotNull() & (F.col("_review_id_cnt") > 1),
        "duplicate_review_id",
    )

    return df.drop("_review_id_cnt")


# -------------------- MAIN --------------------
def main(spark):
    apply_spark_tuning(spark)
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

    print("=== START silver_reviews ===")
    print(f"INPUT = {BRONZE_PARQUET_ALL_REVIEWS}")
    print(f"OUTPUT VALID = {SILVER_REVIEWS}")
    print(f"OUTPUT INVALID = {SILVER_REJECTED_ROOT}/reviews")

    df = read_parquet(spark, BRONZE_PARQUET_ALL_REVIEWS)

    print("READ DONE")
    print("RAW COLUMNS =", df.columns)

    df = rename_raw_columns(df)

    print("RENAMED COLUMNS =", df.columns)

    # Debug timestamp/date columns after rename.
    date_like_cols = [
        c for c in df.columns
        if "time" in c.lower()
        or "date" in c.lower()
        or "created" in c.lower()
        or "updated" in c.lower()
    ]
    print("DATE/TIME COLUMNS AFTER RENAME =", date_like_cols)

    df = clean_df(df)
    df = normalize_boolean_cols(df)

    # Keep timestamp/date columns raw for later BigQuery processing.
    # Do not parse timestamp in Silver PySpark job.
    df = keep_timestamp_raw_for_bigquery(df)

    df = ensure_reason_cols(df)

    df = df.withColumn("silver_processed_at", F.current_timestamp())

    print("NORMALIZE DONE")

    # -------------------- HARD KEY ISSUES --------------------
    if "review_id" in df.columns:
        df = add_issue(df, F.col("review_id").isNull(), "missing_review_id")
    else:
        df = add_issue(df, F.lit(True), "missing_review_id_column")

    if "app_id" in df.columns:
        df = add_issue(df, F.col("app_id").isNull(), "missing_app_id")
    else:
        df = add_issue(df, F.lit(True), "missing_app_id_column")

    if "user_id" in df.columns:
        df = add_issue(df, F.col("user_id").isNull(), "missing_user_id")
    else:
        df = add_issue(df, F.lit(True), "missing_user_id_column")

    df = add_duplicate_review_issue(df)

    # -------------------- TEXT QUALITY ISSUES --------------------
    if "review_text" in df.columns:
        df = df.withColumn(
            "review_text_length",
            F.length(F.trim(F.col("review_text"))),
        )

        df = add_issue(
            df,
            F.col("review_text").isNull()
            | (F.length(F.trim(F.col("review_text"))) == 0),
            "empty_review_text",
        )

        df = add_issue(
            df,
            F.col("review_text").isNotNull()
            & (F.length(F.trim(F.col("review_text"))) > 0)
            & (F.length(F.trim(F.col("review_text"))) < MIN_REVIEW_TEXT_LENGTH),
            "too_short_review_text",
        )
    else:
        df = add_issue(df, F.lit(True), "missing_review_text_column")

    # -------------------- NUMERIC QUALITY ISSUES --------------------
    for c in [
        "author_num_games_owned",
        "author_num_reviews",
        "author_playtime_forever",
        "author_playtime_last_two_weeks",
        "hours",
        "helpful",
        "funny",
        "comment_count",
    ]:
        if c in df.columns:
            df = add_issue(
                df,
                F.col(c).isNotNull() & (F.col(c) < 0),
                f"negative_{c}",
            )

    if "hours" in df.columns:
        df = add_issue(
            df,
            F.col("hours").isNotNull() & (F.col("hours") > MAX_HOURS),
            "too_large_hours",
        )

    # -------------------- TIMESTAMP QUALITY ISSUES --------------------
    # Skipped intentionally.
    # timestamp_created / timestamp_updated are kept as raw string values
    # and will be processed later in BigQuery.

    if "weighted_vote_score" in df.columns:
        df = add_hard_range_issue(
            df,
            "weighted_vote_score",
            min_value=0.0,
            max_value=1.0,
            issue_name="invalid_weighted_vote_score",
        )

    print("ISSUE RULES DONE")

    # -------------------- CHECKPOINT --------------------
    if "review_id" in df.columns:
        df = df.repartition(200, "review_id")
    else:
        df = df.repartition(200)

    df = df.checkpoint(eager=False)

    print("CHECKPOINT SET")

    # -------------------- SPLIT VALID / INVALID --------------------
    hard_issue_patterns = [
        "missing_review_id",
        "missing_review_id_column",
        "missing_app_id",
        "missing_app_id_column",
        "missing_user_id",
        "missing_user_id_column",
        "duplicate_review_id",
        "empty_review_text",
        "too_short_review_text",
        "missing_review_text_column",
        "too_large_hours",
    ]

    hard_invalid_condition = F.lit(False)

    for pattern in hard_issue_patterns:
        hard_invalid_condition = (
            hard_invalid_condition | F.col("quality_issue").contains(pattern)
        )

    invalid_df = (
        df.filter(F.col("quality_issue").isNotNull() & hard_invalid_condition)
        .repartition(40)
    )

    valid_df = (
        df.filter(F.col("quality_issue").isNull() | (~hard_invalid_condition))
        .repartition(160)
    )

    # -------------------- OUTLIER PROCESS --------------------
    outlier_cols = [
        c
        for c in [
            "author_num_games_owned",
            "author_num_reviews",
            "author_playtime_forever",
            "author_playtime_last_two_weeks",
            "hours",
            "helpful",
            "funny",
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

    if "review_text_length" in valid_df.columns:
        valid_df = flag_upper_quantile_outliers(
            valid_df,
            ["review_text_length"],
            quantile=0.999,
            rel_error=0.01,
        )

    valid_df = add_is_outlier_flag(valid_df)

    print("OUTLIER PROCESS DONE")

    # -------------------- WRITE OVER EXISTING DATA --------------------
    write_parquet(
        valid_df,
        SILVER_REVIEWS,
        mode="overwrite",
        num_partitions=160,
    )

    print("WRITE VALID DONE - overwrote existing silver_reviews")

    write_parquet(
        invalid_df,
        f"{SILVER_REJECTED_ROOT}/reviews",
        mode="overwrite",
        num_partitions=40,
    )

    print("WRITE INVALID DONE - overwrote existing rejected reviews")
    print("=== END silver_reviews ===")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("silver-reviews")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )

    try:
        main(spark)
    finally:
        spark.stop()