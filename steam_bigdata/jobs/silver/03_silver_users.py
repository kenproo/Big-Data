from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from steam_bigdata.common.config import (
    BRONZE_PARQUET_REC_USERS,
    SILVER_USERS,
    SILVER_REJECTED_ROOT,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.transforms import clean_string_col, safe_to_long, safe_to_int
from steam_bigdata.common.spark_config import apply_spark_tuning
from steam_bigdata.common.outliers import (
    ensure_reason_cols,
    add_issue,
    add_hard_upper_limit_issue,
    flag_upper_quantile_outliers,
    cap_upper_quantile,
    add_is_outlier_flag,
)


USER_COUNT_HARD_MAX = 10000000


def main(spark):
    apply_spark_tuning(spark)

    df = read_parquet(spark, BRONZE_PARQUET_REC_USERS)

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, clean_string_col(c))

    if "user_id" in df.columns:
        df = df.withColumn("user_id", safe_to_long("user_id"))

    for c in ["products", "reviews", "user_reviews"]:
        if c in df.columns:
            df = df.withColumn(c, safe_to_int(c))

    df = ensure_reason_cols(df)
    df = add_issue(df, F.col("user_id").isNull(), "missing_user_id")

    for c in ["products", "reviews", "user_reviews"]:
        if c in df.columns:
            df = add_issue(df, F.col(c).isNotNull() & (F.col(c) < 0), f"negative_{c}")
            df = add_hard_upper_limit_issue(df, c, USER_COUNT_HARD_MAX)

    if "user_id" in df.columns:
        df = df.dropDuplicates(["user_id"])

    invalid_df = df.filter(F.col("quality_issue").isNotNull())
    valid_df = df.filter(F.col("quality_issue").isNull())

    outlier_cols = [c for c in ["products", "reviews", "user_reviews"] if c in valid_df.columns]
    valid_df = flag_upper_quantile_outliers(valid_df, outlier_cols, quantile=0.999, rel_error=0.01)
    valid_df = cap_upper_quantile(valid_df, outlier_cols, quantile=0.999, rel_error=0.01, suffix="_capped")
    valid_df = add_is_outlier_flag(valid_df)

    write_parquet(valid_df, SILVER_USERS, mode="overwrite", num_partitions=50)
    write_parquet(invalid_df, f"{SILVER_REJECTED_ROOT}/users", mode="overwrite", num_partitions=10)
if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-users").getOrCreate()
    main(spark)