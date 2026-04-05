from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

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

    for c in ["products", "reviews"]:
        if c in df.columns:
            df = df.withColumn(c, safe_to_int(c))

    df = ensure_reason_cols(df)

    if "user_id" in df.columns:
        df = add_issue(df, F.col("user_id").isNull(), "missing_user_id")
    else:
        df = add_issue(df, F.lit(True), "missing_user_id_column")

    for c in ["products", "reviews"]:
        if c in df.columns:
            df = add_issue(df, F.col(c).isNotNull() & (F.col(c) < 0), f"negative_{c}")
            df = add_hard_upper_limit_issue(df, c, USER_COUNT_HARD_MAX)

    if "user_id" in df.columns:
        w = Window.partitionBy("user_id").orderBy(F.lit(1))
        df = df.withColumn("rn_user_id", F.row_number().over(w))
        df = add_issue(
            df,
            F.col("user_id").isNotNull() & (F.col("rn_user_id") > 1),
            "duplicate_user_id",
        )

    invalid_df = df.filter(F.col("quality_issue").isNotNull())
    valid_df = df.filter(F.col("quality_issue").isNull())

    outlier_cols = [c for c in ["products", "reviews"] if c in valid_df.columns]

    if outlier_cols:
        valid_df = flag_upper_quantile_outliers(
            valid_df, outlier_cols, quantile=0.999, rel_error=0.01
        )
        valid_df = cap_upper_quantile(
            valid_df, outlier_cols, quantile=0.999, rel_error=0.01, suffix="_capped"
        )
        valid_df = add_is_outlier_flag(valid_df)
    else:
        valid_df = valid_df.withColumn("is_outlier", F.lit(False))

    if "rn_user_id" in valid_df.columns:
        valid_df = valid_df.drop("rn_user_id")
    if "rn_user_id" in invalid_df.columns:
        invalid_df = invalid_df.drop("rn_user_id")

    valid_df = valid_df.withColumn("silver_processed_at", F.current_timestamp())
    invalid_df = invalid_df.withColumn("silver_processed_at", F.current_timestamp())

    write_parquet(valid_df, SILVER_USERS, mode="overwrite", num_partitions=50)
    write_parquet(invalid_df, f"{SILVER_REJECTED_ROOT}/users", mode="overwrite", num_partitions=10)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-users").getOrCreate()
    main(spark)
    spark.stop()