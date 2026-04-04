from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from steam_bigdata.common.config import (
    BRONZE_PARQUET_STEAM_DESCRIPTIONS,
    SILVER_GAME_TEXT,
    SILVER_REJECTED_ROOT,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.transforms import safe_to_long, clean_html_text
from steam_bigdata.common.spark_config import apply_spark_tuning
from steam_bigdata.common.outliers import (
    ensure_reason_cols,
    add_issue,
    add_hard_upper_limit_issue,
    flag_upper_quantile_outliers,
    add_is_outlier_flag,
)


TEXT_HARD_MAX_LEN = 200000


def main(spark):
    apply_spark_tuning(spark)

    df = read_parquet(spark, BRONZE_PARQUET_STEAM_DESCRIPTIONS)

    keep_cols = [c for c in ["app_id", "summary", "extensive", "about"] if c in df.columns]
    df = df.select(*keep_cols)

    df = df.withColumn("app_id", safe_to_long("app_id"))

    for c in ["summary", "extensive", "about"]:
        if c in df.columns:
            df = df.withColumn(f"{c}_clean", clean_html_text(c))
            df = df.withColumn(f"{c}_len", F.length(F.col(f"{c}_clean")))

    df = ensure_reason_cols(df)
    df = add_issue(df, F.col("app_id").isNull(), "missing_app_id")

    for c in ["summary_len", "extensive_len", "about_len"]:
        if c in df.columns:
            df = add_hard_upper_limit_issue(df, c, TEXT_HARD_MAX_LEN)

    df = df.dropDuplicates(["app_id"])

    invalid_df = df.filter(F.col("quality_issue").isNotNull())
    valid_df = df.filter(F.col("quality_issue").isNull())

    outlier_cols = [c for c in ["summary_len", "extensive_len", "about_len"] if c in valid_df.columns]
    valid_df = flag_upper_quantile_outliers(valid_df, outlier_cols, quantile=0.999, rel_error=0.01)
    valid_df = add_is_outlier_flag(valid_df)

    write_parquet(valid_df, SILVER_GAME_TEXT, mode="overwrite", num_partitions=40)
    write_parquet(invalid_df, f"{SILVER_REJECTED_ROOT}/game_text", mode="overwrite", num_partitions=10)
if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-game-text").getOrCreate()
    main(spark)