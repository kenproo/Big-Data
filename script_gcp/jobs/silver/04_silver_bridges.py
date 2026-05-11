from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from steam_bigdata.common.config import (
    BRONZE_PARQUET_STEAM_TAGS,
    BRONZE_PARQUET_STEAM_CATEGORIES,
    BRONZE_PARQUET_STEAM_GENRES,
    SILVER_GAME_TAGS,
    SILVER_GAME_CATEGORIES,
    SILVER_GAME_GENRES,
    SILVER_REJECTED_ROOT,
)
from steam_bigdata.common.io import read_parquet, write_parquet
from steam_bigdata.common.transforms import safe_to_long
from steam_bigdata.common.spark_config import apply_spark_tuning
from steam_bigdata.common.outliers import (
    ensure_reason_cols,
    add_issue,
    flag_upper_quantile_outliers,
    add_is_outlier_flag,
)


def build_bridge(df, value_col):
    df = (
        df.select("app_id", value_col)
          .withColumn("app_id", safe_to_long("app_id"))
          .withColumn(value_col, F.lower(F.trim(F.col(value_col))))
          .withColumn(value_col, F.regexp_replace(F.col(value_col), r"\s+", " "))
    )

    df = ensure_reason_cols(df)
    df = add_issue(df, F.col("app_id").isNull(), "missing_app_id")
    df = add_issue(df, F.col(value_col).isNull() | (F.col(value_col) == ""), f"missing_{value_col}")

    invalid_df = df.filter(F.col("quality_issue").isNotNull())

    valid_df = (
        df.filter(F.col("quality_issue").isNull())
          .dropDuplicates(["app_id", value_col])
          .withColumn("value_length", F.length(F.col(value_col)))
    )

    valid_df = flag_upper_quantile_outliers(valid_df, ["value_length"], quantile=0.999, rel_error=0.01)
    valid_df = add_is_outlier_flag(valid_df)

    return valid_df, invalid_df


def main(spark):
    apply_spark_tuning(spark)

    tags_valid, tags_invalid = build_bridge(read_parquet(spark, BRONZE_PARQUET_STEAM_TAGS), "tag")
    categories_valid, categories_invalid = build_bridge(read_parquet(spark, BRONZE_PARQUET_STEAM_CATEGORIES), "category")
    genres_valid, genres_invalid = build_bridge(read_parquet(spark, BRONZE_PARQUET_STEAM_GENRES), "genre")

    write_parquet(tags_valid, SILVER_GAME_TAGS, mode="overwrite", num_partitions=30)
    write_parquet(categories_valid, SILVER_GAME_CATEGORIES, mode="overwrite", num_partitions=20)
    write_parquet(genres_valid, SILVER_GAME_GENRES, mode="overwrite", num_partitions=20)

    write_parquet(tags_invalid, f"{SILVER_REJECTED_ROOT}/game_tags", mode="overwrite", num_partitions=5)
    write_parquet(categories_invalid, f"{SILVER_REJECTED_ROOT}/game_categories", mode="overwrite", num_partitions=5)
    write_parquet(genres_invalid, f"{SILVER_REJECTED_ROOT}/game_genres", mode="overwrite", num_partitions=5)
if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-bridges").getOrCreate()
    main(spark)