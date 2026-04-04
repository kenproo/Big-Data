from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from steam_bigdata.common.config import (
    BRONZE_PARQUET_REC_GAMES,
    BRONZE_PARQUET_REC_GAMES_META,
    BRONZE_PARQUET_STEAM_GAMES,
    BRONZE_PARQUET_STEAM_REVIEWS,
    BRONZE_PARQUET_STEAMSPY,
    BRONZE_PARQUET_STEAM_PROMOTIONAL,
    SILVER_GAMES,
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
    if "app_id" in df.columns:
        df = df.withColumn("app_id", safe_to_long("app_id"))
    return df


def pick_best_per_app(df):
    if "app_id" not in df.columns:
        return df
    w = Window.partitionBy("app_id").orderBy(F.lit(1))
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def main(spark):
    apply_spark_tuning(spark)

    rec_games = pick_best_per_app(clean_df(read_parquet(spark, BRONZE_PARQUET_REC_GAMES)))
    rec_meta = pick_best_per_app(clean_df(read_parquet(spark, BRONZE_PARQUET_REC_GAMES_META)))
    steam_games = pick_best_per_app(clean_df(read_parquet(spark, BRONZE_PARQUET_STEAM_GAMES)))
    steam_reviews = pick_best_per_app(clean_df(read_parquet(spark, BRONZE_PARQUET_STEAM_REVIEWS)))
    steamspy = pick_best_per_app(clean_df(read_parquet(spark, BRONZE_PARQUET_STEAMSPY)))
    promotional = pick_best_per_app(clean_df(read_parquet(spark, BRONZE_PARQUET_STEAM_PROMOTIONAL)))

    rec_meta = F.broadcast(rec_meta)
    promotional = F.broadcast(promotional)

    games = (
        steam_games.alias("g")
        .join(rec_games.alias("rg"), on="app_id", how="full")
        .join(rec_meta.alias("rm"), on="app_id", how="left")
        .join(steam_reviews.alias("sr"), on="app_id", how="left")
        .join(steamspy.alias("ss"), on="app_id", how="left")
        .join(promotional.alias("pm"), on="app_id", how="left")
    )

    candidate_name_cols = [c for c in ["name", "game", "title"] if c in games.columns]
    if candidate_name_cols:
        games = games.withColumn("game_name", F.coalesce(*[F.col(c) for c in candidate_name_cols]))

    if "is_free" in games.columns:
        games = games.withColumn(
            "is_free_clean",
            F.when(F.col("is_free").cast("int") == 1, F.lit(True))
             .when(F.col("is_free").cast("int") == 0, F.lit(False))
             .otherwise(F.col("is_free").cast("boolean"))
        )

    for c in ["recommendations", "reviews", "positive", "negative", "userscore", "score_rank", "metacritic_score"]:
        if c in games.columns:
            games = games.withColumn(c, safe_to_double(c))

    if "release_date" in games.columns:
        games = games.withColumn("release_date", F.to_date("release_date"))

    games = ensure_reason_cols(games)

    games = add_issue(games, F.col("app_id").isNull(), "missing_app_id")

    for c in ["recommendations", "reviews", "positive", "negative"]:
        if c in games.columns:
            games = add_issue(
                games,
                F.col(c).isNotNull() & (F.col(c) < 0),
                f"negative_{c}",
            )

    games = add_hard_range_issue(games, "metacritic_score", min_value=0.0, max_value=100.0, issue_name="invalid_metacritic_score")
    games = add_hard_range_issue(games, "userscore", min_value=0.0, max_value=10.0, issue_name="invalid_userscore")

    if "release_date" in games.columns:
        games = add_issue(
            games,
            F.col("release_date").isNotNull() &
            (F.col("release_date") > F.current_date() + F.expr("INTERVAL 30 DAYS")),
            "release_date_too_far_in_future",
        )

    games = games.filter(F.col("app_id").isNotNull()).dropDuplicates(["app_id"])

    invalid_df = games.filter(F.col("quality_issue").isNotNull())
    valid_df = games.filter(F.col("quality_issue").isNull())

    outlier_cols = [c for c in ["recommendations", "reviews", "positive", "negative"] if c in valid_df.columns]
    valid_df = flag_upper_quantile_outliers(valid_df, outlier_cols, quantile=0.999, rel_error=0.01)
    valid_df = cap_upper_quantile(valid_df, outlier_cols, quantile=0.999, rel_error=0.01, suffix="_capped")

    if "game_name" in valid_df.columns:
        valid_df = valid_df.withColumn("game_name_length", F.length("game_name"))
        valid_df = flag_upper_quantile_outliers(valid_df, ["game_name_length"], quantile=0.999, rel_error=0.01)

    valid_df = add_is_outlier_flag(valid_df)

    write_parquet(valid_df, SILVER_GAMES, mode="overwrite", num_partitions=80)
    write_parquet(invalid_df, f"{SILVER_REJECTED_ROOT}/games", mode="overwrite", num_partitions=20)
if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-games").getOrCreate()
    main(spark)