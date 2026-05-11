"""
train_als.py

Train ALS Collaborative Filtering for Steam recommendation.

Mục đích:
- Đọc interaction data từ BigQuery.
- Encode user_id và app_id sang dạng số nguyên.
- Train ALS bằng PySpark.
- Sinh Top-K recommendation cho một tập user demo.
- Normalize ALS score về [0, 1].
- Ghi kết quả ra GCS dưới dạng Parquet.
- Có thể load tiếp vào BigQuery bằng file load_to_bigquery.py.

Cách chạy mẫu trên Dataproc / Spark:

spark-submit train_als.py \
  --project_id project-79499e5c-69d7-42b8-864 \
  --dataset steam_gold \
  --input_table gold_als_interaction_filtered \
  --output_path gs://truong_bigdata_24032026_init/gold/gold_recommendations_top30_10000users_demo \
  --top_k 30 \
  --user_limit 10000
"""

import argparse
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.recommendation import ALS


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset", default="steam_gold")
    parser.add_argument("--input_table", default="gold_als_interaction_filtered")
    parser.add_argument("--output_path", required=True)

    parser.add_argument("--user_col", default="user_id")
    parser.add_argument("--item_col", default="app_id")
    parser.add_argument("--rating_col", default="interaction_strength")

    parser.add_argument("--top_k", type=int, default=30)
    parser.add_argument("--user_limit", type=int, default=10000)

    parser.add_argument("--rank", type=int, default=64)
    parser.add_argument("--max_iter", type=int, default=10)
    parser.add_argument("--reg_param", type=float, default=0.08)
    parser.add_argument("--alpha", type=float, default=20.0)

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("steam-als-recommendation")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    full_input_table = f"{args.project_id}.{args.dataset}.{args.input_table}"

    print(f"Reading BigQuery table: {full_input_table}")

    df = (
        spark.read.format("bigquery")
        .option("table", full_input_table)
        .load()
    )

    # Chỉ giữ interaction hợp lệ.
    interactions = (
        df.select(
            F.col(args.user_col).cast("string").alias("user_id_raw"),
            F.col(args.item_col).cast("string").alias("app_id_raw"),
            F.col(args.rating_col).cast("double").alias("rating")
        )
        .where(F.col("user_id_raw").isNotNull())
        .where(F.col("app_id_raw").isNotNull())
        .where(F.col("rating").isNotNull())
    )

    # Nếu interaction_strength có giá trị âm, ALS implicit cần confidence không âm.
    # Ta dùng positive strength cho ALS demo.
    interactions = (
        interactions
        .withColumn("rating", F.greatest(F.col("rating"), F.lit(0.0)))
        .where(F.col("rating") > 0)
    )

    print("Encoding user_id and app_id...")

    user_map = (
        interactions
        .select("user_id_raw")
        .distinct()
        .withColumn("user_idx", F.row_number().over(Window.orderBy("user_id_raw")) - 1)
    )

    item_map = (
        interactions
        .select("app_id_raw")
        .distinct()
        .withColumn("item_idx", F.row_number().over(Window.orderBy("app_id_raw")) - 1)
    )

    encoded = (
        interactions
        .join(user_map, on="user_id_raw", how="inner")
        .join(item_map, on="app_id_raw", how="inner")
        .select(
            F.col("user_id_raw").alias("user_id"),
            F.col("app_id_raw").alias("app_id"),
            F.col("user_idx").cast("int"),
            F.col("item_idx").cast("int"),
            F.col("rating").cast("float")
        )
        .dropDuplicates(["user_idx", "item_idx"])
        .cache()
    )

    print("Interaction count:", encoded.count())
    print("User count:", encoded.select("user_idx").distinct().count())
    print("Item count:", encoded.select("item_idx").distinct().count())

    # Chọn user demo: ưu tiên user có nhiều interaction.
    demo_users = (
        encoded
        .groupBy("user_idx")
        .agg(F.count("*").alias("interaction_count"))
        .orderBy(F.desc("interaction_count"))
        .limit(args.user_limit)
        .select("user_idx")
    )

    print(f"Training ALS: rank={args.rank}, max_iter={args.max_iter}, reg={args.reg_param}")

    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        implicitPrefs=True,
        rank=args.rank,
        maxIter=args.max_iter,
        regParam=args.reg_param,
        alpha=args.alpha,
        coldStartStrategy="drop",
        nonnegative=True,
        seed=42
    )

    model = als.fit(encoded)

    print(f"Generating Top-{args.top_k} recommendations for {args.user_limit} users...")

    recs = model.recommendForUserSubset(demo_users, args.top_k)

    exploded = (
        recs
        .select(
            "user_idx",
            F.posexplode("recommendations").alias("pos", "rec")
        )
        .select(
            "user_idx",
            (F.col("pos") + 1).alias("rank"),
            F.col("rec.item_idx").alias("item_idx"),
            F.col("rec.rating").alias("raw_als_score")
        )
    )

    # Join ngược về user_id/app_id gốc.
    result = (
        exploded
        .join(user_map, on="user_idx", how="left")
        .join(item_map, on="item_idx", how="left")
        .select(
            F.col("user_id_raw").alias("user_id"),
            F.col("app_id_raw").cast("int").alias("app_id"),
            F.col("rank").cast("int"),
            F.col("raw_als_score").cast("double")
        )
    )

    # Normalize score về [0, 1] để dễ kết hợp hybrid.
    score_stats = result.agg(
        F.min("raw_als_score").alias("min_score"),
        F.max("raw_als_score").alias("max_score")
    ).collect()[0]

    min_score = float(score_stats["min_score"])
    max_score = float(score_stats["max_score"])

    if max_score > min_score:
        result = result.withColumn(
            "als_score",
            (F.col("raw_als_score") - F.lit(min_score)) / F.lit(max_score - min_score)
        )
    else:
        result = result.withColumn("als_score", F.lit(0.0))

    result = (
        result
        .select(
            "user_id",
            "app_id",
            "rank",
            "als_score",
            "raw_als_score",
            F.current_timestamp().alias("generated_at")
        )
        .orderBy("user_id", "rank")
    )

    print(f"Writing recommendations to: {args.output_path}")

    (
        result
        .repartition(32)
        .write
        .mode("overwrite")
        .parquet(args.output_path)
    )

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
