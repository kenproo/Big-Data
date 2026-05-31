from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
import argparse
import time


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_path", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--model_path", required=True)
    parser.add_argument("--recs_path", required=True)

    parser.add_argument("--rank", type=int, default=16)
    parser.add_argument("--max_iter", type=int, default=5)
    parser.add_argument("--reg_param", type=float, default=0.1)
    parser.add_argument("--alpha", type=float, default=20.0)
    parser.add_argument("--top_k", type=int, default=50)
    parser.add_argument("--num_partitions", type=int, default=300)

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Steam ALS Training Serverless")
        .getOrCreate()
    )

    spark.sparkContext.setCheckpointDir(args.checkpoint_path)

    spark.conf.set("spark.sql.shuffle.partitions", str(args.num_partitions))
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    print("========== LOAD DATA ==========")

    df = spark.read.parquet(args.input_path)

    print("Input schema:")
    df.printSchema()

    print("Input split stats:")
    df.groupBy("split").count().show(truncate=False)

    print("Input rating stats:")
    df.select(
        F.count("*").alias("rows"),
        F.countDistinct("user_idx").alias("num_users"),
        F.countDistinct("app_idx").alias("num_games"),
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
        F.max("rating").alias("max_rating"),
        F.countDistinct("rating").alias("distinct_rating")
    ).show(truncate=False)

    print("========== PREPARE TRAIN DATA ==========")

    train_df = (
        df
        .filter(F.col("split") == "train")
        .select(
            F.col("user_idx").cast("int").alias("user_idx"),
            F.col("app_idx").cast("int").alias("app_idx"),
            F.col("rating").cast("float").alias("rating")
        )
        .filter(
            F.col("user_idx").isNotNull()
            & F.col("app_idx").isNotNull()
            & F.col("rating").isNotNull()
            & (F.col("rating") > 0)
        )
        .repartition(args.num_partitions, "user_idx")
        .cache()
    )

    train_count = train_df.count()
    print(f"Train rows: {train_count:,}")

    if train_count == 0:
        raise ValueError(
            "Train data is empty. Check column `split`. "
            "Expected rows where split = 'train'."
        )

    print("Train stats:")
    train_df.select(
        F.count("*").alias("rows"),
        F.countDistinct("user_idx").alias("num_users"),
        F.countDistinct("app_idx").alias("num_games"),
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
        F.max("rating").alias("max_rating"),
        F.countDistinct("rating").alias("distinct_rating")
    ).show(truncate=False)

    print("========== TRAIN ALS ==========")

    als = ALS(
        userCol="user_idx",
        itemCol="app_idx",
        ratingCol="rating",
        implicitPrefs=True,
        rank=args.rank,
        maxIter=args.max_iter,
        regParam=args.reg_param,
        alpha=args.alpha,
        coldStartStrategy="drop",

        # Để False trước để tránh factor bị ép toàn 0 khi debug.
        # Sau khi chạy ổn có thể thử lại True.
        nonnegative=False,

        checkpointInterval=2,
        seed=42
    )

    start = time.time()
    model = als.fit(train_df)
    end = time.time()

    print(f"ALS training finished in {(end - start) / 60:.2f} minutes")

    print("========== CHECK PREDICTION ==========")

    pred_sample = (
        model
        .transform(train_df.limit(1000000))
        .select("user_idx", "app_idx", "rating", "prediction")
    )

    print("Prediction summary:")
    pred_sample.select("rating", "prediction").summary().show()

    pred_sample.select(
        F.count("*").alias("rows"),
        F.min("prediction").alias("min_prediction"),
        F.avg("prediction").alias("avg_prediction"),
        F.max("prediction").alias("max_prediction"),
        F.countDistinct("prediction").alias("distinct_prediction")
    ).show(truncate=False)

    print("Top prediction sample:")
    pred_sample.orderBy(F.desc("prediction")).show(20, truncate=False)

    print("Lowest prediction sample:")
    pred_sample.orderBy(F.asc("prediction")).show(20, truncate=False)

    print("========== SAVE MODEL ==========")

    model.write().overwrite().save(args.model_path)
    print(f"Model saved to: {args.model_path}")

    print("========== GENERATE RECOMMENDATIONS ==========")

    user_recs = model.recommendForAllUsers(args.top_k)

    print("Raw recommendation schema:")
    user_recs.printSchema()

    print("Raw recommendation sample:")
    user_recs.show(3, truncate=False)

    flat_recs = (
        user_recs
        .select(
            F.col("user_idx"),
            F.explode("recommendations").alias("rec")
        )
        .select(
            F.col("user_idx").cast("int").alias("user_idx"),
            F.col("rec.app_idx").cast("int").alias("app_idx"),

            # Quan trọng: Spark ALS lưu score trong rec.rating
            F.col("rec.rating").cast("float").alias("als_score")
        )
    )

    print("Flat recommendation schema:")
    flat_recs.printSchema()

    print("ALS score summary:")
    flat_recs.select("als_score").summary().show()

    flat_recs.select(
        F.count("*").alias("rows"),
        F.countDistinct("user_idx").alias("num_users"),
        F.countDistinct("app_idx").alias("num_games"),
        F.min("als_score").alias("min_score"),
        F.avg("als_score").alias("avg_score"),
        F.max("als_score").alias("max_score"),
        F.countDistinct("als_score").alias("distinct_score")
    ).show(truncate=False)

    print("Recommendation length check:")

    rec_len_df = (
        flat_recs
        .groupBy("user_idx")
        .agg(F.count("*").alias("num_recs"))
    )

    rec_len_df.select("num_recs").summary().show()

    rec_len_df.groupBy("num_recs") \
        .count() \
        .orderBy("num_recs") \
        .show(100, truncate=False)

    print("========== SAVE RECOMMENDATIONS ==========")

    (
        flat_recs
        .repartition(args.num_partitions, "user_idx")
        .write
        .mode("overwrite")
        .parquet(args.recs_path)
    )

    print(f"Recommendations saved to: {args.recs_path}")

    train_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()