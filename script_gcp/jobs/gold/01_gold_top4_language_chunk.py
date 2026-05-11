from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# =========================================================
# CONFIG
# =========================================================

BUCKET = "gs://truong_bigdata_24032026_init"

INPUT_GOLD_REVIEWS_TOP4 = f"{BUCKET}/gold/gold_reviews_top4_languages"
OUTPUT_GOLD_REVIEW_CHUNKS = f"{BUCKET}/gold/gold_app_language_review_chunks_fast"

TOP_LANGUAGES = ["schinese", "english", "spanish", "russian"]

MIN_TEXT_LENGTH = 10

# Tăng reviews/chunk để giảm số chunk MiniLM.
REVIEWS_PER_CHUNK = 150

# Silver/Gold đã clean rồi, ở đây chỉ cắt ngắn để giảm memory + giảm thời gian MiniLM.
MAX_REVIEW_TEXT_CHARS = 350

# Text đưa vào MiniLM không nên quá dài.
MAX_MERGED_TEXT_CHARS = 3000

# Shuffle partitions cho Dataproc.
SHUFFLE_PARTITIONS = 1000


# =========================================================
# MAIN
# =========================================================

def main(spark):
    print("=== START gold_app_language_review_chunks FAST DYNAMIC BUCKET ===")
    print("INPUT =", INPUT_GOLD_REVIEWS_TOP4)
    print("OUTPUT =", OUTPUT_GOLD_REVIEW_CHUNKS)
    print("REVIEWS_PER_CHUNK =", REVIEWS_PER_CHUNK)
    print("MAX_REVIEW_TEXT_CHARS =", MAX_REVIEW_TEXT_CHARS)
    print("MAX_MERGED_TEXT_CHARS =", MAX_MERGED_TEXT_CHARS)

    df = spark.read.parquet(INPUT_GOLD_REVIEWS_TOP4)

    required_cols = [
        "review_id",
        "app_id",
        "user_id",
        "language",
        "rating",
        "hours",
        "helpful_score",
        "funny_score",
        "review_text",
        "review_text_clean",
        "review_text_length",
        "timestamp_created",
        "timestamp_updated",
        "silver_processed_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    # Chỉ giữ cột cần dùng.
    df = df.select(*required_cols)

    # =========================================================
    # BASIC FILTER + TYPE NORMALIZATION
    # =========================================================

    df = (
        df
        .withColumn("language", F.lower(F.trim(F.col("language"))))
        .withColumn("app_id", F.col("app_id").cast("long"))
        .withColumn("review_id", F.col("review_id").cast("string"))
        .withColumn("user_id", F.col("user_id").cast("string"))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("hours", F.coalesce(F.col("hours").cast("double"), F.lit(0.0)))
        .withColumn("helpful_score", F.coalesce(F.col("helpful_score").cast("double"), F.lit(0.0)))
        .withColumn("funny_score", F.coalesce(F.col("funny_score").cast("double"), F.lit(0.0)))
        .filter(F.col("language").isin(TOP_LANGUAGES))
        .filter(F.col("app_id").isNotNull())
        .filter(F.col("app_id") > 0)
    )

    # =========================================================
    # USE EXISTING CLEAN TEXT FROM SILVER/GOLD
    # =========================================================

    df = (
        df
        .withColumn(
            "review_text_for_chunk",
            F.coalesce(F.col("review_text_clean"), F.col("review_text")).cast("string")
        )
        .withColumn("review_text_for_chunk", F.trim(F.col("review_text_for_chunk")))
        .withColumn("review_text_len_for_chunk", F.length(F.col("review_text_for_chunk")))
        .filter(F.col("review_text_for_chunk").isNotNull())
        .filter(F.col("review_text_len_for_chunk") >= MIN_TEXT_LENGTH)
        .withColumn(
            "review_text_for_chunk",
            F.substring(F.col("review_text_for_chunk"), 1, MAX_REVIEW_TEXT_CHARS)
        )
        .withColumn("review_text_len_capped", F.length(F.col("review_text_for_chunk")))
    )

    # =========================================================
    # DYNAMIC TEXT BUCKET
    # =========================================================
    # Không dùng cố định 100 bucket cho mọi game nữa.
    # Game ít review chỉ 1 bucket để không sinh quá nhiều chunk.
    # Game cực nhiều review mới dùng 50-100 bucket để tránh straggler.

    app_lang_counts = (
        df
        .groupBy("app_id", "language")
        .agg(F.count("*").alias("app_language_review_count"))
    )

    app_lang_counts = app_lang_counts.withColumn(
        "bucket_count",
        F.when(F.col("app_language_review_count") < 1000, F.lit(1))
         .when(F.col("app_language_review_count") < 5000, F.lit(5))
         .when(F.col("app_language_review_count") < 20000, F.lit(20))
         .when(F.col("app_language_review_count") < 100000, F.lit(50))
         .otherwise(F.lit(100))
    )

    df = df.join(
        app_lang_counts,
        on=["app_id", "language"],
        how="inner"
    )

    df = df.withColumn(
        "review_hash_key",
        F.concat_ws(
            "_",
            F.coalesce(F.col("review_id"), F.lit("no_review_id")),
            F.coalesce(F.col("user_id"), F.lit("no_user_id")),
            F.col("app_id").cast("string"),
            F.coalesce(F.col("timestamp_created").cast("string"), F.lit("no_time"))
        )
    )

    df = df.withColumn(
        "text_bucket",
        F.pmod(
            F.xxhash64(F.col("review_hash_key")),
            F.col("bucket_count")
        ).cast("int")
    )

    # Cột tối thiểu trước window/shuffle.
    df = df.select(
        "review_id",
        "app_id",
        "user_id",
        "language",
        "text_bucket",
        "bucket_count",
        "app_language_review_count",
        "rating",
        "hours",
        "helpful_score",
        "funny_score",
        "review_text_for_chunk",
        "review_text_len_for_chunk",
        "review_text_len_capped",
        "timestamp_created",
        "timestamp_updated",
        "silver_processed_at",
    )

    df = df.repartition(SHUFFLE_PARTITIONS, "app_id", "language", "text_bucket")

    # =========================================================
    # RANK IN APP + LANGUAGE + DYNAMIC BUCKET
    # =========================================================

    w = (
        Window
        .partitionBy("app_id", "language", "text_bucket")
        .orderBy(
            F.desc("helpful_score"),
            F.desc("review_text_len_capped"),
            F.desc("timestamp_created"),
            F.asc("review_id"),
        )
    )

    chunk_source_df = (
        df
        .withColumn("bucket_review_rank", F.row_number().over(w))
        .withColumn(
            "local_chunk_id",
            F.floor((F.col("bucket_review_rank") - 1) / F.lit(REVIEWS_PER_CHUNK)).cast("long")
        )
        .withColumn(
            "chunk_id",
            (
                F.col("text_bucket").cast("long") * F.lit(1000000000)
                + F.col("local_chunk_id")
            ).cast("long")
        )
        .withColumn(
            "sentiment_label",
            F.when(F.col("rating") >= 0.5, F.lit("[POSITIVE]"))
             .when(F.col("rating") < 0.5, F.lit("[NEGATIVE]"))
             .otherwise(F.lit("[UNKNOWN]"))
        )
        .withColumn(
            "review_for_merge",
            F.concat_ws(
                " ",
                F.col("sentiment_label"),
                F.concat(F.lit("[HOURS="), F.round(F.col("hours"), 1).cast("string"), F.lit("]")),
                F.concat(F.lit("[HELPFUL="), F.round(F.col("helpful_score"), 0).cast("long").cast("string"), F.lit("]")),
                F.col("review_text_for_chunk")
            )
        )
        .select(
            "app_id",
            "language",
            "text_bucket",
            "bucket_count",
            "app_language_review_count",
            "chunk_id",
            "local_chunk_id",
            "bucket_review_rank",
            "review_id",
            "user_id",
            "rating",
            "hours",
            "helpful_score",
            "funny_score",
            "review_text_len_for_chunk",
            "review_text_len_capped",
            "timestamp_created",
            "timestamp_updated",
            "silver_processed_at",
            "review_for_merge",
        )
    )

    # =========================================================
    # GROUP INTO CHUNKS
    # =========================================================

    review_chunks = (
        chunk_source_df
        .groupBy("app_id", "language", "text_bucket", "chunk_id")
        .agg(
            F.max("bucket_count").alias("bucket_count"),
            F.max("app_language_review_count").alias("app_language_review_count"),
            F.max("local_chunk_id").alias("local_chunk_id"),

            F.count("*").alias("review_count"),
            F.approx_count_distinct("user_id", 0.05).alias("user_count"),

            F.sum(F.when(F.col("rating") >= 0.5, 1).otherwise(0)).alias("positive_count"),
            F.sum(F.when(F.col("rating") < 0.5, 1).otherwise(0)).alias("negative_count"),

            F.avg("rating").alias("avg_rating"),
            F.avg("hours").alias("avg_hours"),
            F.sum("helpful_score").alias("total_helpful_score"),
            F.sum("funny_score").alias("total_funny_score"),

            F.avg("review_text_len_for_chunk").alias("avg_review_text_length"),
            F.avg("review_text_len_capped").alias("avg_review_text_length_capped"),

            F.min("timestamp_created").alias("chunk_first_review_time"),
            F.max("timestamp_created").alias("chunk_last_review_time"),
            F.max("timestamp_updated").alias("chunk_last_updated_time"),
            F.max("silver_processed_at").alias("silver_processed_at"),

            F.collect_list(
                F.struct(
                    F.col("bucket_review_rank").alias("rank"),
                    F.col("review_id"),
                    F.col("review_for_merge")
                )
            ).alias("review_items"),
        )
        .withColumn("review_items", F.sort_array(F.col("review_items")))
        .withColumn(
            "merged_review_text",
            F.expr("concat_ws('\n', transform(review_items, x -> x.review_for_merge))")
        )
        .withColumn(
            "merged_review_text",
            F.substring(F.col("merged_review_text"), 1, MAX_MERGED_TEXT_CHARS)
        )
        .withColumn("merged_text_length", F.length(F.col("merged_review_text")))
        .withColumn("embedding_input_text", F.col("merged_review_text"))
        .drop("review_items")
        .filter(F.col("merged_text_length") >= 50)
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    # =========================================================
    # WRITE OUTPUT
    # =========================================================

    (
        review_chunks
        .repartition(SHUFFLE_PARTITIONS, "language", "text_bucket")
        .write
        .mode("overwrite")
        .partitionBy("language")
        .parquet(OUTPUT_GOLD_REVIEW_CHUNKS)
    )

    print("WRITE REVIEW CHUNKS DONE =", OUTPUT_GOLD_REVIEW_CHUNKS)
    print("=== END gold_app_language_review_chunks FAST DYNAMIC BUCKET ===")


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("gold-app-language-review-chunks-fast-dynamic-bucket")

        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

        # Bật codegen để nhanh hơn. Nếu bị Janino/codegen error thì mới đổi false.
        .config("spark.sql.codegen.wholeStage", "true")

        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.default.parallelism", str(SHUFFLE_PARTITIONS))

        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.files.maxPartitionBytes", "128m")

        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.legacy.charVarcharAsString", "true")
        .getOrCreate()
    )

    try:
        main(spark)
    finally:
        spark.stop()