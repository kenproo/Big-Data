def apply_spark_tuning(spark):
    conf = spark.conf

    # Adaptive Query Execution
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    # Shuffle / partition
    conf.set("spark.sql.shuffle.partitions", "400")
    conf.set("spark.default.parallelism", "400")

    # Broadcast
    conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50MB

    # Parquet
    conf.set("spark.sql.parquet.mergeSchema", "false")
    conf.set("spark.sql.parquet.filterPushdown", "true")

    # File sizing
    conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
    conf.set("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))

    # Dynamic partition overwrite
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

