from pyspark.sql import SparkSession, DataFrame

def read_csv(
    spark: SparkSession,
    path: str,
    header: bool = True,
    infer_schema: bool = True,
    sep: str = ","
) -> DataFrame:
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .option("sep", sep)
        .csv(path)
    )

def read_json(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("multiLine", True)
        .json(path)
    )

def read_parquet(spark, path: str) -> DataFrame:
    return spark.read.parquet(path)


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
    num_partitions: int | None = None,
) -> None:
    writer_df = df

    if num_partitions:
        writer_df = writer_df.repartition(num_partitions)

    writer = writer_df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)