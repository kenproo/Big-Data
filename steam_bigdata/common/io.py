from pyspark.sql import SparkSession, DataFrame

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", "\"")
        .csv(path)
    )

def read_json(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("multiLine", True)
        .json(path)
    )

def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    (
        df.write
        .mode(mode)
        .parquet(path)
    )