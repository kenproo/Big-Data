from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

def normalize_columns(df: DataFrame) -> DataFrame:
    for c in df.columns:
        new_name = (
            c.strip()
             .lower()
             .replace(" ", "_")
             .replace("/", "_")
             .replace("-", "_")
             .replace(".", "_")
             .replace("(", "")
             .replace(")", "")
        )
        df = df.withColumnRenamed(c, new_name)
    return df

def add_ingestion_metadata(df: DataFrame, source_name: str) -> DataFrame:
    return (
        df.withColumn("ingestion_ts", F.current_timestamp())
          .withColumn("source_name", F.lit(source_name))
    )

def profile_dataframe(df: DataFrame, table_name: str) -> DataFrame:
    total_rows = df.count()

    agg_exprs = []
    for c in df.columns:
        agg_exprs.append(
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}__null_count")
        )

    agg_row = df.agg(*agg_exprs).collect()[0]

    rows = []
    for field in df.schema.fields:
        col_name = field.name
        null_count = agg_row[f"{col_name}__null_count"]

        rows.append((
            table_name,
            col_name,
            total_rows,
            null_count,
            float(null_count / total_rows) if total_rows > 0 else 0.0,
            str(field.dataType)
        ))

    spark = df.sparkSession
    return spark.createDataFrame(
        rows,
        schema="""
            table_name string,
            column_name string,
            row_count long,
            null_count long,
            null_ratio double,
            data_type string
        """
    )

def profile_numeric_columns(df: DataFrame, table_name: str) -> DataFrame:
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]

    if not numeric_cols:
        spark = df.sparkSession
        return spark.createDataFrame(
            [],
            schema="""
                table_name string,
                column_name string,
                min_value double,
                max_value double,
                avg_value double,
                stddev_value double
            """
        )

    agg_exprs = []
    for c in numeric_cols:
        agg_exprs.extend([
            F.min(c).alias(f"{c}__min"),
            F.max(c).alias(f"{c}__max"),
            F.avg(c).alias(f"{c}__avg"),
            F.stddev(c).alias(f"{c}__stddev")
        ])

    agg_row = df.agg(*agg_exprs).collect()[0]

    rows = []
    for c in numeric_cols:
        rows.append((
            table_name,
            c,
            float(agg_row[f"{c}__min"]) if agg_row[f"{c}__min"] is not None else None,
            float(agg_row[f"{c}__max"]) if agg_row[f"{c}__max"] is not None else None,
            float(agg_row[f"{c}__avg"]) if agg_row[f"{c}__avg"] is not None else None,
            float(agg_row[f"{c}__stddev"]) if agg_row[f"{c}__stddev"] is not None else None
        ))

    spark = df.sparkSession
    return spark.createDataFrame(
        rows,
        schema="""
            table_name string,
            column_name string,
            min_value double,
            max_value double,
            avg_value double,
            stddev_value double
        """
    )