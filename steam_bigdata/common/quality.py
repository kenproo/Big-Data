from pyspark.sql import functions as F


def add_not_null_rule(df, col_name, issue_name):
    return df.withColumn(
        "quality_issue",
        F.when(F.col("quality_issue").isNull() & F.col(col_name).isNull(), F.lit(issue_name))
         .otherwise(F.col("quality_issue"))
    )


def add_non_negative_rule(df, col_name, issue_name):
    return df.withColumn(
        "quality_issue",
        F.when(
            F.col("quality_issue").isNull() & F.col(col_name).isNotNull() & (F.col(col_name) < 0),
            F.lit(issue_name)
        ).otherwise(F.col("quality_issue"))
    )


def add_range_rule(df, col_name, min_value, max_value, issue_name):
    return df.withColumn(
        "quality_issue",
        F.when(
            F.col("quality_issue").isNull()
            & F.col(col_name).isNotNull()
            & ((F.col(col_name) < min_value) | (F.col(col_name) > max_value)),
            F.lit(issue_name)
        ).otherwise(F.col("quality_issue"))
    )


def add_timestamp_order_rule(df, start_col, end_col, issue_name):
    return df.withColumn(
        "quality_issue",
        F.when(
            F.col("quality_issue").isNull()
            & F.col(start_col).isNotNull()
            & F.col(end_col).isNotNull()
            & (F.col(end_col) < F.col(start_col)),
            F.lit(issue_name)
        ).otherwise(F.col("quality_issue"))
    )


def finalize_quality(df):
    return df.withColumn("is_valid_record", F.col("quality_issue").isNull())


def split_valid_invalid(df):
    valid_df = df.filter(F.col("is_valid_record") == True)
    invalid_df = df.filter(F.col("is_valid_record") == False)
    return valid_df, invalid_df