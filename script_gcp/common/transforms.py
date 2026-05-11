import re
from pyspark.sql import functions as F


NULL_LIKE_VALUES = ["", "null", "NULL", "None", "N/A", "na", "NA"]


def clean_string_col(col_name):
    return (
        F.when(F.trim(F.col(col_name)).isin(*NULL_LIKE_VALUES), F.lit(None))
        .otherwise(F.trim(F.col(col_name)))
    )


def safe_to_long(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^0-9\-]", "").cast("long")


def safe_to_int(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^0-9\-]", "").cast("int")


def safe_to_double(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^0-9\.\-]", "").cast("double")


def unix_to_timestamp_safe(col_name):
    return F.when(
        F.col(col_name).cast("long").isNotNull(),
        F.to_timestamp(F.from_unixtime(F.col(col_name).cast("long")))
    ).otherwise(F.lit(None))


def clean_html_text(col_name):
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(F.col(col_name), r"<[^>]+>", " "),
            r"\s+",
            " "
        )
    )