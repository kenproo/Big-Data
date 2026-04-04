from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def append_reason_col(
    df: DataFrame,
    target_col: str,
    condition,
    reason: str,
) -> DataFrame:
    return df.withColumn(
        target_col,
        F.when(
            condition,
            F.when(F.col(target_col).isNull(), F.lit(reason))
             .otherwise(F.concat_ws("|", F.col(target_col), F.lit(reason)))
        ).otherwise(F.col(target_col))
    )


def ensure_reason_cols(
    df: DataFrame,
    quality_col: str = "quality_issue",
    outlier_col: str = "outlier_reasons",
) -> DataFrame:
    if quality_col not in df.columns:
        df = df.withColumn(quality_col, F.lit(None).cast("string"))
    if outlier_col not in df.columns:
        df = df.withColumn(outlier_col, F.lit(None).cast("string"))
    return df


def add_issue(
    df: DataFrame,
    condition,
    issue_name: str,
    quality_col: str = "quality_issue",
) -> DataFrame:
    return append_reason_col(df, quality_col, condition, issue_name)


def add_outlier_reason(
    df: DataFrame,
    condition,
    reason: str,
    outlier_col: str = "outlier_reasons",
) -> DataFrame:
    return append_reason_col(df, outlier_col, condition, reason)


def get_approx_quantiles(
    df: DataFrame,
    cols: Iterable[str],
    quantile: float = 0.999,
    rel_error: float = 0.01,
) -> Dict[str, float]:
    result = {}
    for c in cols:
        if c not in df.columns:
            continue
        try:
            values = df.approxQuantile(c, [quantile], rel_error)
            if values and values[0] is not None:
                result[c] = values[0]
        except Exception:
            pass
    return result


def flag_upper_quantile_outliers(
    df: DataFrame,
    cols: Iterable[str],
    quantile: float = 0.999,
    rel_error: float = 0.01,
    outlier_prefix: str = "p999_outlier_",
) -> DataFrame:
    thresholds = get_approx_quantiles(df, cols, quantile=quantile, rel_error=rel_error)

    for c, threshold in thresholds.items():
        df = add_outlier_reason(
            df,
            F.col(c).isNotNull() & (F.col(c) > F.lit(threshold)),
            f"{outlier_prefix}{c}",
        )

    return df


def cap_upper_quantile(
    df: DataFrame,
    cols: Iterable[str],
    quantile: float = 0.999,
    rel_error: float = 0.01,
    suffix: str = "_capped",
    keep_original: bool = True,
) -> DataFrame:
    thresholds = get_approx_quantiles(df, cols, quantile=quantile, rel_error=rel_error)

    for c, threshold in thresholds.items():
        capped_expr = F.when(F.col(c).isNull(), F.lit(None)).otherwise(
            F.least(F.col(c), F.lit(threshold))
        )

        target_col = f"{c}{suffix}" if keep_original else c
        df = df.withColumn(target_col, capped_expr)

    return df


def add_is_outlier_flag(
    df: DataFrame,
    outlier_col: str = "outlier_reasons",
    flag_col: str = "is_outlier",
) -> DataFrame:
    return df.withColumn(flag_col, F.col(outlier_col).isNotNull())


def add_hard_range_issue(
    df: DataFrame,
    col_name: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    issue_name: Optional[str] = None,
    quality_col: str = "quality_issue",
) -> DataFrame:
    if col_name not in df.columns:
        return df

    condition = F.lit(False)
    if min_value is not None:
        condition = condition | (F.col(col_name).isNotNull() & (F.col(col_name) < F.lit(min_value)))
    if max_value is not None:
        condition = condition | (F.col(col_name).isNotNull() & (F.col(col_name) > F.lit(max_value)))

    return add_issue(
        df,
        condition,
        issue_name or f"invalid_range_{col_name}",
        quality_col=quality_col,
    )


def add_hard_upper_limit_issue(
    df: DataFrame,
    col_name: str,
    hard_max: float,
    quality_col: str = "quality_issue",
) -> DataFrame:
    if col_name not in df.columns:
        return df

    return add_issue(
        df,
        F.col(col_name).isNotNull() & (F.col(col_name) > F.lit(hard_max)),
        f"hard_limit_exceeded_{col_name}",
        quality_col=quality_col,
    )