"""Metric execution engine orchestration helpers.

This module routes metric requests between Spark-native and Python execution
paths and preserves the public engine entrypoint used by evaluation accessors.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from teehr.metrics.models.base import MetricsBasemodel
from teehr.metrics.format import apply_aggregation_metrics
from teehr.metrics.spark_native import (
    _ordered_output_columns,
    build_metric_plan,
    compute_spark_native_metrics,
    supports_spark_native,
)
from teehr.querying.utils import group_df, parse_fields_to_list
from teehr.utils.spark import null_safe_join_on_columns


def aggregate_metrics_with_engine(
    sdf: DataFrame,
    group_by,
    metrics: List[MetricsBasemodel],
    engine: str = "auto",
) -> DataFrame:
    """Aggregate metrics using auto, python, or spark execution modes."""
    if not isinstance(metrics, list):
        metrics = [metrics]

    engine = engine.lower()
    if engine not in {"auto", "python", "spark"}:
        raise ValueError("engine must be one of: 'auto', 'python', 'spark'.")

    group_by_cols = parse_fields_to_list(group_by)

    if engine == "python":
        gp = group_df(sdf, group_by_cols)
        return apply_aggregation_metrics(gp=gp, include_metrics=metrics)

    if engine == "spark":
        unsupported = [m for m in metrics if not supports_spark_native(m)]
        if unsupported:
            names = ", ".join(m.__class__.__name__ for m in unsupported)
            raise ValueError(
                "Spark engine cannot run unsupported metrics in this query: "
                f"{names}. Use engine='auto' or engine='python'."
            )
        return compute_spark_native_metrics(sdf, group_by_cols, metrics)

    spark_metrics, python_metrics = build_metric_plan(metrics)

    if spark_metrics and not python_metrics:
        return compute_spark_native_metrics(sdf, group_by_cols, spark_metrics)

    if python_metrics and not spark_metrics:
        gp = group_df(sdf, group_by_cols)
        return apply_aggregation_metrics(gp=gp, include_metrics=python_metrics)

    spark_df = compute_spark_native_metrics(sdf, group_by_cols, spark_metrics)
    python_gp = group_df(sdf, group_by_cols)
    python_df = apply_aggregation_metrics(gp=python_gp, include_metrics=python_metrics)

    combined_df = null_safe_join_on_columns(
        spark_df,
        python_df,
        join_columns=group_by_cols,
        how="outer",
        left_alias="spark",
        right_alias="python",
    )
    ordered_cols = _ordered_output_columns(combined_df, group_by_cols, metrics)
    return combined_df.select(*ordered_cols)


__all__ = [
    "aggregate_metrics_with_engine",
    "build_metric_plan",
    "compute_spark_native_metrics",
    "supports_spark_native",
]
