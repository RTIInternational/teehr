"""Spark-native execution planner for calculated fields.

This module enables incremental migration away from Python UDF paths by
supporting Spark-native execution for a subset of calculated fields while
falling back to existing ``apply_to`` implementations for unsupported fields.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

from teehr.querying.calculated_fields_adapters import (
    CalculatedFieldAdapter,
    batched_adapter,
    single_field_adapter,
)
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
from teehr.models.calculated_fields.row_level import (
    Month,
    Year,
    WaterYear,
    NormalizedFlow,
    Seasons,
    ForecastLeadTime,
    ThresholdValueExceeded,
    ThresholdValueNotExceeded,
    DayOfYear,
    HourOfYear,
)
from teehr.models.calculated_fields.timeseries_aware import (
    UNIQUENESS_FIELDS,
    AbovePercentileEventDetection,
    BelowPercentileEventDetection,
    AboveThresholdEventDetection,
    BelowThresholdEventDetection,
)


PERCENTILE_CF_TYPES = (
    AbovePercentileEventDetection,
    BelowPercentileEventDetection,
)

THRESHOLD_CF_TYPES = (
    AboveThresholdEventDetection,
    BelowThresholdEventDetection,
)

ROW_LEVEL_NATIVE_CF_TYPES = (
    Month,
    Year,
    WaterYear,
    NormalizedFlow,
    Seasons,
    ForecastLeadTime,
    ThresholdValueExceeded,
    ThresholdValueNotExceeded,
    DayOfYear,
    HourOfYear,
)


def _supports_percentile(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, PERCENTILE_CF_TYPES)


def _supports_threshold(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, THRESHOLD_CF_TYPES)


def _supports_row_level(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, ROW_LEVEL_NATIVE_CF_TYPES)


def supports_spark_native_cf(cf: CalculatedFieldBaseModel) -> bool:
    """Return whether a calculated field currently supports Spark-native execution."""
    return isinstance(
        cf,
        PERCENTILE_CF_TYPES + THRESHOLD_CF_TYPES + ROW_LEVEL_NATIVE_CF_TYPES,
    )


def _normalize_fields(fields: str | list[str] | None) -> list[str]:
    if fields is None:
        return list(UNIQUENESS_FIELDS)
    if isinstance(fields, str):
        return [fields]
    return list(fields)


def _percentile_group_key(cf: CalculatedFieldBaseModel) -> tuple:
    return (
        tuple(_normalize_fields(cf.uniqueness_fields)),
        cf.value_time_field_name,
        cf.value_field_name,
    )


def _threshold_group_key(cf: CalculatedFieldBaseModel) -> tuple:
    return (
        tuple(_normalize_fields(cf.uniqueness_fields)),
        cf.value_time_field_name,
        cf.value_field_name,
        cf.threshold_field_name,
    )


def _add_event_ids(
    sdf: DataFrame,
    group_cols: list[str],
    time_col: str,
    event_col: str,
    output_event_id_col: str,
) -> DataFrame:
    """Add contiguous event segment IDs in ``start-end`` timestamp string format."""
    seg_col = f"__seg_{output_event_id_col}"
    start_col = f"__start_{output_event_id_col}"
    end_col = f"__end_{output_event_id_col}"

    order_window = Window.partitionBy(*group_cols).orderBy(F.col(time_col))

    prev_event = F.lag(F.col(event_col)).over(order_window)
    segment_change = F.when(
        prev_event.isNull() | (F.col(event_col) != prev_event),
        F.lit(1),
    ).otherwise(F.lit(0))

    sdf = sdf.withColumn(seg_col, F.sum(segment_change).over(order_window))

    bounds = sdf.groupBy(*group_cols, seg_col).agg(
        F.min(F.col(time_col)).alias(start_col),
        F.max(F.col(time_col)).alias(end_col),
    )

    sdf = sdf.join(bounds, on=[*group_cols, seg_col], how="left")
    sdf = sdf.withColumn(
        output_event_id_col,
        F.concat(
            F.col(start_col).cast("string"),
            F.lit("-"),
            F.col(end_col).cast("string"),
        ),
    )
    return sdf.drop(seg_col, start_col, end_col)


def _apply_percentile_batch(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
) -> DataFrame:
    """Apply one Spark-native percentile detection batch for compatible fields."""
    first = cfs[0]
    group_cols = _normalize_fields(first.uniqueness_fields)
    value_col = first.value_field_name
    time_col = first.value_time_field_name

    quantiles = sorted({float(cf.quantile) for cf in cfs})
    q_index = {q: idx for idx, q in enumerate(quantiles)}

    thresholds = sdf.groupBy(*group_cols).agg(
        F.percentile_approx(
            F.col(value_col).cast("double"),
            F.array(*[F.lit(q) for q in quantiles]),
            F.lit(10000),
        ).alias("__percentiles")
    )

    sdf = sdf.join(thresholds, on=group_cols, how="left")

    for cf in cfs:
        idx = q_index[float(cf.quantile)]
        threshold_expr = F.col("__percentiles").getItem(idx)

        if isinstance(cf, AbovePercentileEventDetection):
            comp = F.col(value_col).cast("double") > threshold_expr
        else:
            comp = F.col(value_col).cast("double") < threshold_expr

        sdf = sdf.withColumn(
            cf.output_event_field_name,
            F.coalesce(comp, F.lit(False)).cast("boolean"),
        )

        if getattr(cf, "add_quantile_field", False):
            sdf = sdf.withColumn(
                cf.output_quantile_field_name,
                threshold_expr.cast("double"),
            )

        if not getattr(cf, "skip_event_id", False):
            sdf = _add_event_ids(
                sdf=sdf,
                group_cols=group_cols,
                time_col=time_col,
                event_col=cf.output_event_field_name,
                output_event_id_col=cf.output_event_id_field_name,
            )

    return sdf.drop("__percentiles")


def _apply_threshold_batch(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
) -> DataFrame:
    """Apply one Spark-native threshold detection batch for compatible fields."""
    first = cfs[0]
    group_cols = _normalize_fields(first.uniqueness_fields)
    value_col = first.value_field_name
    time_col = first.value_time_field_name
    threshold_col = first.threshold_field_name

    threshold_cast_col = "__threshold_cast"
    sdf = sdf.withColumn(threshold_cast_col, F.col(threshold_col).cast("double"))

    for cf in cfs:
        if isinstance(cf, AboveThresholdEventDetection):
            comp = F.col(value_col).cast("double") > F.col(threshold_cast_col)
        else:
            comp = F.col(value_col).cast("double") < F.col(threshold_cast_col)

        sdf = sdf.withColumn(
            cf.output_event_field_name,
            F.coalesce(comp, F.lit(False)).cast("boolean"),
        )

        if not getattr(cf, "skip_event_id", False):
            sdf = _add_event_ids(
                sdf=sdf,
                group_cols=group_cols,
                time_col=time_col,
                event_col=cf.output_event_field_name,
                output_event_id_col=cf.output_event_id_field_name,
            )

    return sdf.drop(threshold_cast_col)


def _consume_compatible_batch(
    cfs: list[CalculatedFieldBaseModel],
    start: int,
    compatible_types: tuple,
    key_func,
) -> tuple[list[CalculatedFieldBaseModel], int]:
    """Consume a contiguous compatible CF batch from ``start`` index."""
    first = cfs[start]
    batch_key = key_func(first)
    batch = [first]

    idx = start + 1
    while idx < len(cfs):
        nxt = cfs[idx]
        if not isinstance(nxt, compatible_types) or key_func(nxt) != batch_key:
            break
        batch.append(nxt)
        idx += 1

    return batch, idx


def _consume_percentile_batch(
    cfs: list[CalculatedFieldBaseModel],
    start: int,
) -> tuple[list[CalculatedFieldBaseModel], int]:
    return _consume_compatible_batch(
        cfs,
        start,
        PERCENTILE_CF_TYPES,
        _percentile_group_key,
    )


def _consume_threshold_batch(
    cfs: list[CalculatedFieldBaseModel],
    start: int,
) -> tuple[list[CalculatedFieldBaseModel], int]:
    return _consume_compatible_batch(
        cfs,
        start,
        THRESHOLD_CF_TYPES,
        _threshold_group_key,
    )


def _apply_with_model(sdf: DataFrame, cf: CalculatedFieldBaseModel) -> DataFrame:
    return cf.apply_to(sdf)


SPARK_EXECUTION_ADAPTERS: tuple[CalculatedFieldAdapter, ...] = (
    batched_adapter(
        name="spark-percentile-batch",
        supports=_supports_percentile,
        consume_batch=_consume_percentile_batch,
        apply_batch=_apply_percentile_batch,
    ),
    batched_adapter(
        name="spark-threshold-batch",
        supports=_supports_threshold,
        consume_batch=_consume_threshold_batch,
        apply_batch=_apply_threshold_batch,
    ),
    single_field_adapter(
        name="spark-row-level",
        supports=_supports_row_level,
        apply_field=_apply_with_model,
    ),
)


def apply_calculated_fields_with_engine(
    sdf: DataFrame,
    cfs: Iterable[CalculatedFieldBaseModel],
    engine: str = "auto",
) -> DataFrame:
    """Apply calculated fields using auto, python, or spark execution modes."""
    cfs = list(cfs)
    engine = engine.lower()
    if engine not in {"auto", "python", "spark"}:
        raise ValueError("engine must be one of: 'auto', 'python', 'spark'.")

    if engine == "python":
        for cf in cfs:
            sdf = cf.apply_to(sdf)
        return sdf

    if engine == "spark":
        unsupported = [cf.__class__.__name__ for cf in cfs if not supports_spark_native_cf(cf)]
        if unsupported:
            names = ", ".join(unsupported)
            raise ValueError(
                "Spark engine cannot run unsupported calculated fields in this query: "
                f"{names}. Use engine='auto' or engine='python'."
            )

    idx = 0
    while idx < len(cfs):
        cf = cfs[idx]

        adapter = next((a for a in SPARK_EXECUTION_ADAPTERS if a.supports(cf)), None)
        if adapter is not None:
            batch, next_idx = adapter.consume_batch(cfs, idx)
            sdf = adapter.apply_batch(sdf, batch)
            idx = next_idx
            continue

        if engine == "spark":
            raise ValueError(
                "Spark engine cannot run unsupported calculated fields in this query: "
                f"{cf.__class__.__name__}. Use engine='auto' or engine='python'."
            )

        sdf = cf.apply_to(sdf)
        idx += 1

    return sdf
