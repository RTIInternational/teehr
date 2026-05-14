"""Execution planner for calculated fields.

This module orchestrates Spark-native and python/pandas execution adapters for
calculated fields and supports ``auto``, ``spark``, and ``python`` modes.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame

from teehr.calculated_fields.adapters import (
    CalculatedFieldAdapter,
    batched_adapter,
    single_field_adapter,
)
from teehr.calculated_fields.timeseries_aware_pandas import (
    apply_percentile_event_detection_pandas,
    apply_threshold_event_detection_pandas,
)
from teehr.calculated_fields.timeseries_aware_spark import (
    apply_baseflow_period_spark,
    apply_exceedance_probability_spark,
    apply_lead_time_bins_spark,
    apply_percentile_batch_spark,
    apply_threshold_batch_spark,
)
from teehr.calculated_fields.row_level_pandas import (
    apply_day_of_year_pandas,
    apply_forecast_lead_time_pandas,
    apply_hour_of_year_pandas,
    apply_month_pandas,
    apply_normalized_flow_pandas,
    apply_seasons_pandas,
    apply_threshold_value_exceeded_pandas,
    apply_threshold_value_not_exceeded_pandas,
    apply_water_year_pandas,
    apply_year_pandas,
)
from teehr.calculated_fields.row_level_spark import (
    apply_day_of_year,
    apply_forecast_lead_time,
    apply_hour_of_year,
    apply_month,
    apply_normalized_flow,
    apply_seasons,
    apply_threshold_value_exceeded,
    apply_threshold_value_not_exceeded,
    apply_water_year,
    apply_year,
)
from teehr.calculated_fields.models.base import CalculatedFieldBaseModel
from teehr.calculated_fields.models.row_level import (
    Month,
    Year,
    WaterYear,
    NormalizedFlow,
    Seasons,
    ForecastLeadTime,
    ForecastLeadTimeBins,
    ThresholdValueExceeded,
    ThresholdValueNotExceeded,
    DayOfYear,
    HourOfYear,
)
from teehr.calculated_fields.models.timeseries_aware import (
    UNIQUENESS_FIELDS,
    AbovePercentileEventDetection,
    BelowPercentileEventDetection,
    AboveThresholdEventDetection,
    BelowThresholdEventDetection,
    ExceedanceProbability,
    BaseflowPeriodDetection,
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

EXCEEDANCE_CF_TYPES = (ExceedanceProbability,)
BASEFLOW_PERIOD_CF_TYPES = (BaseflowPeriodDetection,)
LEAD_TIME_BINS_CF_TYPES = (ForecastLeadTimeBins,)


def _supports_percentile(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, PERCENTILE_CF_TYPES)


def _supports_threshold(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, THRESHOLD_CF_TYPES)


def _supports_row_level(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, ROW_LEVEL_NATIVE_CF_TYPES)


def _supports_exceedance(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, EXCEEDANCE_CF_TYPES)


def _supports_baseflow_period(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, BASEFLOW_PERIOD_CF_TYPES)


def _supports_lead_time_bins(cf: CalculatedFieldBaseModel) -> bool:
    return isinstance(cf, LEAD_TIME_BINS_CF_TYPES)


def supports_spark_native_cf(cf: CalculatedFieldBaseModel) -> bool:
    """Return whether a calculated field currently supports Spark-native execution."""
    return isinstance(
        cf,
        PERCENTILE_CF_TYPES
        + THRESHOLD_CF_TYPES
        + ROW_LEVEL_NATIVE_CF_TYPES
        + EXCEEDANCE_CF_TYPES
        + BASEFLOW_PERIOD_CF_TYPES
        + LEAD_TIME_BINS_CF_TYPES,
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


def _apply_percentile_batch(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
) -> DataFrame:
    return apply_percentile_batch_spark(
        sdf=sdf,
        cfs=cfs,
        default_uniqueness_fields=UNIQUENESS_FIELDS,
    )


def _apply_threshold_batch(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
) -> DataFrame:
    return apply_threshold_batch_spark(
        sdf=sdf,
        cfs=cfs,
        default_uniqueness_fields=UNIQUENESS_FIELDS,
    )


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


def _apply_row_level_python(sdf: DataFrame, cf: CalculatedFieldBaseModel) -> DataFrame:
    if isinstance(cf, Month):
        return apply_month_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, Year):
        return apply_year_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, WaterYear):
        return apply_water_year_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, NormalizedFlow):
        return apply_normalized_flow_pandas(
            sdf,
            primary_value_field_name=cf.primary_value_field_name,
            drainage_area_field_name=cf.drainage_area_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, Seasons):
        return apply_seasons_pandas(
            sdf,
            value_time_field_name=cf.value_time_field_name,
            season_months=cf.season_months,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, ForecastLeadTime):
        return apply_forecast_lead_time_pandas(
            sdf,
            value_time_field_name=cf.value_time_field_name,
            reference_time_field_name=cf.reference_time_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, ThresholdValueExceeded):
        return apply_threshold_value_exceeded_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            threshold_field_name=cf.threshold_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, ThresholdValueNotExceeded):
        return apply_threshold_value_not_exceeded_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            threshold_field_name=cf.threshold_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, DayOfYear):
        return apply_day_of_year_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, HourOfYear):
        return apply_hour_of_year_pandas(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )

    # Preserve prior behavior for row-level models without a dedicated pandas path.
    return _apply_with_model(sdf, cf)


def _apply_row_level_spark(sdf: DataFrame, cf: CalculatedFieldBaseModel) -> DataFrame:
    if isinstance(cf, Month):
        return apply_month(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, Year):
        return apply_year(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, WaterYear):
        return apply_water_year(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, NormalizedFlow):
        return apply_normalized_flow(
            sdf,
            primary_value_field_name=cf.primary_value_field_name,
            drainage_area_field_name=cf.drainage_area_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, Seasons):
        return apply_seasons(
            sdf,
            value_time_field_name=cf.value_time_field_name,
            season_months=cf.season_months,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, ForecastLeadTime):
        return apply_forecast_lead_time(
            sdf,
            value_time_field_name=cf.value_time_field_name,
            reference_time_field_name=cf.reference_time_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, ThresholdValueExceeded):
        return apply_threshold_value_exceeded(
            sdf,
            input_field_name=cf.input_field_name,
            threshold_field_name=cf.threshold_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, ThresholdValueNotExceeded):
        return apply_threshold_value_not_exceeded(
            sdf,
            input_field_name=cf.input_field_name,
            threshold_field_name=cf.threshold_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, DayOfYear):
        return apply_day_of_year(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )
    if isinstance(cf, HourOfYear):
        return apply_hour_of_year(
            sdf,
            input_field_name=cf.input_field_name,
            output_field_name=cf.output_field_name,
        )

    return _apply_with_model(sdf, cf)


def _apply_exceedance_probability_spark(
    sdf: DataFrame, cf: CalculatedFieldBaseModel
) -> DataFrame:
    return apply_exceedance_probability_spark(
        sdf=sdf,
        cf=cf,
        default_uniqueness_fields=UNIQUENESS_FIELDS,
    )


def _apply_baseflow_period_spark(
    sdf: DataFrame, cf: CalculatedFieldBaseModel
) -> DataFrame:
    return apply_baseflow_period_spark(
        sdf=sdf,
        cf=cf,
        default_uniqueness_fields=UNIQUENESS_FIELDS,
    )


def _apply_lead_time_bins_spark(
    sdf: DataFrame, cf: CalculatedFieldBaseModel
) -> DataFrame:
    return apply_lead_time_bins_spark(sdf=sdf, cf=cf)


def _apply_percentile_batch_python(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
) -> DataFrame:
    """Apply one python/pandas percentile detection batch for compatible fields."""
    for cf in cfs:
        sdf = apply_percentile_event_detection_pandas(
            sdf=sdf,
            value_field_name=cf.value_field_name,
            value_time_field_name=cf.value_time_field_name,
            quantile=cf.quantile,
            output_event_field_name=cf.output_event_field_name,
            output_event_id_field_name=cf.output_event_id_field_name,
            output_quantile_field_name=cf.output_quantile_field_name,
            add_quantile_field=cf.add_quantile_field,
            skip_event_id=cf.skip_event_id,
            uniqueness_fields=cf.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
            is_above=isinstance(cf, AbovePercentileEventDetection),
        )
    return sdf


def _apply_threshold_batch_python(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
) -> DataFrame:
    """Apply one python/pandas threshold detection batch for compatible fields."""
    for cf in cfs:
        sdf = apply_threshold_event_detection_pandas(
            sdf=sdf,
            value_field_name=cf.value_field_name,
            value_time_field_name=cf.value_time_field_name,
            threshold_field_name=cf.threshold_field_name,
            output_event_field_name=cf.output_event_field_name,
            output_event_id_field_name=cf.output_event_id_field_name,
            skip_event_id=cf.skip_event_id,
            uniqueness_fields=cf.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
            is_above=isinstance(cf, AboveThresholdEventDetection),
        )
    return sdf


def _supports_any(_: CalculatedFieldBaseModel) -> bool:
    return True


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
        apply_field=_apply_row_level_spark,
    ),
    single_field_adapter(
        name="spark-exceedance-probability",
        supports=_supports_exceedance,
        apply_field=_apply_exceedance_probability_spark,
    ),
    single_field_adapter(
        name="spark-baseflow-period",
        supports=_supports_baseflow_period,
        apply_field=_apply_baseflow_period_spark,
    ),
    single_field_adapter(
        name="spark-lead-time-bins",
        supports=_supports_lead_time_bins,
        apply_field=_apply_lead_time_bins_spark,
    ),
)


PYTHON_EXECUTION_ADAPTERS: tuple[CalculatedFieldAdapter, ...] = (
    single_field_adapter(
        name="python-row-level",
        supports=_supports_row_level,
        apply_field=_apply_row_level_python,
    ),
    batched_adapter(
        name="python-percentile-batch",
        supports=_supports_percentile,
        consume_batch=_consume_percentile_batch,
        apply_batch=_apply_percentile_batch_python,
    ),
    batched_adapter(
        name="python-threshold-batch",
        supports=_supports_threshold,
        consume_batch=_consume_threshold_batch,
        apply_batch=_apply_threshold_batch_python,
    ),
    single_field_adapter(
        name="python-model-fallback",
        supports=_supports_any,
        apply_field=_apply_with_model,
    ),
)


def _apply_with_adapters(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
    adapters: tuple[CalculatedFieldAdapter, ...],
    *,
    strict_engine_name: str | None = None,
) -> DataFrame:
    """Run a calculated-field sequence through an ordered adapter registry."""
    idx = 0
    while idx < len(cfs):
        cf = cfs[idx]

        adapter = next((a for a in adapters if a.supports(cf)), None)
        if adapter is not None:
            batch, next_idx = adapter.consume_batch(cfs, idx)
            sdf = adapter.apply_batch(sdf, batch)
            idx = next_idx
            continue

        if strict_engine_name is not None:
            raise ValueError(
                f"{strict_engine_name} engine cannot run unsupported calculated fields in this query: "
                f"{cf.__class__.__name__}. Use engine='auto' or engine='python'."
            )

        sdf = cf.apply_to(sdf)
        idx += 1

    return sdf


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
        return _apply_with_adapters(sdf, cfs, PYTHON_EXECUTION_ADAPTERS)

    if engine == "spark":
        unsupported = [cf.__class__.__name__ for cf in cfs if not supports_spark_native_cf(cf)]
        if unsupported:
            names = ", ".join(unsupported)
            raise ValueError(
                "Spark engine cannot run unsupported calculated fields in this query: "
                f"{names}. Use engine='auto' or engine='python'."
            )

    if engine == "spark":
        return _apply_with_adapters(
            sdf,
            cfs,
            SPARK_EXECUTION_ADAPTERS,
            strict_engine_name="Spark",
        )

    # auto: prefer Spark-native adapters, then fall back to python adapters.
    idx = 0
    while idx < len(cfs):
        cf = cfs[idx]

        spark_adapter = next((a for a in SPARK_EXECUTION_ADAPTERS if a.supports(cf)), None)
        if spark_adapter is not None:
            batch, next_idx = spark_adapter.consume_batch(cfs, idx)
            sdf = spark_adapter.apply_batch(sdf, batch)
            idx = next_idx
            continue

        python_adapter = next((a for a in PYTHON_EXECUTION_ADAPTERS if a.supports(cf)), None)
        if python_adapter is not None:
            batch, next_idx = python_adapter.consume_batch(cfs, idx)
            sdf = python_adapter.apply_batch(sdf, batch)
            idx = next_idx
            continue

        sdf = cf.apply_to(sdf)
        idx += 1

    return sdf
