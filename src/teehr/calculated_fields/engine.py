"""Execution planner for calculated fields.

This module orchestrates Spark-native and python/pandas execution adapters for
calculated fields and supports ``auto``, ``spark``, and ``python`` modes.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

from teehr.calculated_fields.adapters import (
    CalculatedFieldAdapter,
    batched_adapter,
    single_field_adapter,
)
from teehr.calculated_fields.timeseries_aware_pandas import (
    apply_percentile_event_detection_pandas,
    apply_threshold_event_detection_pandas,
)
from teehr.calculated_fields.row_level_pandas_spark import (
    apply_day_of_year_pandas,
    apply_hour_of_year_pandas,
    apply_month_pandas,
    apply_normalized_flow_pandas,
    apply_seasons_pandas,
    apply_threshold_value_exceeded_pandas,
    apply_threshold_value_not_exceeded_pandas,
    apply_water_year_pandas,
    apply_year_pandas,
)
from teehr.calculated_fields.base_models import CalculatedFieldBaseModel
from teehr.calculated_fields.row_level_models import (
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
from teehr.calculated_fields.timeseries_aware_models import (
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


def _seconds_to_iso_expr(sec_col: F.Column) -> F.Column:
    """Convert a seconds Column to an ISO 8601 duration string Column.

    Replicates the output of ``_timedelta_to_iso_duration`` for Spark-native
    execution so that bin IDs produced by the Spark and pandas paths match.
    """
    sec = sec_col.cast("long")
    d = (sec / F.lit(86400)).cast("long")
    rem1 = sec - d * F.lit(86400)
    h = (rem1 / F.lit(3600)).cast("long")
    rem2 = rem1 - h * F.lit(3600)
    m = (rem2 / F.lit(60)).cast("long")
    s = rem2 - m * F.lit(60)

    # When the duration spans at least one day, always emit the hours component
    # (matching _timedelta_to_iso_duration which leaves "T0H" in that case).
    t_with_days = F.when(
        (m == 0) & (s == 0), F.concat(F.lit("T"), h.cast("string"), F.lit("H"))
    ).when(
        s == 0,
        F.concat(F.lit("T"), h.cast("string"), F.lit("H"), m.cast("string"), F.lit("M")),
    ).otherwise(
        F.concat(
            F.lit("T"), h.cast("string"), F.lit("H"),
            m.cast("string"), F.lit("M"),
            s.cast("string"), F.lit("S"),
        )
    )

    # Without days: only include non-zero components; zero total → "T0S".
    t_no_days = F.when(
        (h == 0) & (m == 0) & (s == 0), F.lit("T0S")
    ).when(
        (m == 0) & (s == 0), F.concat(F.lit("T"), h.cast("string"), F.lit("H"))
    ).when(
        s == 0,
        F.concat(F.lit("T"), h.cast("string"), F.lit("H"), m.cast("string"), F.lit("M")),
    ).otherwise(
        F.concat(
            F.lit("T"), h.cast("string"), F.lit("H"),
            m.cast("string"), F.lit("M"),
            s.cast("string"), F.lit("S"),
        )
    )

    return F.when(
        d > 0,
        F.concat(F.lit("P"), d.cast("string"), F.lit("D"), t_with_days),
    ).otherwise(
        F.concat(F.lit("P"), t_no_days)
    )


def _apply_exceedance_probability_spark(
    sdf: DataFrame, cf: CalculatedFieldBaseModel
) -> DataFrame:
    """Compute ExceedanceProbability via window RANK / (COUNT + 1)."""
    group_cols = _normalize_fields(cf.uniqueness_fields)
    value_col = cf.value_field_name

    rank_window = Window.partitionBy(*group_cols).orderBy(
        F.col(value_col).cast("double").desc()
    )
    count_window = Window.partitionBy(*group_cols)

    rank = F.rank().over(rank_window).cast("double")
    n = F.count(F.lit(1)).over(count_window).cast("double")
    ep = rank / (n + F.lit(1.0))

    if cf.as_percentile:
        ep = ep * F.lit(100.0)

    return sdf.withColumn(cf.output_field_name, ep)


def _apply_baseflow_period_spark(
    sdf: DataFrame, cf: CalculatedFieldBaseModel
) -> DataFrame:
    """Compute BaseflowPeriodDetection via row-level arithmetic + _add_event_ids."""
    if cf.baseflow_field_name is None:
        raise ValueError(
            "BaseflowPeriodDetection requires baseflow_field_name to be specified."
        )

    group_cols = _normalize_fields(cf.uniqueness_fields)
    streamflow = F.col(cf.value_field_name).cast("double")
    baseflow = F.col(cf.baseflow_field_name).cast("double")
    quickflow_adj = (streamflow - baseflow) * F.lit(float(cf.event_threshold))

    sdf = sdf.withColumn(
        cf.output_baseflow_period_field_name,
        F.coalesce(baseflow > quickflow_adj, F.lit(False)).cast("boolean"),
    )

    # Compute event IDs for all rows using the shared helper, then null-out
    # rows where the period flag is False (matching the pandas path behaviour).
    _tmp = f"__bfp_id_{cf.output_baseflow_period_id_field_name}"
    sdf = _add_event_ids(
        sdf=sdf,
        group_cols=group_cols,
        time_col=cf.value_time_field_name,
        event_col=cf.output_baseflow_period_field_name,
        output_event_id_col=_tmp,
    )
    sdf = sdf.withColumn(
        cf.output_baseflow_period_id_field_name,
        F.when(F.col(cf.output_baseflow_period_field_name), F.col(_tmp)).otherwise(
            F.lit(None)
        ),
    ).drop(_tmp)

    return sdf


def _apply_lead_time_bins_spark(
    sdf: DataFrame, cf: CalculatedFieldBaseModel
) -> DataFrame:
    """Compute ForecastLeadTimeBins via Spark-native arithmetic / when-otherwise."""
    import pandas as pd
    from teehr.calculated_fields.row_level_pandas_spark import (
        validate_forecast_lead_time_bin_size,
        _timedelta_to_iso_duration,
        apply_forecast_lead_time,
    )

    if cf.lead_time_field_name not in sdf.columns:
        sdf = apply_forecast_lead_time(
            sdf,
            value_time_field_name=cf.value_time_field_name,
            reference_time_field_name=cf.reference_time_field_name,
            output_field_name=cf.lead_time_field_name,
        )

    normalized = validate_forecast_lead_time_bin_size(cf.bin_size)

    # Derive lead time in integer seconds from raw timestamps (type-safe vs
    # Spark's DayTimeIntervalType).
    lead_sec = (
        F.unix_timestamp(F.col(cf.value_time_field_name))
        - F.unix_timestamp(F.col(cf.reference_time_field_name))
    ).cast("long")

    if isinstance(normalized, pd.Timedelta):
        # Uniform binning: floor(lead / bin_size) gives bin number; build ISO
        # duration strings from bin start/end arithmetic.
        bin_size_sec = F.lit(int(normalized.total_seconds()))
        bin_num = (lead_sec / bin_size_sec).cast("long")
        start_sec = bin_num * bin_size_sec
        end_sec = (bin_num + F.lit(1)) * bin_size_sec
        bin_id_expr = F.concat(
            _seconds_to_iso_expr(start_sec),
            F.lit("_"),
            _seconds_to_iso_expr(end_sec),
        )
    else:
        # Variable binning: pre-compute bin IDs in the driver, then build a
        # when/otherwise chain. An overflow bin is added when the data contains
        # lead times beyond the last declared boundary (requires one collect).
        max_row = sdf.agg(F.max(lead_sec).alias("_max_ls")).collect()[0]
        max_lead_sec = max_row["_max_ls"]

        bins_to_use = []
        for start_td, end_td, bin_id in normalized:
            final_id = (
                bin_id
                if bin_id is not None
                else f"{_timedelta_to_iso_duration(start_td)}_{_timedelta_to_iso_duration(end_td)}"
            )
            bins_to_use.append(
                (int(start_td.total_seconds()), int(end_td.total_seconds()), final_id)
            )

        last_end_sec = bins_to_use[-1][1]
        if max_lead_sec is not None and max_lead_sec >= last_end_sec:
            overflow_start = last_end_sec
            overflow_end = int(max_lead_sec)
            if normalized[-1][2] is None:
                overflow_id = (
                    f"{_timedelta_to_iso_duration(pd.Timedelta(seconds=overflow_start))}_"
                    f"{_timedelta_to_iso_duration(pd.Timedelta(seconds=overflow_end))}"
                )
            else:
                overflow_id = "overflow"
            bins_to_use.append((overflow_start, overflow_end, overflow_id))

        # Build the when/otherwise chain (first matching condition wins).
        first_start_s, first_end_s, first_bid = bins_to_use[0]
        first_cond = (
            lead_sec >= F.lit(first_start_s)
            if len(bins_to_use) == 1
            else (lead_sec >= F.lit(first_start_s)) & (lead_sec < F.lit(first_end_s))
        )
        bin_id_expr = F.when(first_cond, F.lit(first_bid))

        for i in range(1, len(bins_to_use)):
            start_s, end_s, bid = bins_to_use[i]
            is_last = i == len(bins_to_use) - 1
            cond = (
                lead_sec >= F.lit(start_s)
                if is_last
                else (lead_sec >= F.lit(start_s)) & (lead_sec < F.lit(end_s))
            )
            bin_id_expr = bin_id_expr.when(cond, F.lit(bid))

        bin_id_expr = bin_id_expr.otherwise(F.lit(None).cast("string"))

    return sdf.withColumn(cf.output_field_name, bin_id_expr)


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
        apply_field=_apply_with_model,
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
