"""Spark-native executors for timeseries-aware calculated fields.

These helpers centralize the Spark execution path so the engine module can
focus on routing and adapter orchestration.
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

from teehr.calculated_fields.models.base import CalculatedFieldBaseModel
from teehr.calculated_fields.models.timeseries_aware import (
    AbovePercentileEventDetection,
    AboveThresholdEventDetection,
)


def _normalize_fields(
    fields: str | list[str] | None,
    default_uniqueness_fields: Sequence[str],
) -> list[str]:
    if fields is None:
        return list(default_uniqueness_fields)
    if isinstance(fields, str):
        return [fields]
    return list(fields)


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


def apply_percentile_batch_spark(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
    default_uniqueness_fields: Sequence[str],
) -> DataFrame:
    """Apply one Spark-native percentile detection batch for compatible fields."""
    first = cfs[0]
    group_cols = _normalize_fields(first.uniqueness_fields, default_uniqueness_fields)
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


def apply_threshold_batch_spark(
    sdf: DataFrame,
    cfs: list[CalculatedFieldBaseModel],
    default_uniqueness_fields: Sequence[str],
) -> DataFrame:
    """Apply one Spark-native threshold detection batch for compatible fields."""
    first = cfs[0]
    group_cols = _normalize_fields(first.uniqueness_fields, default_uniqueness_fields)
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


def _seconds_to_iso_expr(sec_col: F.Column) -> F.Column:
    """Convert a seconds Column to an ISO 8601 duration string Column."""
    sec = sec_col.cast("long")
    d = (sec / F.lit(86400)).cast("long")
    rem1 = sec - d * F.lit(86400)
    h = (rem1 / F.lit(3600)).cast("long")
    rem2 = rem1 - h * F.lit(3600)
    m = (rem2 / F.lit(60)).cast("long")
    s = rem2 - m * F.lit(60)

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


def apply_exceedance_probability_spark(
    sdf: DataFrame,
    cf: CalculatedFieldBaseModel,
    default_uniqueness_fields: Sequence[str],
) -> DataFrame:
    """Compute ExceedanceProbability via window RANK / (COUNT + 1)."""
    group_cols = _normalize_fields(cf.uniqueness_fields, default_uniqueness_fields)
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


def apply_baseflow_period_spark(
    sdf: DataFrame,
    cf: CalculatedFieldBaseModel,
    default_uniqueness_fields: Sequence[str],
) -> DataFrame:
    """Compute BaseflowPeriodDetection via row-level arithmetic + event IDs."""
    if cf.baseflow_field_name is None:
        raise ValueError(
            "BaseflowPeriodDetection requires baseflow_field_name to be specified."
        )

    group_cols = _normalize_fields(cf.uniqueness_fields, default_uniqueness_fields)
    streamflow = F.col(cf.value_field_name).cast("double")
    baseflow = F.col(cf.baseflow_field_name).cast("double")
    quickflow_adj = (streamflow - baseflow) * F.lit(float(cf.event_threshold))

    sdf = sdf.withColumn(
        cf.output_baseflow_period_field_name,
        F.coalesce(baseflow > quickflow_adj, F.lit(False)).cast("boolean"),
    )

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


def apply_lead_time_bins_spark(
    sdf: DataFrame,
    cf: CalculatedFieldBaseModel,
) -> DataFrame:
    """Compute ForecastLeadTimeBins via Spark-native arithmetic / when-otherwise."""
    from teehr.calculated_fields.row_level_spark import (
        _timedelta_to_iso_duration,
        apply_forecast_lead_time,
        validate_forecast_lead_time_bin_size,
    )

    if cf.lead_time_field_name not in sdf.columns:
        sdf = apply_forecast_lead_time(
            sdf,
            value_time_field_name=cf.value_time_field_name,
            reference_time_field_name=cf.reference_time_field_name,
            output_field_name=cf.lead_time_field_name,
        )

    normalized = validate_forecast_lead_time_bin_size(cf.bin_size)

    lead_sec = (
        F.unix_timestamp(F.col(cf.value_time_field_name))
        - F.unix_timestamp(F.col(cf.reference_time_field_name))
    ).cast("long")

    if isinstance(normalized, pd.Timedelta):
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
