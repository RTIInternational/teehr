"""Spark-native metric planning and aggregation helpers."""

from __future__ import annotations

from typing import Iterable, List, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from teehr.metrics.models.base import MetricsBasemodel
from teehr.metrics.adapters import (
    MetricExecutionAdapter,
    contiguous_batch_adapter,
    single_metric_adapter,
)
from teehr.querying.utils import parse_fields_to_list, validate_fields_exist
from teehr.utils.spark import null_safe_join_on_columns

EPSILON = 1e-6

SUPPORTED_SIGNATURE_METRICS = {
    "Count",
    "Minimum",
    "Maximum",
    "Average",
    "Sum",
    "Variance",
    "MaxValueTime",
}

SUPPORTED_DETERMINISTIC_METRICS = {
    "MeanError",
    "RelativeBias",
    "MultiplicativeBias",
    "MeanSquareError",
    "RootMeanSquareError",
    "MeanAbsoluteError",
    "MeanAbsoluteRelativeError",
    "PearsonCorrelation",
    "Rsquared",
    "NashSutcliffeEfficiency",
    "NormalizedNashSutcliffeEfficiency",
    "VariabilityRatio",
    "RootMeanStandardDeviationRatio",
    "KlingGuptaEfficiency",
    "KlingGuptaEfficiencyMod1",
    "KlingGuptaEfficiencyMod2",
    "RelativeMean",
    "RelativeMedian",
    "RelativeMinimum",
    "RelativeMaximum",
    "RelativeStandardDeviation",
    "MaxValueDelta",
    "ConfusionMatrix",
    "FalseAlarmRatio",
    "ProbabilityOfDetection",
    "ProbabilityOfFalseDetection",
    "CriticalSuccessIndex",
    "SuccessRatio",
    "FrequencyBiasIndex",
}

SUPPORTED_FDC_METRICS = {
    "FlowDurationCurveSlope",
}

SUPPORTED_SPEARMAN_METRICS = {
    "SpearmanCorrelation",
}

SUPPORTED_TIMEDELTA_METRICS = {
    "MaxValueTimeDelta",
}

SUPPORTED_ANNUAL_PEAK_METRICS = {
    "AnnualPeakRelativeBias",
}

SUPPORTED_CENTER_OF_TIMING_METRICS = {
    "CenterOfTiming",
}

SUPPORTED_STANDARD_DEVIATION_OF_TIMING_METRICS = {
    "StandardDeviationOfTiming"
}

SUPPORTED_METRICS = (
    SUPPORTED_SIGNATURE_METRICS
    | SUPPORTED_DETERMINISTIC_METRICS
    | SUPPORTED_FDC_METRICS
    | SUPPORTED_SPEARMAN_METRICS
    | SUPPORTED_TIMEDELTA_METRICS
    | SUPPORTED_ANNUAL_PEAK_METRICS
    | SUPPORTED_CENTER_OF_TIMING_METRICS
    | SUPPORTED_STANDARD_DEVIATION_OF_TIMING_METRICS
)


def _field_name(value, default: str | None = None) -> str | None:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return value.value


def _metric_class_name(metric: MetricsBasemodel) -> str:
    return metric.__class__.__name__


def _is_signature_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_SIGNATURE_METRICS


def _is_deterministic_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_DETERMINISTIC_METRICS


def _is_fdc_slope_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_FDC_METRICS


def _is_spearman_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_SPEARMAN_METRICS


def _is_timedelta_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_TIMEDELTA_METRICS


def _is_annual_peak_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_ANNUAL_PEAK_METRICS


def _is_center_of_timing_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_CENTER_OF_TIMING_METRICS


def _is_standard_deviation_of_timing_metric(metric: MetricsBasemodel) -> bool:
    return _metric_class_name(metric) in SUPPORTED_STANDARD_DEVIATION_OF_TIMING_METRICS


def supports_spark_native(metric_model: MetricsBasemodel) -> bool:
    """Return True when a metric can run on the Spark-native path."""
    class_name = _metric_class_name(metric_model)
    bootstrap = getattr(metric_model, "bootstrap", None)
    transform = getattr(metric_model, "transform", None)
    return (
        bootstrap is None
        and transform is None
        and class_name in SUPPORTED_METRICS
    )


def build_metric_plan(
    metrics: Iterable[MetricsBasemodel],
) -> Tuple[List[MetricsBasemodel], List[MetricsBasemodel]]:
    """Split metrics into Spark-native and Python fallback groups."""
    spark_metrics: List[MetricsBasemodel] = []
    python_metrics: List[MetricsBasemodel] = []

    for metric in metrics:
        if supports_spark_native(metric):
            spark_metrics.append(metric)
        else:
            python_metrics.append(metric)

    return spark_metrics, python_metrics


def signature_agg_expr(metric: MetricsBasemodel):
    """Return Spark aggregation expression for supported signatures."""
    class_name = _metric_class_name(metric)
    input_fields = metric.get_input_field_names()
    primary_col = _field_name(input_fields[0] if input_fields else None, "primary_value")
    pcol = F.col(primary_col).cast("double")

    if class_name == "Count":
        return F.count(pcol)
    if class_name == "Minimum":
        return F.min(pcol)
    if class_name == "Maximum":
        return F.max(pcol)
    if class_name == "Average":
        return F.avg(pcol)
    if class_name == "Sum":
        return F.sum(pcol)
    if class_name == "Variance":
        return F.var_pop(pcol)
    if class_name == "MaxValueTime":
        value_time_col = _field_name(metric.value_time_field_name, "value_time")
        return F.max_by(F.col(value_time_col), pcol)

    raise ValueError(f"Unsupported spark-native signature metric: {class_name}")


def _deterministic_fields(metrics: List[MetricsBasemodel]) -> Tuple[str, str]:
    p_cols = {
        _field_name(metric.primary_field_name, "primary_value")
        for metric in metrics
    }
    s_cols = {
        _field_name(metric.secondary_field_name, "secondary_value")
        for metric in metrics
    }

    if len(p_cols) != 1 or len(s_cols) != 1:
        raise ValueError(
            "Spark-native deterministic metrics require a single shared "
            "primary_field_name and secondary_field_name in one aggregate call."
        )

    return next(iter(p_cols)), next(iter(s_cols))


def _nan():
    return F.lit(float("nan"))


def _divide(numerator, denominator):
    """Safe divide that returns null for zero denominator in ANSI mode."""
    return F.try_divide(numerator, denominator)


def _ratio(numerator, denominator, use_epsilon: bool):
    if use_epsilon:
        return numerator / (denominator + F.lit(EPSILON))
    return _divide(numerator, denominator)


def _compute_signature_metrics(
    sdf: DataFrame,
    group_by_cols: List[str],
    metrics: List[MetricsBasemodel],
) -> DataFrame | None:
    if not metrics:
        return None

    required_fields = set(group_by_cols)
    for metric in metrics:
        input_fields = metric.get_input_field_names()
        required_fields.add(_field_name(input_fields[0] if input_fields else None, "primary_value"))
        if _metric_class_name(metric) == "MaxValueTime":
            required_fields.add(_field_name(metric.value_time_field_name, "value_time"))

    validate_fields_exist(sdf.columns, sorted(required_fields))

    agg_exprs = [signature_agg_expr(metric).alias(metric.output_field_name) for metric in metrics]
    return sdf.groupBy(*group_by_cols).agg(*agg_exprs)


def _compute_deterministic_metrics(
    sdf: DataFrame,
    group_by_cols: List[str],
    metrics: List[MetricsBasemodel],
) -> DataFrame | None:
    if not metrics:
        return None

    p_col, s_col = _deterministic_fields(metrics)
    validate_fields_exist(sdf.columns, group_by_cols + [p_col, s_col])

    p = F.col(p_col).cast("double")
    s = F.col(s_col).cast("double")
    valid = p.isNotNull() & s.isNotNull()

    requested_class_names = {_metric_class_name(metric) for metric in metrics}
    needs_median = "RelativeMedian" in requested_class_names
    needs_min = "RelativeMinimum" in requested_class_names
    needs_max = (
        "RelativeMaximum" in requested_class_names
        or "MaxValueDelta" in requested_class_names
    )
    threshold_metric_names = {
        "ConfusionMatrix",
        "FalseAlarmRatio",
        "ProbabilityOfDetection",
        "ProbabilityOfFalseDetection",
        "CriticalSuccessIndex",
        "SuccessRatio",
        "FrequencyBiasIndex",
    }
    needs_threshold_counts = bool(requested_class_names & threshold_metric_names)

    threshold_col = None
    if needs_threshold_counts:
        threshold_cols = {
            _field_name(getattr(metric, "threshold_field_name", None))
            for metric in metrics
            if _metric_class_name(metric) in threshold_metric_names
        }
        threshold_cols.discard(None)

        if len(threshold_cols) != 1:
            raise ValueError(
                "Spark-native threshold metrics require a single shared "
                "threshold_field_name in one aggregate call."
            )

        threshold_col = next(iter(threshold_cols))
        validate_fields_exist(sdf.columns, [threshold_col])

    agg_exprs = [
        F.count(F.lit(1)).alias("_n"),
        F.sum(p).alias("_sum_p"),
        F.sum(s).alias("_sum_s"),
        F.avg(p).alias("_mean_p"),
        F.avg(s).alias("_mean_s"),
        F.sum(s - p).alias("_sum_diff"),
        F.sum(F.abs(s - p)).alias("_sum_abs_diff"),
        F.sum((s - p) * (s - p)).alias("_sum_sq_diff"),
        F.sum(p * p).alias("_sum_p2"),
        F.stddev_pop(p).alias("_std_p"),
        F.stddev_pop(s).alias("_std_s"),
        F.try_divide(F.covar_pop(p, s), F.stddev_pop(p) * F.stddev_pop(s)).alias("_corr_ps"),
        F.covar_pop(p, s).alias("_cov_pop_ps"),
    ]

    if needs_median:
        agg_exprs.extend(
            [
                F.expr(f"percentile_approx(CAST({p_col} AS DOUBLE), 0.5, 10000)").alias("_median_p"),
                F.expr(f"percentile_approx(CAST({s_col} AS DOUBLE), 0.5, 10000)").alias("_median_s"),
            ]
        )
    if needs_min:
        agg_exprs.extend([F.min(p).alias("_min_p"), F.min(s).alias("_min_s")])
    if needs_max:
        agg_exprs.extend([F.max(p).alias("_max_p"), F.max(s).alias("_max_s")])
    if needs_threshold_counts:
        thr = F.col(threshold_col).cast("double")
        agg_exprs.extend(
            [
                F.countDistinct(F.col(threshold_col)).alias("_n_thresholds"),
                F.sum(F.when((p >= thr) & (s >= thr), F.lit(1)).otherwise(F.lit(0))).alias("_tp"),
                F.sum(F.when((p < thr) & (s < thr), F.lit(1)).otherwise(F.lit(0))).alias("_tn"),
                F.sum(F.when((p < thr) & (s >= thr), F.lit(1)).otherwise(F.lit(0))).alias("_fp"),
                F.sum(F.when((p >= thr) & (s < thr), F.lit(1)).otherwise(F.lit(0))).alias("_fn"),
            ]
        )

    stats_df = sdf.where(valid).groupBy(*group_by_cols).agg(*agg_exprs)

    if needs_threshold_counts:
        # Keep threshold validation in the Spark plan so errors are raised lazily
        # when an action is triggered, rather than during DataFrame construction.
        stats_df = stats_df.where(
            F.coalesce(
                F.assert_true(
                    F.col("_n_thresholds") <= F.lit(1),
                    "Threshold field must contain a single unique value for each population grouping.",
                ),
                F.lit(True),
            )
        )

    metric_exprs = []
    for metric in metrics:
        class_name = _metric_class_name(metric)
        mean_error = _divide(F.col("_sum_diff"), F.col("_n"))
        mse = _divide(F.col("_sum_sq_diff"), F.col("_n"))
        rmse = F.sqrt(mse)

        if class_name == "MeanError":
            expr = mean_error
        elif class_name == "RelativeBias":
            expr = _ratio(F.col("_sum_diff"), F.col("_sum_p"), metric.add_epsilon)
        elif class_name == "MultiplicativeBias":
            expr = _ratio(F.col("_mean_s"), F.col("_mean_p"), metric.add_epsilon)
        elif class_name == "MeanSquareError":
            expr = mse
        elif class_name == "RootMeanSquareError":
            expr = rmse
        elif class_name == "MeanAbsoluteError":
            expr = F.col("_sum_abs_diff") / F.col("_n")
        elif class_name == "MeanAbsoluteRelativeError":
            expr = _ratio(F.col("_sum_abs_diff"), F.col("_sum_p"), metric.add_epsilon)
        elif class_name == "PearsonCorrelation":
            if metric.add_epsilon:
                expr = _ratio(F.col("_cov_pop_ps"), (F.col("_std_p") * F.col("_std_s")), True)
            else:
                expr = F.col("_corr_ps")
        elif class_name == "Rsquared":
            if metric.add_epsilon:
                pearson = _ratio(F.col("_cov_pop_ps"), (F.col("_std_p") * F.col("_std_s")), True)
            else:
                pearson = F.col("_corr_ps")
            expr = pearson * pearson
        elif class_name == "VariabilityRatio":
            expr = _ratio(F.col("_std_s"), F.col("_std_p"), metric.add_epsilon)
        elif class_name == "RootMeanStandardDeviationRatio":
            expr = _ratio(rmse, F.col("_std_p"), metric.add_epsilon)
        elif class_name == "NashSutcliffeEfficiency":
            denom = F.col("_sum_p2") - (F.col("_n") * F.col("_mean_p") * F.col("_mean_p"))
            if metric.add_epsilon:
                denom = denom + F.lit(EPSILON)
            invalid = (F.col("_n") == 0) | (F.col("_sum_p") == 0) | (F.col("_sum_s") == 0) | (denom == 0)
            expr = F.when(invalid, _nan()).otherwise(F.lit(1.0) - _divide(F.col("_sum_sq_diff"), denom))
        elif class_name == "NormalizedNashSutcliffeEfficiency":
            denom = F.col("_sum_p2") - (F.col("_n") * F.col("_mean_p") * F.col("_mean_p"))
            if metric.add_epsilon:
                denom = denom + F.lit(EPSILON)
            invalid = (F.col("_n") == 0) | (F.col("_sum_p") == 0) | (F.col("_sum_s") == 0) | (denom == 0)
            expr = F.when(invalid, _nan()).otherwise(F.lit(1.0) / (F.lit(1.0) + _divide(F.col("_sum_sq_diff"), denom)))
        elif class_name == "KlingGuptaEfficiency":
            invalid = (F.col("_std_p") == 0) | (F.col("_std_s") == 0)
            linear_correlation = F.col("_corr_ps")
            relative_variability = _ratio(F.col("_std_s"), F.col("_std_p"), metric.add_epsilon)
            relative_mean = _ratio(F.col("_mean_s"), F.col("_mean_p"), metric.add_epsilon)
            euclidean_distance = F.sqrt(
                F.lit(metric.sr) * F.pow(linear_correlation - F.lit(1.0), 2)
                + F.lit(metric.sa) * F.pow(relative_variability - F.lit(1.0), 2)
                + F.lit(metric.sb) * F.pow(relative_mean - F.lit(1.0), 2)
            )
            expr = F.when(invalid, _nan()).otherwise(F.lit(1.0) - euclidean_distance)
        elif class_name == "KlingGuptaEfficiencyMod1":
            invalid = (F.col("_std_p") == 0) | (F.col("_std_s") == 0)
            linear_correlation = F.col("_corr_ps")
            variability_ratio = _divide(
                _ratio(F.col("_std_s"), F.col("_mean_s"), metric.add_epsilon),
                _ratio(F.col("_std_p"), F.col("_mean_p"), metric.add_epsilon),
            )
            relative_mean = _ratio(F.col("_mean_s"), F.col("_mean_p"), metric.add_epsilon)
            euclidean_distance = F.sqrt(
                F.lit(metric.sr) * F.pow(linear_correlation - F.lit(1.0), 2)
                + F.lit(metric.sa) * F.pow(variability_ratio - F.lit(1.0), 2)
                + F.lit(metric.sb) * F.pow(relative_mean - F.lit(1.0), 2)
            )
            expr = F.when(invalid, _nan()).otherwise(F.lit(1.0) - euclidean_distance)
        elif class_name == "KlingGuptaEfficiencyMod2":
            invalid = (F.col("_std_p") == 0) | (F.col("_std_s") == 0)
            linear_correlation = F.col("_corr_ps")
            relative_variability = _ratio(F.col("_std_s"), F.col("_std_p"), metric.add_epsilon)
            if metric.add_epsilon:
                bias_component = _divide(
                    F.pow(F.col("_mean_s") - F.col("_mean_p"), 2),
                    F.pow(F.col("_std_p"), 2) + F.lit(EPSILON),
                )
            else:
                bias_component = _divide(
                    F.pow(F.col("_mean_s") - F.col("_mean_p"), 2),
                    F.pow(F.col("_std_p"), 2),
                )
            euclidean_distance = F.sqrt(
                F.lit(metric.sr) * F.pow(linear_correlation - F.lit(1.0), 2)
                + F.lit(metric.sa) * F.pow(relative_variability - F.lit(1.0), 2)
                + F.lit(metric.sb) * bias_component
            )
            expr = F.when(invalid, _nan()).otherwise(F.lit(1.0) - euclidean_distance)
        elif class_name == "RelativeMean":
            expr = _ratio(F.col("_mean_s"), F.col("_mean_p"), metric.add_epsilon)
        elif class_name == "RelativeMedian":
            expr = _ratio(F.col("_median_s"), F.col("_median_p"), metric.add_epsilon)
        elif class_name == "RelativeMinimum":
            expr = _ratio(F.col("_min_s"), F.col("_min_p"), metric.add_epsilon)
        elif class_name == "RelativeMaximum":
            expr = _ratio(F.col("_max_s"), F.col("_max_p"), metric.add_epsilon)
        elif class_name == "RelativeStandardDeviation":
            expr = _ratio(F.col("_std_s"), F.col("_std_p"), metric.add_epsilon)
        elif class_name == "MaxValueDelta":
            expr = F.col("_max_s") - F.col("_max_p")
        elif class_name == "ConfusionMatrix":
            expr = F.create_map(
                F.lit("TP"), F.col("_tp").cast("int"),
                F.lit("TN"), F.col("_tn").cast("int"),
                F.lit("FP"), F.col("_fp").cast("int"),
                F.lit("FN"), F.col("_fn").cast("int"),
            )
        elif class_name == "FalseAlarmRatio":
            expr = F.when(
                (F.col("_tp") + F.col("_fp")) == 0,
                _nan(),
            ).otherwise(_divide(F.col("_fp"), F.col("_tp") + F.col("_fp")))
        elif class_name == "ProbabilityOfDetection":
            expr = F.when(
                (F.col("_tp") + F.col("_fn")) == 0,
                _nan(),
            ).otherwise(_divide(F.col("_tp"), F.col("_tp") + F.col("_fn")))
        elif class_name == "ProbabilityOfFalseDetection":
            expr = F.when(
                (F.col("_fp") + F.col("_tn")) == 0,
                _nan(),
            ).otherwise(_divide(F.col("_fp"), F.col("_fp") + F.col("_tn")))
        elif class_name == "CriticalSuccessIndex":
            expr = F.when(
                (F.col("_tp") + F.col("_fp") + F.col("_fn")) == 0,
                _nan(),
            ).otherwise(_divide(F.col("_tp"), F.col("_tp") + F.col("_fp") + F.col("_fn")))
        elif class_name == "SuccessRatio":
            expr = F.when(
                (F.col("_tp") + F.col("_tn") + F.col("_fp") + F.col("_fn")) == 0,
                _nan(),
            ).otherwise(
                _divide(
                    F.col("_tp") + F.col("_tn"),
                    F.col("_tp") + F.col("_tn") + F.col("_fp") + F.col("_fn"),
                )
            )
        elif class_name == "FrequencyBiasIndex":
            expr = F.when(
                (F.col("_tp") + F.col("_fn")) == 0,
                _nan(),
            ).otherwise(_divide(F.col("_tp") + F.col("_fp"), F.col("_tp") + F.col("_fn")))
        else:
            raise ValueError(f"Unsupported spark-native deterministic metric: {class_name}")

        metric_exprs.append(expr.alias(metric.output_field_name))

    return stats_df.select(*group_by_cols, *metric_exprs)


def _compute_fdc_slope_metric(
    sdf: DataFrame,
    group_by_cols: List[str],
    metric: MetricsBasemodel,
) -> DataFrame | None:
    """Compute FlowDurationCurveSlope via per-group Weibull plotting positions.

    Algorithm mirrors the pandas path exactly:
    - Sort values descending within each group.
    - Assign exceedance probability prob[i] = (row_num - 1) / (n + 1).
    - Pick the row whose prob is nearest to each target quantile.
    - Slope = (value_upper - value_lower) / (prob_upper - prob_lower),
      with the probability denominator optionally scaled to percentile range.
    """
    from pyspark.sql import Window as W

    p_col = _field_name(getattr(metric, "primary_field_name", None), "primary_value")
    validate_fields_exist(sdf.columns, group_by_cols + [p_col])

    lower_q = float(metric.lower_quantile)
    upper_q = float(metric.upper_quantile)

    if not (0 <= lower_q < upper_q <= 1):
        raise ValueError(
            "FlowDurationCurveSlope requires 0 <= lower_quantile < upper_quantile <= 1."
        )

    # Assign each row a 0-based rank (descending value order) within its group
    # and compute Weibull exceedance probability: prob = (row_num - 1) / (n + 1)
    w_ord = W.partitionBy(*group_by_cols).orderBy(
        F.col(p_col).cast("double").desc(), F.monotonically_increasing_id()
    )
    w_par = W.partitionBy(*group_by_cols)

    sdf2 = (
        sdf.withColumn("_rn", F.row_number().over(w_ord))
        .withColumn("_n", F.count(F.lit(1)).over(w_par))
        .withColumn(
            "_prob",
            (F.col("_rn").cast("double") - F.lit(1.0))
            / (F.col("_n").cast("double") + F.lit(1.0)),
        )
    )

    # Pick the nearest row to each target quantile using min(struct) which
    # sorts lexicographically: first by distance, then by value (tie-break).
    agg_df = sdf2.groupBy(*group_by_cols).agg(
        F.min(
            F.struct(
                F.abs(F.col("_prob") - F.lit(lower_q)).alias("dist"),
                F.col(p_col).cast("double").alias("val"),
                F.col("_prob").alias("prob"),
            )
        ).alias("_lower"),
        F.min(
            F.struct(
                F.abs(F.col("_prob") - F.lit(upper_q)).alias("dist"),
                F.col(p_col).cast("double").alias("val"),
                F.col("_prob").alias("prob"),
            )
        ).alias("_upper"),
    )

    # Scale exceedance probabilities to percentile range (0-100) if requested.
    scale = F.lit(100.0) if metric.as_percentile else F.lit(1.0)
    lower_prob = F.col("_lower.prob") * scale
    upper_prob = F.col("_upper.prob") * scale
    denom = upper_prob - lower_prob
    if metric.add_epsilon:
        denom = denom + F.lit(EPSILON)

    slope_expr = _divide(F.col("_upper.val") - F.col("_lower.val"), denom)

    return agg_df.select(
        *group_by_cols,
        slope_expr.alias(metric.output_field_name),
    )


def _compute_center_of_timing_metric(
    sdf: DataFrame,
    group_by_cols: List[str],
    metric: MetricsBasemodel,
) -> DataFrame | None:
    """Compute CenterOfTiming via weighted mean of day-of-water-year.

    Mirrors the pandas path exactly:
    1) Resample to daily (avg per date within each group).
    2) If count of valid daily values < (1 - missing_threshold) * 366, return null.
    3) Compute day-of-water-year: Oct 1 is day 1.
    4) Filter to non-zero daily values.
    5) CT = sum(p_daily * day_of_WY) / sum(p_daily).
    """
    p_col = _field_name(getattr(metric, "primary_field_name", None), "primary_value")
    t_col = _field_name(getattr(metric, "value_time_field_name", None), "value_time")
    validate_fields_exist(sdf.columns, group_by_cols + [p_col, t_col])

    p = F.col(p_col).cast("double")
    t = F.col(t_col)
    missing_threshold = float(getattr(metric, "missing_threshold", 0.1))

    # Step 1: Resample to daily — avg per calendar date per group
    daily_df = (
        sdf.where(p.isNotNull() & t.isNotNull())
        .withColumn("_date", F.to_date(t))
        .groupBy(*group_by_cols, "_date")
        .agg(F.avg(p).alias("_p_daily"))
        .where(F.col("_p_daily").isNotNull())
    )

    # Step 2: Day-of-water-year (WY starts Oct 1)
    wy_start = F.when(
        F.month(F.col("_date")) >= 10,
        F.make_date(F.year(F.col("_date")), F.lit(10), F.lit(1)),
    ).otherwise(
        F.make_date(F.year(F.col("_date")) - 1, F.lit(10), F.lit(1)),
    )
    daily_df = daily_df.withColumn(
        "_day_of_wy",
        (F.datediff(F.col("_date"), wy_start) + 1).cast("double"),
    )

    # Step 3: Count valid daily rows per group (for missing-threshold check)
    count_df = daily_df.groupBy(*group_by_cols).agg(
        F.count(F.lit(1)).alias("_n_daily")
    )

    # Step 4: Non-zero weighted sums
    ct_df = (
        daily_df
        .where(F.col("_p_daily") > 0)
        .groupBy(*group_by_cols)
        .agg(
            F.sum(F.col("_p_daily") * F.col("_day_of_wy")).alias("_sum_pw"),
            F.sum(F.col("_p_daily")).alias("_sum_p"),
        )
    )

    result_df = null_safe_join_on_columns(
        count_df,
        ct_df,
        join_columns=group_by_cols,
        how="left",
    )

    min_days = (1.0 - missing_threshold) * 366.0
    ct_expr = F.when(
        F.col("_n_daily") < F.lit(min_days),
        F.lit(None).cast("double"),
    ).otherwise(
        _divide(F.col("_sum_pw"), F.col("_sum_p"))
    )

    return result_df.select(
        *group_by_cols,
        ct_expr.alias(metric.output_field_name),
    )


def _compute_standard_deviation_of_timing_metric(
    sdf: DataFrame,
    group_by_cols: List[str],
    metric: MetricsBasemodel,
) -> DataFrame | None:
    """Compute StandardDeviationOfTiming as a weighted sample std-dev of day-of-WY.

    Mirrors the pandas path:
    1) Resample to daily (avg per date per group).
    2) If valid daily count < (1 - missing_threshold) * 366, return null.
    3) Day-of-water-year: Oct 1 = day 1.
    4) Filter to non-zero daily values.
    5) Single-pass aggregation collecting sum_pw, sum_p, sum_pd2, n_prime.
    6) Apply König-Steiner identity: numerator = sum_pd2 - sum_pw² / sum_p.
    7) SDoT = sqrt(numerator / (sum_p * (n_prime - 1))), null if n_prime <= 1.
    """
    p_col = _field_name(getattr(metric, "primary_field_name", None), "primary_value")
    t_col = _field_name(getattr(metric, "value_time_field_name", None), "value_time")
    validate_fields_exist(sdf.columns, group_by_cols + [p_col, t_col])

    p = F.col(p_col).cast("double")
    t = F.col(t_col)
    missing_threshold = float(getattr(metric, "missing_threshold", 0.1))

    # Step 1: Resample to daily
    daily_df = (
        sdf.where(p.isNotNull() & t.isNotNull())
        .withColumn("_date", F.to_date(t))
        .groupBy(*group_by_cols, "_date")
        .agg(F.avg(p).alias("_p_daily"))
        .where(F.col("_p_daily").isNotNull())
    )

    # Step 2: Day-of-water-year (WY starts Oct 1)
    wy_start = F.when(
        F.month(F.col("_date")) >= 10,
        F.make_date(F.year(F.col("_date")), F.lit(10), F.lit(1)),
    ).otherwise(
        F.make_date(F.year(F.col("_date")) - 1, F.lit(10), F.lit(1)),
    )
    daily_df = daily_df.withColumn(
        "_day_of_wy",
        (F.datediff(F.col("_date"), wy_start) + 1).cast("double"),
    )

    # Step 3: Total daily count for missing-threshold gate
    count_df = daily_df.groupBy(*group_by_cols).agg(
        F.count(F.lit(1)).alias("_n_daily")
    )

    # Step 4-5: Single-pass weighted stats over non-zero rows only
    pw = F.col("_p_daily")
    d = F.col("_day_of_wy")
    stats_df = (
        daily_df
        .where(pw > 0)
        .groupBy(*group_by_cols)
        .agg(
            F.sum(pw * d).alias("_sum_pw"),       # sum(p * day)
            F.sum(pw).alias("_sum_p"),             # sum(p)
            F.sum(pw * d * d).alias("_sum_pd2"),   # sum(p * day²)
            F.count(F.lit(1)).alias("_n_prime"),   # count of non-zero rows
        )
    )

    result_df = null_safe_join_on_columns(
        count_df,
        stats_df,
        join_columns=group_by_cols,
        how="left",
    )

    min_days = (1.0 - missing_threshold) * 366.0

    # Step 6: König-Steiner identity — no need to materialize CT
    numerator = (
        F.col("_sum_pd2")
        - (F.col("_sum_pw") * F.col("_sum_pw") / F.col("_sum_p"))
    )
    denominator = F.col("_sum_p") * (F.col("_n_prime") - F.lit(1))
    if metric.add_epsilon:
        denominator = denominator + F.lit(EPSILON)

    # Step 7: Guard conditions matching pandas behavior
    invalid = (
        (F.col("_n_daily") < F.lit(min_days))
        | F.col("_n_prime").isNull()
        | (F.col("_n_prime") <= F.lit(1))
    )
    sdot_expr = F.when(invalid, F.lit(None).cast("double")).otherwise(
        F.sqrt(_divide(numerator, denominator))
    )

    return result_df.select(
        *group_by_cols,
        sdot_expr.alias(metric.output_field_name),
    )


def _compute_spearman_metric(
    sdf: DataFrame,
    group_by_cols: List[str],
    metric: MetricsBasemodel,
) -> DataFrame | None:
    """Compute SpearmanCorrelation via average-rank Pearson formula.

    Replicates the pandas path exactly:
      covariance  = np.cov(rank_p, rank_s)[0,1]   # ddof=1 (sample)
      std_p/std_s = np.std(rank_p/rank_s)          # ddof=0 (population)
      result      = covariance / (std_p * std_s)

    Ties receive the average of the ranks they would occupy (matches
    scipy.stats.rankdata method='average').
    """
    from pyspark.sql import Window as W

    p_col = _field_name(getattr(metric, "primary_field_name", None), "primary_value")
    s_col = _field_name(getattr(metric, "secondary_field_name", None), "secondary_value")
    validate_fields_exist(sdf.columns, group_by_cols + [p_col, s_col])

    p_expr = F.col(p_col).cast("double")
    s_expr = F.col(s_col).cast("double")

    # Rank windows: ascending order within each group.
    # rank() assigns the same minimum rank to all tied values.
    w_ord_p = W.partitionBy(*group_by_cols).orderBy(p_expr.asc())
    w_ord_s = W.partitionBy(*group_by_cols).orderBy(s_expr.asc())

    # Tie-count windows: partition by (group + exact value).
    w_tie_p = W.partitionBy(*(group_by_cols + [p_col]))
    w_tie_s = W.partitionBy(*(group_by_cols + [s_col]))

    # average rank = rank_min + (count_ties - 1) / 2
    sdf2 = (
        sdf.withColumn("_rank_min_p", F.rank().over(w_ord_p))
        .withColumn("_tie_cnt_p", F.count(F.lit(1)).over(w_tie_p))
        .withColumn(
            "_rank_p",
            F.col("_rank_min_p").cast("double")
            + (F.col("_tie_cnt_p").cast("double") - F.lit(1.0)) / F.lit(2.0),
        )
        .withColumn("_rank_min_s", F.rank().over(w_ord_s))
        .withColumn("_tie_cnt_s", F.count(F.lit(1)).over(w_tie_s))
        .withColumn(
            "_rank_s",
            F.col("_rank_min_s").cast("double")
            + (F.col("_tie_cnt_s").cast("double") - F.lit(1.0)) / F.lit(2.0),
        )
    )

    # Match pandas ddof mixing: covar_samp (n-1) / (stddev_pop * stddev_pop)
    cov_expr = F.covar_samp(F.col("_rank_p"), F.col("_rank_s"))
    std_p_expr = F.stddev_pop(F.col("_rank_p"))
    std_s_expr = F.stddev_pop(F.col("_rank_s"))

    if metric.add_epsilon:
        denom = std_p_expr * std_s_expr + F.lit(EPSILON)
    else:
        denom = std_p_expr * std_s_expr

    spearman_expr = _divide(cov_expr, denom)

    return sdf2.groupBy(*group_by_cols).agg(
        spearman_expr.alias(metric.output_field_name)
    )


def _compute_max_value_timedelta_metric(
    sdf: DataFrame,
    group_by_cols: List[str],
    metric: MetricsBasemodel,
) -> DataFrame | None:
    """Compute MaxValueTimeDelta from first max-value occurrence timestamps.

    pandas path uses idxmax() for each series, which returns the first index
    of the maximum value. This implementation mirrors that by selecting the
    first row in each group ordered by value desc, then value_time asc.
    """
    from pyspark.sql import Window as W

    p_col = _field_name(getattr(metric, "primary_field_name", None), "primary_value")
    s_col = _field_name(getattr(metric, "secondary_field_name", None), "secondary_value")
    t_col = _field_name(getattr(metric, "value_time_field_name", None), "value_time")
    validate_fields_exist(sdf.columns, group_by_cols + [p_col, s_col, t_col])

    p = F.col(p_col).cast("double")
    s = F.col(s_col).cast("double")
    t = F.col(t_col)
    valid = p.isNotNull() & s.isNotNull() & t.isNotNull()

    # Tie-break by original row position to align with idxmax first-occurrence behavior.
    ranked = sdf.where(valid).withColumn("_row_id", F.monotonically_increasing_id())

    w_p = W.partitionBy(*group_by_cols).orderBy(
        p.desc(),
        F.col("_row_id").asc(),
    )
    w_s = W.partitionBy(*group_by_cols).orderBy(
        s.desc(),
        F.col("_row_id").asc(),
    )

    p_max = (
        ranked.withColumn("_rn_p", F.row_number().over(w_p))
        .where(F.col("_rn_p") == 1)
        .select(*group_by_cols, t.alias("_p_max_time"))
    )
    s_max = (
        ranked.withColumn("_rn_s", F.row_number().over(w_s))
        .where(F.col("_rn_s") == 1)
        .select(*group_by_cols, t.alias("_s_max_time"))
    )

    return null_safe_join_on_columns(
        p_max,
        s_max,
        join_columns=group_by_cols,
        how="inner",
        left_alias="p",
        right_alias="s",
    ).select(
        *group_by_cols,
        (F.col("_s_max_time").cast("long") - F.col("_p_max_time").cast("long"))
        .cast("double")
        .alias(metric.output_field_name),
    )


def _compute_annual_peak_relative_bias_metric(
    sdf: DataFrame,
    group_by_cols: List[str],
    metric: MetricsBasemodel,
) -> DataFrame | None:
    """Compute AnnualPeakRelativeBias from yearly primary/secondary maxima.

    Mirrors pandas logic:
    1) group by year and take max(primary), max(secondary)
    2) sum yearly maxima across years
    3) (sum_secondary_peaks - sum_primary_peaks) / sum_primary_peaks
    """
    p_col = _field_name(getattr(metric, "primary_field_name", None), "primary_value")
    s_col = _field_name(getattr(metric, "secondary_field_name", None), "secondary_value")
    t_col = _field_name(getattr(metric, "value_time_field_name", None), "value_time")
    validate_fields_exist(sdf.columns, group_by_cols + [p_col, s_col, t_col])

    p = F.col(p_col).cast("double")
    s = F.col(s_col).cast("double")
    t = F.col(t_col)
    valid = p.isNotNull() & s.isNotNull() & t.isNotNull()

    yearly_peaks = (
        sdf.where(valid)
        .withColumn("_year", F.year(t))
        .groupBy(*group_by_cols, "_year")
        .agg(
            F.max(p).alias("_p_year_max"),
            F.max(s).alias("_s_year_max"),
        )
    )

    sums = yearly_peaks.groupBy(*group_by_cols).agg(
        F.sum(F.col("_p_year_max")).alias("_sum_p_peaks"),
        F.sum(F.col("_s_year_max")).alias("_sum_s_peaks"),
    )

    if metric.add_epsilon:
        expr = _divide(
            F.col("_sum_s_peaks") - F.col("_sum_p_peaks"),
            F.col("_sum_p_peaks") + F.lit(EPSILON),
        )
    else:
        expr = _divide(
            F.col("_sum_s_peaks") - F.col("_sum_p_peaks"),
            F.col("_sum_p_peaks"),
        )

    return sums.select(*group_by_cols, expr.alias(metric.output_field_name))


def _apply_signature_batch(
    sdf: DataFrame,
    group_by_cols: List[str],
    metrics: List[MetricsBasemodel],
) -> DataFrame | None:
    return _compute_signature_metrics(sdf, group_by_cols, metrics)


def _apply_deterministic_batch(
    sdf: DataFrame,
    group_by_cols: List[str],
    metrics: List[MetricsBasemodel],
) -> DataFrame | None:
    return _compute_deterministic_metrics(sdf, group_by_cols, metrics)


SPARK_METRIC_ADAPTERS: tuple[MetricExecutionAdapter, ...] = (
    single_metric_adapter(
        name="spark-fdc-slope",
        supports=_is_fdc_slope_metric,
        apply_metric=_compute_fdc_slope_metric,
    ),
    single_metric_adapter(
        name="spark-center-of-timing",
        supports=_is_center_of_timing_metric,
        apply_metric=_compute_center_of_timing_metric,
    ),
    single_metric_adapter(
        name="spark-standard-deviation-of-timing",
        supports=_is_standard_deviation_of_timing_metric,
        apply_metric=_compute_standard_deviation_of_timing_metric,
    ),
    single_metric_adapter(
        name="spark-max-value-timedelta",
        supports=_is_timedelta_metric,
        apply_metric=_compute_max_value_timedelta_metric,
    ),
    single_metric_adapter(
        name="spark-spearman",
        supports=_is_spearman_metric,
        apply_metric=_compute_spearman_metric,
    ),
    single_metric_adapter(
        name="spark-annual-peak-relative-bias",
        supports=_is_annual_peak_metric,
        apply_metric=_compute_annual_peak_relative_bias_metric,
    ),
    contiguous_batch_adapter(
        name="spark-signature-batch",
        supports=_is_signature_metric,
        apply_batch=_apply_signature_batch,
    ),
    contiguous_batch_adapter(
        name="spark-deterministic-batch",
        supports=_is_deterministic_metric,
        apply_batch=_apply_deterministic_batch,
    ),
)


def _compute_spark_native_with_adapters(
    sdf: DataFrame,
    group_by_cols: List[str],
    metrics: List[MetricsBasemodel],
) -> DataFrame:
    # Preserve previous deterministic behavior: all deterministic metrics in one
    # spark-native query must reference the same primary/secondary field pair.
    deterministic = [m for m in metrics if _is_deterministic_metric(m)]
    if deterministic:
        _deterministic_fields(deterministic)

    metric_frames: List[DataFrame] = []
    idx = 0
    while idx < len(metrics):
        metric = metrics[idx]
        adapter = next((a for a in SPARK_METRIC_ADAPTERS if a.supports(metric)), None)
        if adapter is None:
            raise ValueError(
                "No Spark metric adapter found for metric: "
                f"{metric.__class__.__name__}."
            )

        batch, next_idx = adapter.consume_batch(metrics, idx)
        frame = adapter.apply_batch(sdf, group_by_cols, batch)
        if frame is not None:
            metric_frames.append(frame)
        idx = next_idx

    if not metric_frames:
        raise ValueError("No spark-native metrics were provided.")

    result_df = metric_frames[0]
    for frame in metric_frames[1:]:
        result_df = null_safe_join_on_columns(
            result_df,
            frame,
            join_columns=group_by_cols,
            how="outer",
        )

    ordered_cols = list(dict.fromkeys(group_by_cols + [m.output_field_name for m in metrics]))
    return result_df.select(*ordered_cols)


def compute_spark_native_metrics(
    sdf: DataFrame,
    group_by,
    metrics: List[MetricsBasemodel],
) -> DataFrame:
    """Compute supported metrics with Spark-native aggregations only."""
    group_by_cols = parse_fields_to_list(group_by)
    if not isinstance(metrics, list):
        metrics = [metrics]
    return _compute_spark_native_with_adapters(sdf, group_by_cols, metrics)


def _ordered_output_columns(
    df: DataFrame,
    group_by_cols: List[str],
    metrics: List[MetricsBasemodel],
) -> List[str]:
    """Build a stable output column list for mixed engine results.

    Bootstrap metrics with unpacked quantiles may not retain the base
    ``output_field_name`` column. In those cases, include available quantile
    columns instead.
    """
    available = set(df.columns)
    ordered_cols: List[str] = list(group_by_cols)

    for metric in metrics:
        base = metric.output_field_name
        if base in available:
            ordered_cols.append(base)
            continue

        # Unpacked bootstrap columns (e.g., metric_0_5 or metric_0.5)
        quantiles = getattr(getattr(metric, "bootstrap", None), "quantiles", None)
        if getattr(metric, "unpack_results", False) and quantiles is not None:
            for q in quantiles:
                candidates = [
                    f"{base}_{q}",
                    f"{base}_{str(q).replace('.', '_')}",
                ]
                for col in candidates:
                    if col in available and col not in ordered_cols:
                        ordered_cols.append(col)
            continue

        # Last-resort fallback for pre-expanded columns.
        prefix = f"{base}_"
        matches = sorted(c for c in df.columns if c.startswith(prefix))
        for col in matches:
            if col not in ordered_cols:
                ordered_cols.append(col)

    return ordered_cols
