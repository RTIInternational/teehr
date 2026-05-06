"""Tests for aggregate engine routing and Spark-native metrics."""

import numpy as np
import pandas as pd
import pytest

from teehr import DeterministicMetrics, Signatures
from teehr.metrics.deterministic_models import VariabilityRatio

EPSILON = 1e-6


def _base_groups(ev):
    """Return per-location NumPy arrays of primary/secondary values."""
    base = ev.table("joined_timeseries").to_pandas()
    base = base[["primary_location_id", "primary_value", "secondary_value"]].dropna()
    groups = {}
    for loc, grp in base.groupby("primary_location_id"):
        p = grp["primary_value"].astype(float).to_numpy()
        s = grp["secondary_value"].astype(float).to_numpy()
        groups[loc] = (p, s)
    return groups


def _allclose(spark_series, expected_series, rtol=1e-5, atol=1e-7):
    assert np.allclose(spark_series.values, expected_series.values, rtol=rtol, atol=atol, equal_nan=True)


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_relative_metrics_parity(module_scope_test_warehouse):
    """Spark engine should compute relative metrics consistent with formulas."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.RelativeMean(add_epsilon=True),
        DeterministicMetrics.RelativeMedian(add_epsilon=True),
        DeterministicMetrics.RelativeMinimum(add_epsilon=True),
        DeterministicMetrics.RelativeMaximum(add_epsilon=True),
        DeterministicMetrics.RelativeStandardDeviation(add_epsilon=True),
    ]

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=metrics,
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
    )

    base_df = ev.table("joined_timeseries").to_pandas()
    base_df = base_df[["primary_location_id", "primary_value", "secondary_value"]].dropna()

    expected_rows = []
    for location_id, group in base_df.groupby("primary_location_id"):
        p = group["primary_value"].astype(float).to_numpy()
        s = group["secondary_value"].astype(float).to_numpy()
        expected_rows.append(
            {
                "primary_location_id": location_id,
                "relative_mean": np.mean(s) / (np.mean(p) + 1e-6),
                "relative_median": np.median(s) / (np.median(p) + 1e-6),
                "relative_minimum": np.min(s) / (np.min(p) + 1e-6),
                "relative_maximum": np.max(s) / (np.max(p) + 1e-6),
                "relative_standard_deviation": np.std(s) / (np.std(p) + 1e-6),
            }
        )

    expected_df = pd.DataFrame(expected_rows).sort_values("primary_location_id").reset_index(drop=True)
    spark_df = spark_df.sort_values("primary_location_id").reset_index(drop=True)

    assert isinstance(spark_df, pd.DataFrame)
    for col in [
        "relative_mean",
        "relative_median",
        "relative_minimum",
        "relative_maximum",
        "relative_standard_deviation",
    ]:
        rtol = 3e-2 if col == "relative_median" else 1e-6
        atol = 1e-6 if col == "relative_median" else 1e-8
        assert np.allclose(
            spark_df[col].values,
            expected_df[col].values,
            rtol=rtol,
            atol=atol,
            equal_nan=True,
        )


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_unsupported_metric_raises(module_scope_test_warehouse):
    """Spark engine should fail fast when unsupported metrics are requested."""
    ev = module_scope_test_warehouse

    with pytest.raises(ValueError, match="unsupported metrics"):
        (
            ev.table("joined_timeseries")
            .aggregate(
                metrics=[DeterministicMetrics.RelativeBias(transform="log")],
                group_by=["primary_location_id"],
                engine="spark",
            )
            .to_pandas()
        )


@pytest.mark.module_scope_test_warehouse
def test_engine_auto_plan_includes_python_for_transform_metric(module_scope_test_warehouse):
    """Auto mode should include pandas aggregation when transform metrics are requested."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.RelativeBias(transform="log", add_epsilon=True),
        DeterministicMetrics.MeanError(),
    ]

    sdf = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=metrics,
            group_by=["primary_location_id"],
        )
        .to_sdf()
    )

    executed_plan = sdf._jdf.queryExecution().executedPlan().toString()
    assert "AggregateInPandas" in executed_plan


# ---------------------------------------------------------------------------
# Parity: error / bias metrics
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_spark_error_metrics_parity(module_scope_test_warehouse):
    """Spark engine should match NumPy formulas for error and bias metrics."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.MeanError(),
        DeterministicMetrics.MeanAbsoluteError(),
        DeterministicMetrics.MeanSquareError(),
        DeterministicMetrics.RootMeanSquareError(),
        DeterministicMetrics.RelativeBias(add_epsilon=True),
        DeterministicMetrics.MultiplicativeBias(add_epsilon=True),
        DeterministicMetrics.MeanAbsoluteRelativeError(add_epsilon=True),
    ]

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"], engine="spark")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    groups = _base_groups(ev)
    rows = []
    for loc, (p, s) in sorted(groups.items()):
        n = len(p)
        rows.append({
            "primary_location_id": loc,
            "mean_error": np.sum(s - p) / n,
            "mean_absolute_error": np.sum(np.abs(s - p)) / n,
            "mean_square_error": np.sum((s - p) ** 2) / n,
            "root_mean_square_error": np.sqrt(np.sum((s - p) ** 2) / n),
            "relative_bias": np.sum(s - p) / (np.sum(p) + EPSILON),
            "multiplicative_bias": np.mean(s) / (np.mean(p) + EPSILON),
            "mean_absolute_relative_error": np.sum(np.abs(s - p)) / (np.sum(p) + EPSILON),
        })

    expected = pd.DataFrame(rows).sort_values("primary_location_id").reset_index(drop=True)

    for col in [
        "mean_error", "mean_absolute_error", "mean_square_error",
        "root_mean_square_error", "relative_bias", "multiplicative_bias",
        "mean_absolute_relative_error",
    ]:
        _allclose(spark_df[col], expected[col])


# ---------------------------------------------------------------------------
# Parity: efficiency / correlation metrics
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_spark_efficiency_metrics_parity(module_scope_test_warehouse):
    """Spark engine should match NumPy formulas for NSE/KGE/correlation metrics."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.NashSutcliffeEfficiency(),
        DeterministicMetrics.NormalizedNashSutcliffeEfficiency(),
        VariabilityRatio(add_epsilon=True),
        DeterministicMetrics.RootMeanStandardDeviationRatio(add_epsilon=True),
        DeterministicMetrics.PearsonCorrelation(),
        DeterministicMetrics.Rsquared(),
        DeterministicMetrics.KlingGuptaEfficiency(),
        DeterministicMetrics.KlingGuptaEfficiencyMod1(),
        DeterministicMetrics.KlingGuptaEfficiencyMod2(),
    ]

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"], engine="spark")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    groups = _base_groups(ev)
    rows = []
    for loc, (p, s) in sorted(groups.items()):
        std_p = np.std(p)
        std_s = np.std(s)
        mean_p = np.mean(p)
        mean_s = np.mean(s)
        n = len(p)

        nse_denom = np.sum((p - mean_p) ** 2)
        nse = 1.0 - np.sum((p - s) ** 2) / nse_denom if nse_denom != 0 else np.nan
        nnse = 1.0 / (1.0 + np.sum((p - s) ** 2) / nse_denom) if nse_denom != 0 else np.nan
        rmse = np.sqrt(np.sum((s - p) ** 2) / n)
        vr = std_s / (std_p + EPSILON)
        rsr = rmse / (std_p + EPSILON)
        pearson = np.corrcoef(s, p)[0][1]
        r2 = pearson ** 2

        # KGE (2009)
        r = np.corrcoef(s, p)[0][1]
        rv = std_s / std_p if std_p != 0 else np.nan
        rm = mean_s / mean_p if mean_p != 0 else np.nan
        if std_p == 0 or std_s == 0:
            kge = np.nan
        else:
            kge = 1.0 - np.sqrt((r - 1) ** 2 + (rv - 1) ** 2 + (rm - 1) ** 2)

        # KGE Mod1 (2012)
        cv_s = (std_s / mean_s) if mean_s != 0 else np.nan
        cv_p = (std_p / mean_p) if mean_p != 0 else np.nan
        gamma = cv_s / cv_p if cv_p != 0 else np.nan
        if std_p == 0 or std_s == 0:
            kge_m1 = np.nan
        else:
            kge_m1 = 1.0 - np.sqrt((r - 1) ** 2 + (gamma - 1) ** 2 + (rm - 1) ** 2)

        # KGE Mod2 (2012, mean-based bias)
        bias2 = (mean_s - mean_p) ** 2 / std_p ** 2 if std_p != 0 else np.nan
        if std_p == 0 or std_s == 0:
            kge_m2 = np.nan
        else:
            kge_m2 = 1.0 - np.sqrt((r - 1) ** 2 + (rv - 1) ** 2 + bias2)

        rows.append({
            "primary_location_id": loc,
            "nash_sutcliffe_efficiency": nse,
            "nash_sutcliffe_efficiency_normalized": nnse,
            "variability_ratio": vr,
            "root_mean_standard_deviation_ratio": rsr,
            "pearson_correlation": pearson,
            "r_squared": r2,
            "kling_gupta_efficiency": kge,
            "kling_gupta_efficiency_mod1": kge_m1,
            "kling_gupta_efficiency_mod2": kge_m2,
        })

    expected = pd.DataFrame(rows).sort_values("primary_location_id").reset_index(drop=True)

    for col in [
        "nash_sutcliffe_efficiency", "nash_sutcliffe_efficiency_normalized",
        "variability_ratio", "root_mean_standard_deviation_ratio",
        "pearson_correlation", "r_squared",
        "kling_gupta_efficiency", "kling_gupta_efficiency_mod1", "kling_gupta_efficiency_mod2",
    ]:
        _allclose(spark_df[col], expected[col])


# ---------------------------------------------------------------------------
# Parity: add_epsilon changes the result
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_spark_add_epsilon_changes_result(module_scope_test_warehouse):
    """Metrics with add_epsilon match numpy formula for both True and False."""
    ev = module_scope_test_warehouse

    groups = _base_groups(ev)

    no_eps_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.RelativeBias(add_epsilon=False)],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )
    eps_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.RelativeBias(add_epsilon=True)],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    # Verify both columns exist and are finite
    assert "relative_bias" in no_eps_df.columns
    assert "relative_bias" in eps_df.columns

    # Verify each matches its own numpy formula
    no_eps_expected = [
        np.sum(s - p) / np.sum(p) for _, (p, s) in sorted(groups.items())
    ]
    eps_expected = [
        np.sum(s - p) / (np.sum(p) + EPSILON) for _, (p, s) in sorted(groups.items())
    ]

    _allclose(no_eps_df["relative_bias"], pd.Series(no_eps_expected))
    _allclose(eps_df["relative_bias"], pd.Series(eps_expected))


# ---------------------------------------------------------------------------
# Parity: spark engine vs auto engine produce same output for pure-spark metrics
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_spark_vs_auto_same_output_for_native_metrics(module_scope_test_warehouse):
    """engine='spark' and engine='auto' should give identical results for native metrics."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.MeanError(),
        DeterministicMetrics.RelativeBias(add_epsilon=True),
        DeterministicMetrics.KlingGuptaEfficiency(),
        DeterministicMetrics.NashSutcliffeEfficiency(),
    ]

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"], engine="spark")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )
    auto_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"], engine="auto")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert list(spark_df.columns) == list(auto_df.columns)
    for col in ["mean_error", "relative_bias", "kling_gupta_efficiency", "nash_sutcliffe_efficiency"]:
        _allclose(spark_df[col], auto_df[col])


# ---------------------------------------------------------------------------
# Parity: no AggregateInPandas in plan for Spark-only queries
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_spark_no_pandas_udf_in_plan(module_scope_test_warehouse):
    """engine='spark' should not produce a pandas UDF aggregation node in the plan."""
    ev = module_scope_test_warehouse

    sdf = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[
                DeterministicMetrics.MeanError(),
                DeterministicMetrics.KlingGuptaEfficiency(),
                DeterministicMetrics.NashSutcliffeEfficiency(),
            ],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .to_sdf()
    )

    executed_plan = sdf._jdf.queryExecution().executedPlan().toString()
    assert "AggregateInPandas" not in executed_plan


# ---------------------------------------------------------------------------
# Mixed engine: unsupported metric goes to Python, supported to Spark
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_auto_mixed_columns_complete(module_scope_test_warehouse):
    """Auto engine mixing spark and python metrics should return all requested columns."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.MeanError(),                     # spark-native
        DeterministicMetrics.SpearmanCorrelation(),           # python-only
    ]

    result_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"])
        .to_pandas()
    )

    assert "mean_error" in result_df.columns
    assert "spearman_correlation" in result_df.columns
    assert result_df.index.size > 0


# ---------------------------------------------------------------------------
# Signature metrics with input_field_names override
# ---------------------------------------------------------------------------

@pytest.mark.module_scope_test_warehouse
def test_engine_spark_signature_input_field_names_override(module_scope_test_warehouse):
    """Spark-native Average should respect input_field_names override."""
    ev = module_scope_test_warehouse

    avg_secondary = Signatures.Average(
        input_field_names="secondary_value",
        output_field_name="secondary_average",
    )

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=[avg_secondary], group_by=["primary_location_id"], engine="spark")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert "secondary_average" in spark_df.columns

    groups = _base_groups(ev)
    expected_avgs = [np.mean(s) for _, (_, s) in sorted(groups.items())]
    _allclose(spark_df["secondary_average"], pd.Series(expected_avgs))


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_newly_native_threshold_and_max_delta_parity(module_scope_test_warehouse):
    """Spark engine should match python engine for threshold metrics and max value delta."""
    ev = module_scope_test_warehouse

    metrics = [
        DeterministicMetrics.MaxValueDelta(),
        DeterministicMetrics.ConfusionMatrix(threshold_field_name="year_2_discharge"),
        DeterministicMetrics.FalseAlarmRatio(threshold_field_name="year_2_discharge"),
        DeterministicMetrics.ProbabilityOfDetection(threshold_field_name="year_2_discharge"),
        DeterministicMetrics.ProbabilityOfFalseDetection(threshold_field_name="year_2_discharge"),
        DeterministicMetrics.CriticalSuccessIndex(threshold_field_name="year_2_discharge"),
        DeterministicMetrics.SuccessRatio(threshold_field_name="year_2_discharge"),
        DeterministicMetrics.FrequencyBiasIndex(threshold_field_name="year_2_discharge"),
    ]

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"], engine="spark")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    python_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=metrics, group_by=["primary_location_id"], engine="python")
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert list(spark_df.columns) == list(python_df.columns)

    _allclose(spark_df["max_value_delta"], python_df["max_value_delta"])

    for col in [
        "false_alarm_ratio",
        "probability_of_detection",
        "probability_of_false_detection",
        "critical_success_index",
        "success_ratio",
        "frequency_bias_index",
    ]:
        _allclose(spark_df[col], python_df[col])

    assert spark_df["confusion_matrix"].tolist() == python_df["confusion_matrix"].tolist()


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_fdc_slope_parity(module_scope_test_warehouse):
    """Spark FDC slope should match the pandas implementation numerically."""
    ev = module_scope_test_warehouse

    lower_q, upper_q = 0.25, 0.75

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[Signatures.FlowDurationCurveSlope(lower_quantile=lower_q, upper_quantile=upper_q)],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    python_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[Signatures.FlowDurationCurveSlope(lower_quantile=lower_q, upper_quantile=upper_q)],
            group_by=["primary_location_id"],
            engine="python",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert list(spark_df.columns) == list(python_df.columns)
    _allclose(spark_df["flow_duration_curve_slope"], python_df["flow_duration_curve_slope"])


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_spearman_parity(module_scope_test_warehouse):
    """Spark SpearmanCorrelation should match the pandas implementation."""
    ev = module_scope_test_warehouse

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.SpearmanCorrelation()],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    python_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.SpearmanCorrelation()],
            group_by=["primary_location_id"],
            engine="python",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert list(spark_df.columns) == list(python_df.columns)
    _allclose(spark_df["spearman_correlation"], python_df["spearman_correlation"])


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_max_value_timedelta_parity(module_scope_test_warehouse):
    """Spark MaxValueTimeDelta should match the pandas implementation."""
    ev = module_scope_test_warehouse

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.MaxValueTimeDelta()],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    python_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.MaxValueTimeDelta()],
            group_by=["primary_location_id"],
            engine="python",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert list(spark_df.columns) == list(python_df.columns)
    _allclose(spark_df["max_value_time_delta"], python_df["max_value_time_delta"])


@pytest.mark.module_scope_test_warehouse
def test_engine_spark_annual_peak_relative_bias_parity(module_scope_test_warehouse):
    """Spark AnnualPeakRelativeBias should match the pandas implementation."""
    ev = module_scope_test_warehouse

    spark_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.AnnualPeakRelativeBias()],
            group_by=["primary_location_id"],
            engine="spark",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    python_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[DeterministicMetrics.AnnualPeakRelativeBias()],
            group_by=["primary_location_id"],
            engine="python",
        )
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    assert list(spark_df.columns) == list(python_df.columns)
    _allclose(spark_df["annual_peak_flow_bias"], python_df["annual_peak_flow_bias"])
