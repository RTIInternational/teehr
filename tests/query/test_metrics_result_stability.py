"""Guards against metric values moving unintentionally.

Two complementary checks, because the rest of the suite cannot make either:

1. ``test_native_metrics_agree_across_engines`` -- every Spark-native-eligible
   metric computed both ways on the same rows, values compared. The existing
   engine tests check the Spark path against numpy formulas written inline in
   the test, which validates the arithmetic but carries none of the closures'
   guards (finite-pair masking, zero denominators, std==0), so it is blind to
   the edge-case divergence the two paths actually had: `inf` on one side and
   NULL on the other, NaN on one and NULL on the other.

2. ``test_metric_values_match_golden`` -- absolute values against a committed
   file. Everything else in the suite is *relational*: the vectorized kernels
   are checked against the pandas closures, and the closures are the reference.
   That structure cannot detect both paths moving together, which is what a
   change to a closure does.

Regenerate the golden file deliberately, never to make a red test green:

    TEEHR_UPDATE_GOLDEN=1 pytest tests/query/test_metrics_result_stability.py

and put the reason for every changed value in the changelog.
"""
import json
import math
import os
from pathlib import Path

import numpy as np
import pytest

from teehr import DeterministicMetrics as dm
from teehr import Signatures as sg
from teehr.metrics.engine import aggregate_metrics_with_engine
from teehr.metrics.spark_native import SUPPORTED_METRICS

GOLDEN = Path("tests", "data", "metric_regression", "golden_metrics.json")

#: No column is exempt. relative_median used to be: the Spark path took
#: percentile_approx, which is nearest-rank and so returned the lower of the
#: two middle values on an even-sized group where np.median interpolates. It
#: now takes the exact percentile, which interpolates too, so the engines
#: agree to the same tolerance as everything else. A relaxation reappearing
#: here means that regressed.
APPROX_QUANTILE_COLS = frozenset()
APPROX_RTOL = RTOL = 1e-6
#: The Python path declares FloatType, so its values are float32 while the
#: Spark path computes in double. That is ~6e-8 relative, not a divergence.


def _two_field_metrics():
    """One instance of every two-field metric both engines can compute.

    Metrics needing a threshold column or a value_time axis are excluded and
    listed in EXCLUDED below, each with the test that does cover it, so this
    stays honest as the native metric set grows.
    """
    return [
        dm.RelativeMean(), dm.RelativeMedian(), dm.RelativeMinimum(),
        dm.RelativeMaximum(), dm.RelativeStandardDeviation(),
        dm.RelativeBias(), dm.MultiplicativeBias(), dm.VariabilityRatio(),
        dm.NashSutcliffeEfficiency(),
        dm.NormalizedNashSutcliffeEfficiency(),
        dm.KlingGuptaEfficiency(), dm.KlingGuptaEfficiencyMod1(),
        dm.KlingGuptaEfficiencyMod2(),
        dm.PearsonCorrelation(), dm.Rsquared(),
        dm.MeanError(), dm.MeanAbsoluteError(), dm.MeanSquareError(),
        dm.RootMeanSquareError(), dm.RootMeanStandardDeviationRatio(),
        dm.MeanAbsoluteRelativeError(), dm.MaxValueDelta(),
        sg.Count(), sg.Average(), sg.Minimum(), sg.Maximum(), sg.Sum(),
        sg.Variance(),
    ]


#: Native metrics deliberately outside this file, and where they are covered.
EXCLUDED = {
    "ConfusionMatrix": "test_engine_spark_newly_native_threshold_and_max_delta_parity",
    "CriticalSuccessIndex": "same",
    "FalseAlarmRatio": "same",
    "FrequencyBiasIndex": "same",
    "ProbabilityOfDetection": "same",
    "ProbabilityOfFalseDetection": "same",
    "SuccessRatio": "same",
    "FlowDurationCurveSlope": "test_engine_spark_fdc_slope_parity",
    "SpearmanCorrelation": "test_engine_spark_spearman_parity",
    "MaxValueTimeDelta": "test_engine_spark_max_value_timedelta_parity",
    "AnnualPeakRelativeBias": "test_engine_spark_annual_peak_relative_bias_parity",
    "MaxValueTime": "value_time axis; test_metrics_aggregate",
    "CenterOfTiming": "value_time axis; test_metrics_aggregate",
    "StandardDeviationOfTiming": "value_time axis; test_metrics_aggregate",
}


def test_every_native_metric_is_covered_somewhere():
    """A new Spark-native metric cannot skip cross-engine comparison silently."""
    here = {type(m).__name__ for m in _two_field_metrics()}
    missing = SUPPORTED_METRICS - here - set(EXCLUDED)
    assert not missing, (
        f"Spark-native metrics with no cross-engine test: {sorted(missing)}. "
        "Add them to _two_field_metrics(), or to EXCLUDED naming the test "
        "that covers them."
    )


def _degenerate_sdf(spark):
    """Rows chosen to exercise the guards, not the arithmetic.

    flat      -- constant primary, so std == 0 (the KGE/NSE guard boundary)
    zero-min  -- a zero minimum and a zero-sum window (zero denominators)
    gappy     -- nulls in each series at different rows (pairwise masking)
    """
    rows = []
    for i in range(40):
        rows.append(("flat", 5.0, 5.0 + (i % 3)))
    for i in range(40):
        rows.append(("zero-min", float(i % 10), float(i % 7) + 1.0))
    for i in range(40):
        p = None if i % 11 == 0 else float(i % 13) + 0.5
        s = None if i % 7 == 0 else float(i % 5) + 0.25
        rows.append(("gappy", p, s))
    return spark.createDataFrame(
        rows,
        "primary_location_id string, primary_value double, "
        "secondary_value double",
    )


def _by_group(sdf, group_col="primary_location_id"):
    out = {}
    for row in sdf.collect():
        d = row.asDict()
        out[d.pop(group_col)] = d
    return out


def _compare(left, right, left_name, right_name, skip_cols=frozenset()):
    """Every column equal, NULL-vs-number included."""
    assert set(left) == set(right), "engines returned different groups"
    problems = []
    for group in sorted(left):
        for col in sorted(left[group]):
            if col in skip_cols:
                continue
            a, b = left[group][col], right[group][col]
            if a is None or b is None:
                if a is not None or b is not None:
                    problems.append(f"{col} [{group}]: {left_name}={a!r} {right_name}={b!r}")
                continue
            if isinstance(a, dict) or isinstance(b, dict):
                assert a == b, f"{col} [{group}]"
                continue
            a, b = float(a), float(b)
            if math.isnan(a) or math.isnan(b):
                if not (math.isnan(a) and math.isnan(b)):
                    problems.append(f"{col} [{group}]: {left_name}={a!r} {right_name}={b!r}")
                continue
            rtol = APPROX_RTOL if col in APPROX_QUANTILE_COLS else RTOL
            if not np.isclose(a, b, rtol=rtol, atol=1e-8):
                problems.append(
                    f"{col} [{group}]: {left_name}={a!r} {right_name}={b!r} "
                    f"(rtol {abs(a - b) / max(abs(a), abs(b)):.2e} > {rtol})"
                )
    assert not problems, "engine disagreement:\n  " + "\n  ".join(problems)


@pytest.mark.module_scope_test_warehouse
def test_native_metrics_agree_across_engines(module_scope_test_warehouse):
    """Realistic data: every native metric, both engines, all columns."""
    ev = module_scope_test_warehouse
    sdf = ev.table("joined_timeseries").to_sdf()
    metrics = _two_field_metrics()

    python = _by_group(aggregate_metrics_with_engine(
        sdf=sdf, group_by=["primary_location_id"], metrics=metrics,
        engine="python",
    ))
    spark = _by_group(aggregate_metrics_with_engine(
        sdf=sdf, group_by=["primary_location_id"], metrics=metrics,
        engine="spark",
    ))
    _compare(python, spark, "python", "spark")


@pytest.mark.module_scope_test_warehouse
def test_native_metrics_agree_across_engines_on_degenerate_data(
    module_scope_test_warehouse,
):
    """The guards, which the inline-formula tests cannot reach.

    Zero denominators, a constant series and pairwise-null rows are where the
    two paths diverged in production: numpy returned inf where try_divide
    returned NULL, and a pandas UDF's NaN became NULL where the Spark path
    wrote a literal NaN.
    """
    ev = module_scope_test_warehouse
    sdf = _degenerate_sdf(ev.spark)
    metrics = _two_field_metrics()

    python = _by_group(aggregate_metrics_with_engine(
        sdf=sdf, group_by=["primary_location_id"], metrics=metrics,
        engine="python",
    ))
    spark = _by_group(aggregate_metrics_with_engine(
        sdf=sdf, group_by=["primary_location_id"], metrics=metrics,
        engine="spark",
    ))
    _compare(python, spark, "python", "spark", skip_cols=APPROX_QUANTILE_COLS)

    # And the rule those degenerate rows are here to pin down.
    for group in python:
        for col, value in python[group].items():
            assert value is None or not (
                isinstance(value, float) and math.isinf(value)
            ), f"{col} [{group}] is inf; undefined results must be NULL"


@pytest.mark.module_scope_test_warehouse
def test_metric_values_match_golden(module_scope_test_warehouse):
    """Absolute values, both engines, against a committed file.

    Bootstrap columns are deliberately absent: their values are a function of
    arch's RNG stream, so a numpy or arch bump would turn a dependency upgrade
    into golden-file churn. The bootstrap has its own anchors -- equivalence
    with a hand-rolled arch run, and the Gumboot R benchmark.
    """
    ev = module_scope_test_warehouse
    sdf = ev.table("joined_timeseries").to_sdf()
    metrics = _two_field_metrics()

    actual = {}
    for engine in ("python", "spark"):
        by_group = _by_group(aggregate_metrics_with_engine(
            sdf=sdf, group_by=["primary_location_id"], metrics=metrics,
            engine=engine,
        ))
        actual[engine] = {
            group: {
                col: (
                    None if v is None
                    else "nan" if isinstance(v, float) and math.isnan(v)
                    else "inf" if isinstance(v, float) and math.isinf(v)
                    else float(v) if isinstance(v, (int, float))
                    else str(v)
                )
                for col, v in sorted(cols.items())
            }
            for group, cols in by_group.items()
        }

    if os.environ.get("TEEHR_UPDATE_GOLDEN"):
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(json.dumps(actual, indent=1, sort_keys=True) + "\n")
        pytest.skip(f"regenerated {GOLDEN}")

    assert GOLDEN.exists(), (
        f"{GOLDEN} is missing; regenerate with TEEHR_UPDATE_GOLDEN=1"
    )
    expected = json.loads(GOLDEN.read_text())

    problems = []
    for engine in sorted(expected):
        for group in sorted(expected[engine]):
            for col, want in expected[engine][group].items():
                got = actual[engine][group].get(col, "<missing>")
                if isinstance(want, float) and isinstance(got, float):
                    rtol = APPROX_RTOL if col in APPROX_QUANTILE_COLS else RTOL
                    if np.isclose(want, got, rtol=rtol, atol=1e-8):
                        continue
                elif want == got:
                    continue
                problems.append(f"{engine}/{group}/{col}: golden={want!r} got={got!r}")
    assert not problems, (
        "metric values moved; if intended, explain each in the changelog and "
        "regenerate with TEEHR_UPDATE_GOLDEN=1:\n  " + "\n  ".join(problems)
    )
