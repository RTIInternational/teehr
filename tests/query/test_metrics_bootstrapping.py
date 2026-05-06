"""Test evaluation class."""
from teehr import DeterministicMetrics, Signatures
from teehr import Operators as ops
import pandas as pd
from pathlib import Path
import numpy as np
from arch.bootstrap import CircularBlockBootstrap, StationaryBootstrap
import pyspark.sql.functions as F
import pytest

from teehr.models.filters import TableFilter
from teehr.metrics.models.bootstrap import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap


BOOT_YEAR_FILE = Path(
    "tests",
    "data",
    "test_warehouse_data",
    "bootstrap",
    "boot_year_file_R.csv"
)
R_BENCHMARK_RESULTS = Path(
    "tests",
    "data",
    "test_warehouse_data",
    "bootstrap",
    "r_benchmark_results.csv"
)


@pytest.mark.session_scope_test_warehouse
def test_bootstrapping_signatures(session_scope_test_warehouse):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = session_scope_test_warehouse

    # Get the currently available fields to use in the query.

    fdc = Signatures.FlowDurationCurveSlope()
    fdc.bootstrap = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=[0.05, 0.5, 0.95],
        reps=50
    )
    fdc.unpack_results = True
    sig_metrics_df = ev.table("joined_timeseries").aggregate(
        metrics=[fdc],
        group_by=["primary_location_id"],
    ).order_by(["primary_location_id"]).to_pandas()

    assert isinstance(sig_metrics_df, pd.DataFrame)
    assert sig_metrics_df.index.size == 3
    assert sig_metrics_df.columns.size == 4
    assert np.isclose(sig_metrics_df["flow_duration_curve_slope_0_5"].sum(), -172.21364)


@pytest.mark.session_scope_test_warehouse
def test_unpacking_bootstrap_results(session_scope_test_warehouse):
    """Test unpacking bootstrapping quantile results."""
    # Define the evaluation object.
    ev = session_scope_test_warehouse

    # Define a bootstrapper.
    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=[0.05, 0.5, 0.95],
        reps=500
    )
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    kge.unpack_results = True
    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A"
        )
    ]
    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[kge],
        group_by=["primary_location_id"],
    ).to_pandas()
    cols = metrics_df.columns
    benchmark_cols = [
        "primary_location_id",
        "kling_gupta_efficiency_0_95",
        "kling_gupta_efficiency_0_5",
        "kling_gupta_efficiency_0_05"
    ]

    assert sorted(cols) == sorted(benchmark_cols)


@pytest.mark.session_scope_test_warehouse
def test_circularblock_bootstrapping(session_scope_test_warehouse):
    """Test get_metrics method circular block bootstrapping."""
    # Define the evaluation object.
    ev = session_scope_test_warehouse

    # Define a bootstrapper.
    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    # kge.unpack_results = True

    # Manual bootstrapping.
    df = ev.table("joined_timeseries").to_pandas()
    df_gageA = df.groupby("primary_location_id").get_group("gage-A")

    p = df_gageA.primary_value
    s = df_gageA.secondary_value

    bs = CircularBlockBootstrap(
        kge.bootstrap.block_size,
        p,
        s,
        seed=kge.bootstrap.seed,
        random_state=kge.bootstrap.random_state
    )
    results = bs.apply(
        kge.func(kge),
        kge.bootstrap.reps,
    )

    # TEEHR bootstrapping.

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[kge],
        group_by=["primary_location_id"],
    ).to_pandas()

    # Unpack and compare the results.
    teehr_results = np.sort(
        np.array(metrics_df.kling_gupta_efficiency.values[0])
    )
    manual_results = np.sort(results.ravel()).astype(np.float32)

    assert (teehr_results == manual_results).all()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


@pytest.mark.session_scope_test_warehouse
def test_stationary_bootstrapping(session_scope_test_warehouse):
    """Test get_metrics method stationary bootstrapping."""
    # Define the evaluation object.
    ev = session_scope_test_warehouse

    # Define a bootstrapper.
    boot = Bootstrappers.Stationary(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot

    # Manual bootstrapping.
    df = ev.table("joined_timeseries").to_pandas()
    df_gageA = df.groupby("primary_location_id").get_group("gage-A")

    p = df_gageA.primary_value
    s = df_gageA.secondary_value

    bs = StationaryBootstrap(
        kge.bootstrap.block_size,
        p,
        s,
        seed=kge.bootstrap.seed,
        random_state=kge.bootstrap.random_state
    )
    results = bs.apply(
        kge.func(kge),
        kge.bootstrap.reps,
    )

    # TEEHR bootstrapping.

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[kge],
        group_by=["primary_location_id"]
    ).to_pandas()

    # Unpack and compare the results.
    teehr_results = np.sort(
        np.array(metrics_df.kling_gupta_efficiency.values[0])
    )
    manual_results = np.sort(results.ravel()).astype(np.float32)

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


@pytest.mark.function_scope_test_warehouse
def test_gumboot_bootstrapping(function_scope_test_warehouse):
    """Test get_metrics method gumboot bootstrapping."""
    # Manually create an evaluation using timseries from the R
    # Gumboot package vignette.
    ev = function_scope_test_warehouse
    # Write the staged joined_timeseries data to the warehouse.
    joined_timeseries_filepath = Path(
        "tests",
        "data",
        "test_warehouse_data",
        "timeseries",
        "flows_1030500.parquet"
    )
    sdf = ev.spark.read.parquet(joined_timeseries_filepath.as_posix())
    ev._write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace"
    )
    # Write the staged locations data to the warehouse.
    test_study_data_dir = Path("tests", "data", "test_warehouse_data")
    sdf = ev.spark.read.parquet(
        Path(test_study_data_dir, "geo", "gages.parquet").as_posix()
    )
    ev._write.to_warehouse(
        source_data=sdf,
        table_name="locations",
        write_mode="create_or_replace"
    )
    # quantiles = [0.05, 0.5, 0.95]
    quantiles = None

    # Define a bootstrapper.
    boot = Bootstrappers.Gumboot(
        seed=40,
        quantiles=quantiles,
        reps=500,
        boot_year_file=BOOT_YEAR_FILE
    )
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    nse = DeterministicMetrics.NashSutcliffeEfficiency(bootstrap=boot)

    # Manually calling Gumboot.
    df = ev.table("joined_timeseries").to_pandas()
    df_gageA = df.groupby("primary_location_id").get_group("gage-A")

    p = df_gageA.primary_value
    s = df_gageA.secondary_value
    vt = df_gageA.value_time

    bs = GumbootBootstrap(
        p,
        s,
        value_time=vt,
        seed=kge.bootstrap.seed,
        water_year_month=kge.bootstrap.water_year_month,
        boot_year_file=kge.bootstrap.boot_year_file
    )
    results = bs.apply(
        kge.func(kge),
        kge.bootstrap.reps,
    )

    # TEEHR Gumboot bootstrapping.

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[kge, nse],
        group_by=["primary_location_id"]
    ).to_pandas()

    _ = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[kge, nse],
        group_by=["primary_location_id"]
    ).to_sdf()

    # Unpack and compare the results.
    teehr_results = np.sort(
        np.array(metrics_df.kling_gupta_efficiency.values[0])
    )
    manual_results = np.sort(results.ravel()).astype(np.float32)
    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)

    # Also compare to R benchmark results.
    r_df = pd.read_csv(R_BENCHMARK_RESULTS)
    r_kge_vals = np.sort(r_df.KGE.values)
    assert np.allclose(teehr_results, r_kge_vals, rtol=1e-06)


@pytest.mark.session_scope_test_warehouse
def test_bootstrapping_transforms(session_scope_test_warehouse):
    """Test applying metric transforms (bootstrap)."""
    # Define the evaluation object.
    ev = session_scope_test_warehouse

    # Define a bootstrapper.
    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    kge.transform = 'log'

    # Manual bootstrapping.
    df = ev.table("joined_timeseries").to_pandas()
    df_gageA = df.groupby("primary_location_id").get_group("gage-A")

    p = df_gageA.primary_value
    s = df_gageA.secondary_value

    bs = CircularBlockBootstrap(
        kge.bootstrap.block_size,
        p,
        s,
        seed=kge.bootstrap.seed,
        random_state=kge.bootstrap.random_state
    )
    results = bs.apply(
        kge.func(kge),
        kge.bootstrap.reps,
    )

    # TEEHR bootstrapping.

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[kge],
        group_by=["primary_location_id"],
    ).to_pandas()

    # Unpack and compare the results.
    teehr_results = np.sort(
        np.array(metrics_df.kling_gupta_efficiency.values[0])
    )
    manual_results = np.sort(results.ravel()).astype(np.float32)

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


@pytest.mark.session_scope_test_warehouse
def test_bootstrapping_fdc_slope_signature(session_scope_test_warehouse):
    """Test bootstrapping FDC slope signature."""
    # Define the evaluation object.
    ev = session_scope_test_warehouse

    # Define a bootstrapper.
    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=[0.05, 0.5, 0.95],
        reps=500
    )
    fdc = Signatures.FlowDurationCurveSlope()
    fdc.bootstrap = boot
    fdc.unpack_results = True
    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A"
        )
    ]
    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[fdc],
        group_by=["primary_location_id"],
    ).to_pandas()

    cols = metrics_df.columns
    benchmark_cols = [
        "primary_location_id",
        "flow_duration_curve_slope_0_95",
        "flow_duration_curve_slope_0_5",
        "flow_duration_curve_slope_0_05"
    ]

    assert (sorted(cols) == sorted(benchmark_cols))


@pytest.mark.function_scope_test_warehouse
def test_circularblock_bootstrapping_threshold_metric(function_scope_test_warehouse):
    """Test CircularBlock bootstrapping for threshold-based deterministic metrics."""
    ev = function_scope_test_warehouse

    # Build a numeric threshold field for stable threshold metric evaluation.
    sdf = ev.table("joined_timeseries").to_sdf().withColumn(
        "threshold_numeric", F.lit(5.0)
    )
    ev._write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace",
    )

    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=200,
    )
    pod = DeterministicMetrics.ProbabilityOfDetection(
        threshold_field_name="threshold_numeric"
    )
    pod.bootstrap = boot

    df = ev.table("joined_timeseries").to_pandas()
    df_gage_a = df.groupby("primary_location_id").get_group("gage-A")
    p = df_gage_a.primary_value
    s = df_gage_a.secondary_value
    t = df_gage_a.threshold_numeric

    bs = CircularBlockBootstrap(
        pod.bootstrap.block_size,
        p,
        s,
        t,
        seed=pod.bootstrap.seed,
        random_state=pod.bootstrap.random_state,
    )
    results = bs.apply(
        pod.func(pod),
        pod.bootstrap.reps,
    )

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A",
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[pod],
        group_by=["primary_location_id"],
    ).to_pandas()

    teehr_results = np.sort(
        np.array(metrics_df.probability_of_detection.values[0], dtype=float)
    )
    manual_results = np.sort(results.ravel().astype(np.float32))

    assert np.allclose(teehr_results, manual_results, equal_nan=True)


@pytest.mark.function_scope_test_warehouse
def test_stationary_bootstrapping_threshold_metric(function_scope_test_warehouse):
    """Test Stationary bootstrapping for threshold-based deterministic metrics."""
    ev = function_scope_test_warehouse

    # Build a numeric threshold field for stable threshold metric evaluation.
    sdf = ev.table("joined_timeseries").to_sdf().withColumn(
        "threshold_numeric", F.lit(5.0)
    )
    ev._write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace",
    )

    boot = Bootstrappers.Stationary(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=200,
    )
    pod = DeterministicMetrics.ProbabilityOfDetection(
        threshold_field_name="threshold_numeric"
    )
    pod.bootstrap = boot

    df = ev.table("joined_timeseries").to_pandas()
    df_gage_a = df.groupby("primary_location_id").get_group("gage-A")
    p = df_gage_a.primary_value
    s = df_gage_a.secondary_value
    t = df_gage_a.threshold_numeric

    bs = StationaryBootstrap(
        pod.bootstrap.block_size,
        p,
        s,
        t,
        seed=pod.bootstrap.seed,
        random_state=pod.bootstrap.random_state,
    )
    results = bs.apply(
        pod.func(pod),
        pod.bootstrap.reps,
    )

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A",
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[pod],
        group_by=["primary_location_id"],
    ).to_pandas()

    teehr_results = np.sort(
        np.array(metrics_df.probability_of_detection.values[0], dtype=float)
    )
    manual_results = np.sort(results.ravel().astype(np.float32))

    assert np.allclose(teehr_results, manual_results, equal_nan=True)


@pytest.mark.function_scope_test_warehouse
def test_gumboot_bootstrapping_threshold_metric(function_scope_test_warehouse):
    """Test Gumboot bootstrapping for threshold-based deterministic metrics."""
    ev = function_scope_test_warehouse

    # Build a numeric threshold field for stable threshold metric evaluation.
    sdf = ev.table("joined_timeseries").to_sdf().withColumn(
        "threshold_numeric", F.lit(5.0)
    )
    ev._write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace",
    )

    boot = Bootstrappers.Gumboot(
        seed=40,
        quantiles=None,
        reps=200,
        boot_year_file=BOOT_YEAR_FILE,
    )
    pod = DeterministicMetrics.ProbabilityOfDetection(
        threshold_field_name="threshold_numeric"
    )
    pod.bootstrap = boot

    df = ev.table("joined_timeseries").to_pandas()
    df_gage_a = df.groupby("primary_location_id").get_group("gage-A")
    p = df_gage_a.primary_value
    s = df_gage_a.secondary_value
    t = df_gage_a.threshold_numeric
    vt = df_gage_a.value_time

    bs = GumbootBootstrap(
        p,
        s,
        t,
        value_time=vt,
        seed=pod.bootstrap.seed,
        water_year_month=pod.bootstrap.water_year_month,
        boot_year_file=pod.bootstrap.boot_year_file,
    )
    results = bs.apply(
        pod.func(pod),
        pod.bootstrap.reps,
    )

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A",
        )
    ]

    metrics_df = ev.table("joined_timeseries").filter(
        filters=filters,
    ).aggregate(
        metrics=[pod],
        group_by=["primary_location_id"],
    ).to_pandas()

    teehr_results = np.sort(
        np.array(metrics_df.probability_of_detection.values[0], dtype=float)
    )
    manual_results = np.sort(results.ravel().astype(np.float32))

    assert np.allclose(teehr_results, manual_results, equal_nan=True)


# ---------------------------------------------------------------------------
# Shared bootstrap sample tests
# ---------------------------------------------------------------------------

@pytest.mark.session_scope_test_warehouse
def test_shared_bootstrap_quantile_columns_correct(
    session_scope_test_warehouse,
):
    """Two metrics with identical bootstrap config use shared samples.

    Asserts correct column names, finite values, and monotone quantile ordering.
    """
    ev = session_scope_test_warehouse

    boot_cfg = dict(seed=42, block_size=100, quantiles=[0.05, 0.5, 0.95], reps=200)

    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = Bootstrappers.CircularBlock(**boot_cfg)
    kge.unpack_results = True

    nse = DeterministicMetrics.NashSutcliffeEfficiency()
    nse.bootstrap = Bootstrappers.CircularBlock(**boot_cfg)
    nse.unpack_results = True

    shared_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=[kge, nse], group_by=["primary_location_id"])
        .order_by("primary_location_id")
        .to_pandas()
        .sort_values("primary_location_id")
        .reset_index(drop=True)
    )

    expected_kge_cols = {
        "kling_gupta_efficiency_0_05",
        "kling_gupta_efficiency_0_5",
        "kling_gupta_efficiency_0_95",
    }
    expected_nse_cols = {
        "nash_sutcliffe_efficiency_0_05",
        "nash_sutcliffe_efficiency_0_5",
        "nash_sutcliffe_efficiency_0_95",
    }

    assert expected_kge_cols.issubset(set(shared_df.columns)), (
        f"Missing KGE quantile columns. Got: {list(shared_df.columns)}"
    )
    assert expected_nse_cols.issubset(set(shared_df.columns)), (
        f"Missing NSE quantile columns. Got: {list(shared_df.columns)}"
    )

    for col in expected_kge_cols | expected_nse_cols:
        assert np.isfinite(shared_df[col].values).all(), (
            f"Non-finite values in shared bootstrap column '{col}'"
        )

    assert (
        shared_df["kling_gupta_efficiency_0_05"].values
        <= shared_df["kling_gupta_efficiency_0_5"].values
    ).all()
    assert (
        shared_df["kling_gupta_efficiency_0_5"].values
        <= shared_df["kling_gupta_efficiency_0_95"].values
    ).all()


@pytest.mark.session_scope_test_warehouse
def test_shared_bootstrap_singleton_group_unchanged(
    session_scope_test_warehouse,
):
    """A single metric with quantile bootstrap should produce correct columns
    through the shared path (singleton group, no actual sharing).
    """
    ev = session_scope_test_warehouse

    boot = Bootstrappers.CircularBlock(
        seed=40, block_size=100, quantiles=[0.05, 0.5, 0.95], reps=200
    )
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    kge.unpack_results = True

    result_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=[kge], group_by=["primary_location_id"])
        .order_by("primary_location_id")
        .to_pandas()
    )

    expected_cols = {
        "primary_location_id",
        "kling_gupta_efficiency_0_05",
        "kling_gupta_efficiency_0_5",
        "kling_gupta_efficiency_0_95",
    }
    assert expected_cols.issubset(set(result_df.columns))
    assert result_df.index.size == 3


@pytest.mark.session_scope_test_warehouse
def test_shared_bootstrap_different_configs_not_shared(
    session_scope_test_warehouse,
):
    """Two metrics with DIFFERENT bootstrap configs fall into separate groups
    and should still produce correct output for both.
    """
    ev = session_scope_test_warehouse

    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = Bootstrappers.CircularBlock(
        seed=1, block_size=50, quantiles=[0.05, 0.95], reps=100
    )
    kge.unpack_results = True

    nse = DeterministicMetrics.NashSutcliffeEfficiency()
    nse.bootstrap = Bootstrappers.CircularBlock(
        seed=2, block_size=200, quantiles=[0.05, 0.95], reps=100
    )
    nse.unpack_results = True

    result_df = (
        ev.table("joined_timeseries")
        .aggregate(metrics=[kge, nse], group_by=["primary_location_id"])
        .to_pandas()
    )

    assert "kling_gupta_efficiency_0_05" in result_df.columns
    assert "kling_gupta_efficiency_0_95" in result_df.columns
    assert "nash_sutcliffe_efficiency_0_05" in result_df.columns
    assert "nash_sutcliffe_efficiency_0_95" in result_df.columns
    assert result_df.index.size == 3


@pytest.mark.session_scope_test_warehouse
def test_shared_bootstrap_mixed_with_non_bootstrap_metric(
    session_scope_test_warehouse,
):
    """Shared bootstrap metrics combined with a plain (non-bootstrap) metric
    should all appear in the result with correct columns.
    """
    ev = session_scope_test_warehouse

    boot = Bootstrappers.CircularBlock(
        seed=42, block_size=100, quantiles=[0.5], reps=100
    )

    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    kge.unpack_results = True

    nse = DeterministicMetrics.NashSutcliffeEfficiency()
    nse.bootstrap = boot
    nse.unpack_results = True

    me = DeterministicMetrics.MeanError()  # no bootstrap

    result_df = (
        ev.table("joined_timeseries")
        .aggregate(
            metrics=[kge, nse, me],
            group_by=["primary_location_id"],
        )
        .to_pandas()
    )

    assert "kling_gupta_efficiency_0_5" in result_df.columns
    assert "nash_sutcliffe_efficiency_0_5" in result_df.columns
    assert "mean_error" in result_df.columns
    assert result_df.index.size == 3


@pytest.mark.session_scope_test_warehouse
def test_shared_bootstrap_raw_arrays_no_quantiles(
    session_scope_test_warehouse,
):
    """Shared bootstrap path supports quantiles=None for multiple metrics."""
    ev = session_scope_test_warehouse

    boot_cfg = dict(seed=40, block_size=100, quantiles=None, reps=200)

    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = Bootstrappers.CircularBlock(**boot_cfg)

    nse = DeterministicMetrics.NashSutcliffeEfficiency()
    nse.bootstrap = Bootstrappers.CircularBlock(**boot_cfg)

    filters = [
        TableFilter(
            column="primary_location_id",
            operator=ops.eq,
            value="gage-A",
        )
    ]

    metrics_df = (
        ev.table("joined_timeseries")
        .filter(filters=filters)
        .aggregate(metrics=[kge, nse], group_by=["primary_location_id"])
        .to_pandas()
    )

    assert "kling_gupta_efficiency" in metrics_df.columns
    assert "nash_sutcliffe_efficiency" in metrics_df.columns

    teehr_kge = np.sort(np.asarray(metrics_df.kling_gupta_efficiency.values[0], dtype=float))
    teehr_nse = np.sort(np.asarray(metrics_df.nash_sutcliffe_efficiency.values[0], dtype=float))
    assert teehr_kge.shape[0] == boot_cfg["reps"]
    assert teehr_nse.shape[0] == boot_cfg["reps"]

    # Compare against manual bootstrapping with the same seed/config.
    df = ev.table("joined_timeseries").to_pandas()
    df_gage_a = df.groupby("primary_location_id").get_group("gage-A")
    p = df_gage_a.primary_value
    s = df_gage_a.secondary_value

    bs = CircularBlockBootstrap(
        boot_cfg["block_size"],
        p,
        s,
        seed=boot_cfg["seed"],
    )
    manual_kge = np.sort(np.asarray(bs.apply(kge.func(kge), boot_cfg["reps"]).ravel(), dtype=float))

    bs = CircularBlockBootstrap(
        boot_cfg["block_size"],
        p,
        s,
        seed=boot_cfg["seed"],
    )
    manual_nse = np.sort(np.asarray(bs.apply(nse.func(nse), boot_cfg["reps"]).ravel(), dtype=float))

    assert np.allclose(teehr_kge, manual_kge, equal_nan=True)
    assert np.allclose(teehr_nse, manual_nse, equal_nan=True)
