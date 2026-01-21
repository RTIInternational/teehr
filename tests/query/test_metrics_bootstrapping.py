"""Test evaluation class."""
from teehr import DeterministicMetrics, Signatures
from teehr import Operators as ops
import tempfile
import pandas as pd
from pathlib import Path
import numpy as np
from arch.bootstrap import CircularBlockBootstrap, StationaryBootstrap
import pytest

from teehr.models.filters import JoinedTimeseriesFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap
from teehr.evaluation.evaluation import Evaluation
from teehr.evaluation.spark_session_utils import create_spark_session


TEST_STUDY_DATA_DIR_v0_4 = Path("tests", "data", "test_study")


BOOT_YEAR_FILE = Path(
    "tests",
    "data",
    "test_study",
    "bootstrap",
    "boot_year_file_R.csv"
)
R_BENCHMARK_RESULTS = Path(
    "tests",
    "data",
    "test_study",
    "bootstrap",
    "r_benchmark_results.csv"
)

SPARK_SESSION = create_spark_session()

@pytest.mark.read_only_test_warehouse
def test_bootstrapping_signatures(read_only_test_warehouse):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = read_only_test_warehouse

    # Get the currently available fields to use in the query.
    flds = ev.joined_timeseries.field_enum()

    fdc = Signatures.FlowDurationCurveSlope()
    fdc.bootstrap = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=[0.05, 0.5, 0.95],
        reps=50
    )
    fdc.unpack_results = True
    sig_metrics_df = ev.metrics.query(
        include_metrics=[fdc],
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    assert isinstance(sig_metrics_df, pd.DataFrame)
    assert sig_metrics_df.index.size == 3
    assert sig_metrics_df.columns.size == 4
    assert np.isclose(sig_metrics_df["flow_duration_curve_slope_0.5"].sum(), -172.21364)

@pytest.mark.read_only_test_warehouse
def test_unpacking_bootstrap_results(read_only_test_warehouse):
    """Test unpacking bootstrapping quantile results."""
    # Define the evaluation object.
    ev = read_only_test_warehouse

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
    flds = ev.joined_timeseries.field_enum()
    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]
    metrics_df = ev.metrics.query(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id],
    ).to_pandas()
    cols = metrics_df.columns
    benchmark_cols = [
        "primary_location_id",
        "kling_gupta_efficiency_0.95",
        "kling_gupta_efficiency_0.5",
        "kling_gupta_efficiency_0.05"
    ]

    assert (cols == benchmark_cols).all()

@pytest.mark.read_only_test_warehouse
def test_circularblock_bootstrapping(read_only_test_warehouse):
    """Test get_metrics method circular block bootstrapping."""
    # Define the evaluation object.
    ev = read_only_test_warehouse

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
    df = ev.joined_timeseries.to_pandas()
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
    flds = ev.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.metrics.query(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id],
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

@pytest.mark.read_only_test_warehouse
def test_stationary_bootstrapping(read_only_test_warehouse):
    """Test get_metrics method stationary bootstrapping."""
    # Define the evaluation object.
    ev = read_only_test_warehouse

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
    df = ev.joined_timeseries.to_pandas()
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
    flds = ev.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.metrics.query(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id]
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

@pytest.mark.skip("This is a write operation")
def test_gumboot_bootstrapping(tmpdir, spark_session):
    """Test get_metrics method gumboot bootstrapping."""
    # Manually create an evaluation using timseries from the R
    # Gumboot package vignette.
    ev = Evaluation(dir_path=tmpdir, spark=spark_session, create_dir=True)
    ev.clone_template()
    # Write the staged joined_timeseries data to the warehouse.
    joined_timeseries_filepath = Path(
        "tests",
        "data",
        "test_study",
        "timeseries",
        "flows_1030500.parquet"
    )
    sdf = ev.spark.read.parquet(joined_timeseries_filepath.as_posix())
    ev.write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace"
    )
    # Write the staged locations data to the warehouse.
    test_study_data_dir = Path("tests", "data", "v0_3_test_study")
    sdf = ev.spark.read.parquet(
        Path(test_study_data_dir, "geo", "gages.parquet").as_posix()
    )
    ev.write.to_warehouse(
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
    df = ev.joined_timeseries.to_pandas()
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
    flds = ev.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.metrics.query(
        include_metrics=[kge, nse],
        filters=filters,
        group_by=[flds.primary_location_id]
    ).to_pandas()

    _ = ev.metrics.query(
        include_metrics=[kge, nse],
        filters=filters,
        group_by=[flds.primary_location_id]
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

@pytest.mark.read_only_test_warehouse
def test_bootstrapping_transforms(read_only_test_warehouse):
    """Test applying metric transforms (bootstrap)."""
    # Define the evaluation object.
    ev = read_only_test_warehouse

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
    df = ev.joined_timeseries.to_pandas()
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
    flds = ev.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.metrics.query(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id],
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

@pytest.mark.read_only_test_warehouse
def test_bootstrapping_fdc_slope_signature(read_only_test_warehouse):
    """Test bootstrapping FDC slope signature."""
    # Define the evaluation object.
    ev = read_only_test_warehouse

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
    flds = ev.joined_timeseries.field_enum()
    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]
    metrics_df = ev.metrics.query(
        include_metrics=[fdc],
        filters=filters,
        group_by=[flds.primary_location_id],
    ).to_pandas()

    cols = metrics_df.columns
    benchmark_cols = [
        "primary_location_id",
        "flow_duration_curve_slope_0.95",
        "flow_duration_curve_slope_0.5",
        "flow_duration_curve_slope_0.05"
    ]

    assert (sorted(cols) == sorted(benchmark_cols))