"""Test evaluation class."""
from teehr import Configuration
from teehr import DeterministicMetrics, ProbabilisticMetrics, Signatures
from teehr import Operators as ops
import tempfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np
from arch.bootstrap import CircularBlockBootstrap, StationaryBootstrap

from teehr.models.filters import JoinedTimeseriesFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap
from teehr.evaluation.evaluation import Evaluation
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm
# import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa

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



def test_bootstrapping_signatures(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

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

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert metrics_df.columns.size == 9
    assert len(metrics_df2) == 1
    assert metrics_df2.location_id.values[0] == "gage-A"
    assert np.isclose(metrics_df2["sum"].values[0], 31.3)
    ev.spark.stop()


def test_unpacking_bootstrap_results(tmpdir):
    """Test unpacking bootstrapping quantile results."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

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
    ev.spark.stop()


def test_circularblock_bootstrapping(tmpdir):
    """Test get_metrics method circular block bootstrapping."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

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
    ev.spark.stop()


def test_stationary_bootstrapping(tmpdir):
    """Test get_metrics method stationary bootstrapping."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

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
    ev.spark.stop()


def test_gumboot_bootstrapping(tmpdir):
    """Test get_metrics method gumboot bootstrapping."""
    # Manually create an evaluation using timseries from the R
    # Gumboot package vignette.
    ev = Evaluation(dir_path=tmpdir, create_dir=True)
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
    (
        sdf.writeTo(
            f"{ev.catalog_name}.{ev.namespace}.joined_timeseries"
        )
        .using("iceberg")
        .createOrReplace()
    )
    # Write the staged locations data to the warehouse.
    test_study_data_dir = Path("tests", "data", "v0_3_test_study")
    sdf = ev.spark.read.parquet(
        Path(test_study_data_dir, "geo", "gages.parquet").as_posix()
    )
    (
        sdf.writeTo(
            f"{ev.catalog_name}.{ev.namespace}.locations"
        )
        .using("iceberg")
        .createOrReplace()
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
    ev.spark.stop()


def test_bootstrapping_transforms(tmpdir):
    """Test applying metric transforms (bootstrap)."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

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
    ev.spark.stop()


def test_bootstrapping_fdc_slope_signature(tmpdir):
    """Test bootstrapping FDC slope signature."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

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

    assert (cols == benchmark_cols).all()
    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_unpacking_bootstrap_results(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_circularblock_bootstrapping(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_stationary_bootstrapping(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_gumboot_bootstrapping(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        # TODO: High memory usage?
        test_bootstrapping_transforms(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir)
        )
        # TODO: Test bootstrapping FDC slope signature
        test_bootstrapping_fdc_slope_signature(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir)
        )
