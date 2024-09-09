"""Test evaluation class."""
from teehr import Metrics
from teehr import Operators as ops
import tempfile
import pandas as pd
import geopandas as gpd
import numpy as np
from arch.bootstrap import CircularBlockBootstrap, StationaryBootstrap

from teehr.models.filters import JoinedTimeseriesFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap

from setup_v0_3_study import setup_v0_3_study


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


def test_get_all_metrics(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # Test all the metrics.
    include_all_metrics = [
        func() for func in Metrics.__dict__.values() if callable(func)
    ]

    # Get the currently available fields to use in the query.
    flds = eval.joined_timeseries.field_enum()

    metrics_df = eval.metrics.query(
        include_metrics=include_all_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert metrics_df.columns.size == 33


def test_metrics_filter_and_geometry(tmpdir):
    """Test get_metrics method with filter and geometry."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # Define some metrics.
    kge = Metrics.KlingGuptaEfficiency()
    primary_avg = Metrics.PrimaryAverage()
    mvtd = Metrics.MaxValueTimeDelta()
    pmvt = Metrics.PrimaryMaxValueTime()

    include_metrics = [pmvt, mvtd, primary_avg, kge]

    # Get the currently available fields to use in the query.
    flds = eval.joined_timeseries.field_enum()

    # Define some filters.
    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = eval.metrics.query(
        include_metrics=include_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
        filters=filters,
    ).to_geopandas()

    assert isinstance(metrics_df, gpd.GeoDataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 6


def test_circularblock_bootstrapping(tmpdir):
    """Test get_metrics method circular block bootstrapping."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # Define a bootstrapper.
    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)

    # Manual bootstrapping.
    df = eval.joined_timeseries.to_pandas()
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
        kge.func,
        kge.bootstrap.reps,
    )

    # TEEHR bootstrapping.
    flds = eval.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = eval.metrics.query(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id],
    ).to_pandas()

    # Unpack and compare the results.
    teehr_results = np.sort(
        np.array(metrics_df.kling_gupta_efficiency.values[0])
    )
    manual_results = np.sort(results.ravel())

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


def test_stationary_bootstrapping(tmpdir):
    """Test get_metrics method stationary bootstrapping."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # Define a bootstrapper.
    boot = Bootstrappers.Stationary(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)

    # Manual bootstrapping.
    df = eval.joined_timeseries.to_pandas()
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
        kge.func,
        kge.bootstrap.reps,
    )

    # TEEHR bootstrapping.
    flds = eval.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = eval.metrics.query(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id]
    ).to_pandas()

    # Unpack and compare the results.
    teehr_results = np.sort(
        np.array(metrics_df.kling_gupta_efficiency.values[0])
    )
    manual_results = np.sort(results.ravel())

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


def test_gumboot_bootstrapping(tmpdir):
    """Test get_metrics method gumboot bootstrapping."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # quantiles = [0.05, 0.5, 0.95]
    quantiles = None

    # Define a bootstrapper.
    boot = Bootstrappers.Gumboot(
        seed=40,
        quantiles=quantiles,
        reps=500,
        boot_year_file=BOOT_YEAR_FILE,
        min_days=100,
        min_years=10
    )
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)
    nse = Metrics.NashSutcliffeEfficiency(bootstrap=boot)

    # Manually calling Gumboot.
    df = eval.joined_timeseries.to_pandas()
    df_gageA = df.groupby("primary_location_id").get_group("gage-A")

    p = df_gageA.primary_value
    s = df_gageA.secondary_value
    vt = df_gageA.value_time

    bs = GumbootBootstrap(
        p,
        s,
        value_time=vt,
        seed=kge.bootstrap.seed,
        random_state=kge.bootstrap.random_state,
        water_year_month=kge.bootstrap.water_year_month,
        boot_year_file=kge.bootstrap.boot_year_file
    )
    results = bs.apply(
        kge.func,
        kge.bootstrap.reps,
    )

    # TEEHR Gumboot bootstrapping.
    flds = eval.joined_timeseries.field_enum()

    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = eval.query.get_metrics(
        include_metrics=[kge, nse],
        filters=filters,
        group_by=[flds.primary_location_id],
        include_geometry=False
    )

    # Unpack and compare the results.
    teehr_results = np.sort(np.array(metrics_df.kling_gupta_efficiency.values[0]))
    manual_results = np.sort(results.ravel())
    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)

    # Also compare to R benchmark results.
    r_df = pd.read_csv(R_BENCHMARK_RESULTS)
    r_kge_vals = np.sort(r_df.KGE.values)
    assert np.allclose(teehr_results, r_kge_vals, rtol=1e-08)

    pass



if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_get_all_metrics(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_metrics_filter_and_geometry(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_circularblock_bootstrapping(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_stationary_bootstrapping(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_gumboot_bootstrapping(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
