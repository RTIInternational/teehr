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

from setup_v0_3_study import setup_v0_3_study


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
