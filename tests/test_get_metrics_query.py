"""Test evaluation class."""
from teehr import Evaluation, Metrics
from teehr import Operators as ops
from pathlib import Path
import shutil
import tempfile
import pandas as pd
import geopandas as gpd
import numpy as np
from arch.bootstrap import CircularBlockBootstrap, StationaryBootstrap

from teehr.models.dataset.filters import JoinedTimeseriesFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap

TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
JOINED_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "timeseries",
    "test_joined_timeseries_part1.parquet"
)


def test_get_all_metrics(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    # Copy in joined timeseries file.
    shutil.copy(
        JOINED_TIMESERIES_FILEPATH,
        Path(eval.joined_timeseries_dir, JOINED_TIMESERIES_FILEPATH.name)
    )

    # Test all the metrics.
    include_all_metrics = [
        func() for func in Metrics.__dict__.values() if callable(func)
    ]

    # Get the currently available fields to use in the query.
    flds = eval.joined_timeseries.field_enum()

    metrics_df = eval.query.get_metrics(
        include_metrics=include_all_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
        include_geometry=False
    )

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 2
    assert metrics_df.columns.size == 33


def test_metrics_filter_and_geometry(tmpdir):
    """Test get_metrics method with filter and geometry."""
    # Define the evaluation object.
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    # Copy in joined timeseries file.
    shutil.copy(
        JOINED_TIMESERIES_FILEPATH,
        Path(eval.joined_timeseries_dir, JOINED_TIMESERIES_FILEPATH.name)
    )
    # Copy in the locations file.
    shutil.copy(
        Path(TEST_STUDY_DATA_DIR, "geo", "gages.parquet"),
        Path(eval.locations_dir, "gages.parquet")
    )

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

    metrics_df = eval.query.get_metrics(
        include_metrics=include_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
        filters=filters,
        include_geometry=True
    )

    assert isinstance(metrics_df, gpd.GeoDataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 6


def test_circularblock_bootstrapping(tmpdir):
    """Test get_metrics method circular block bootstrapping."""
    # Define the evaluation object.
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    # Copy in joined timeseries file.
    shutil.copy(
        JOINED_TIMESERIES_FILEPATH,
        Path(eval.joined_timeseries_dir, JOINED_TIMESERIES_FILEPATH.name)
    )
    # Copy in the locations file.
    shutil.copy(
        Path(TEST_STUDY_DATA_DIR, "geo", "gages.parquet"),
        Path(eval.locations_dir, "gages.parquet")
    )

    # Define a bootstrapper.
    boot = Bootstrappers.CircularBlock(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)

    # Manual bootstrapping.
    df = pd.read_parquet(JOINED_TIMESERIES_FILEPATH)
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

    metrics_df = eval.query.get_metrics(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id],
        include_geometry=False
    )

    # Unpack and compare the results.
    teehr_results = np.sort(np.array(metrics_df.KGE.values[0]))
    manual_results = np.sort(results.ravel())

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


def test_stationary_bootstrapping(tmpdir):
    """Test get_metrics method stationary bootstrapping."""
    # Define the evaluation object.
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    # Copy in joined timeseries file.
    shutil.copy(
        JOINED_TIMESERIES_FILEPATH,
        Path(eval.joined_timeseries_dir, JOINED_TIMESERIES_FILEPATH.name)
    )
    # Copy in the locations file.
    shutil.copy(
        Path(TEST_STUDY_DATA_DIR, "geo", "gages.parquet"),
        Path(eval.locations_dir, "gages.parquet")
    )

    # Define a bootstrapper.
    boot = Bootstrappers.Stationary(
        seed=40,
        block_size=100,
        quantiles=None,
        reps=500
    )
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)

    # Manual bootstrapping.
    df = pd.read_parquet(JOINED_TIMESERIES_FILEPATH)
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

    metrics_df = eval.query.get_metrics(
        include_metrics=[kge],
        filters=filters,
        group_by=[flds.primary_location_id],
        include_geometry=False
    )

    # Unpack and compare the results.
    teehr_results = np.sort(np.array(metrics_df.KGE.values[0]))
    manual_results = np.sort(results.ravel())

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


def test_gumboot_bootstrapping(tmpdir):
    """Test get_metrics method gumboot bootstrapping."""
    # Define the evaluation object.
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    # TODO:
    JOINED_TIMESERIES_FILEPATH = Path(
        "tests",
        "data",
        "test_study",
        "timeseries",
        "flows_1030500.parquet"
    )

    # Copy in joined timeseries file.
    shutil.copy(
        JOINED_TIMESERIES_FILEPATH,
        Path(eval.joined_timeseries_dir, JOINED_TIMESERIES_FILEPATH.name)
    )
    # Copy in the locations file.
    shutil.copy(
        Path(TEST_STUDY_DATA_DIR, "geo", "gages.parquet"),
        Path(eval.locations_dir, "gages.parquet")
    )

    quantiles = [0.05, 0.5, 0.95]

    # Define a bootstrapper.
    boot = Bootstrappers.Gumboot(
        seed=40,
        quantiles=quantiles,
        reps=500,
        boot_year_file="/home/sam/temp/boot_year_file_R.csv",
        min_days=100,
        min_years=10
    )
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)
    nse = Metrics.NashSutcliffeEfficiency(bootstrap=boot)

    # Manually calling Gumboot.
    df = pd.read_parquet(JOINED_TIMESERIES_FILEPATH)
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
        min_days=kge.bootstrap.min_days,
        min_years=kge.bootstrap.min_years,
        reps=kge.bootstrap.reps,
        water_year_month=kge.bootstrap.water_year_month,
        start_year=kge.bootstrap.start_year,
        end_year=kge.bootstrap.end_year,
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

    # # Unpack and compare the results.
    # teehr_results = np.sort(np.array(metrics_df.KGE.values[0]))
    # manual_results = np.sort(results.ravel())

    # assert (teehr_results == manual_results).all()
    # assert isinstance(metrics_df, pd.DataFrame)

    pass



if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        # test_get_all_metrics(
        #     tempfile.mkdtemp(
        #         prefix="1-",
        #         dir=tempdir
        #     )
        # )
        # test_metrics_filter_and_geometry(
        #     tempfile.mkdtemp(
        #         prefix="2-",
        #         dir=tempdir
        #     )
        # )
        # test_circularblock_bootstrapping(
        #     tempfile.mkdtemp(
        #         prefix="3-",
        #         dir=tempdir
        #     )
        # )
        # test_stationary_bootstrapping(
        #     tempfile.mkdtemp(
        #         prefix="4-",
        #         dir=tempdir
        #     )
        # )
        test_gumboot_bootstrapping(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
