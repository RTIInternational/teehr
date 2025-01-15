"""Test evaluation class."""
import time

from teehr import Configuration
from teehr import DeterministicMetrics, ProbabilisticMetrics, SignatureMetrics
from teehr import Operators as ops
import tempfile
import shutil
import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np
from arch.bootstrap import CircularBlockBootstrap, StationaryBootstrap

from teehr.models.filters import JoinedTimeseriesFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap
from teehr.evaluation.evaluation import Evaluation

from setup_v0_3_study import setup_v0_3_study
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


def test_executing_deterministic_metrics(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

    # Test all the metrics.
    include_all_metrics = [
        func() for func in DeterministicMetrics.__dict__.values() if callable(func)
    ]

    # Get the currently available fields to use in the query.
    flds = ev.joined_timeseries.field_enum()

    metrics_df = ev.metrics.query(
        include_metrics=include_all_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert metrics_df.columns.size == 20


def test_executing_signature_metrics(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

    # Test all the metrics.
    include_all_metrics = [
        func() for func in SignatureMetrics.__dict__.values() if callable(func)
    ]

    # Get the currently available fields to use in the query.
    flds = ev.joined_timeseries.field_enum()

    metrics_df = ev.metrics.query(
        include_metrics=include_all_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert metrics_df.columns.size == 8


def test_metrics_filter_and_geometry(tmpdir):
    """Test get_metrics method with filter and geometry."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # Define some metrics.
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    primary_avg = SignatureMetrics.Average()
    mvtd = DeterministicMetrics.MaxValueTimeDelta()
    pmvt = SignatureMetrics.MaxValueTime()

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
    kge = DeterministicMetrics.KlingGuptaEfficiency(bootstrap=boot)
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
    kge = DeterministicMetrics.KlingGuptaEfficiency(bootstrap=boot)
    # kge.unpack_results = True

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
    manual_results = np.sort(results.ravel()).astype(np.float32)

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
    kge = DeterministicMetrics.KlingGuptaEfficiency(bootstrap=boot)

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
    manual_results = np.sort(results.ravel()).astype(np.float32)

    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 2


def test_gumboot_bootstrapping(tmpdir):
    """Test get_metrics method gumboot bootstrapping."""
    # Manually create an evaluation using timseries from the R
    # Gumboot package vignette.
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    joined_timeseries_filepath = Path(
        "tests",
        "data",
        "test_study",
        "timeseries",
        "flows_1030500.parquet"
    )
    # Copy in joined timeseries file.
    shutil.copy(
        joined_timeseries_filepath,
        Path(eval.joined_timeseries.dir, joined_timeseries_filepath.name)
    )
    # Copy in the locations file.
    test_study_data_dir = Path("tests", "data", "v0_3_test_study")
    shutil.copy(
        Path(test_study_data_dir, "geo", "gages.parquet"),
        Path(eval.locations.dir, "gages.parquet")
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
    kge = DeterministicMetrics.KlingGuptaEfficiency(bootstrap=boot)
    nse = DeterministicMetrics.NashSutcliffeEfficiency(bootstrap=boot)

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

    metrics_df = eval.metrics.query(
        include_metrics=[kge, nse],
        filters=filters,
        group_by=[flds.primary_location_id]
    ).to_pandas()

    _ = eval.metrics.query(
        include_metrics=[kge, nse],
        filters=filters,
        group_by=[flds.primary_location_id]
    ).to_sdf()

    # Unpack and compare the results.
    teehr_results = np.sort(np.array(metrics_df.kling_gupta_efficiency.values[0]))
    manual_results = np.sort(results.ravel()).astype(np.float32)
    assert (teehr_results == manual_results).all()
    assert isinstance(metrics_df, pd.DataFrame)

    # Also compare to R benchmark results.
    r_df = pd.read_csv(R_BENCHMARK_RESULTS)
    r_kge_vals = np.sort(r_df.KGE.values)
    assert np.allclose(teehr_results, r_kge_vals, rtol=1e-06)


def test_metric_chaining(tmpdir):
    """Test get_metrics method with chaining."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # Test chaining.
    metrics_df = eval.metrics.query(
        order_by=["primary_location_id", "month"],
        group_by=["primary_location_id", "month"],
        include_metrics=[
            DeterministicMetrics.KlingGuptaEfficiency(),
            DeterministicMetrics.NashSutcliffeEfficiency(),
            DeterministicMetrics.RelativeBias()
        ]
    ).query(
        order_by=["primary_location_id"],
        group_by=["primary_location_id"],
        include_metrics=[
            SignatureMetrics.Average(
                input_field_names="relative_bias",
                output_field_name="primary_average"
            )
        ]
    ).to_pandas()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert all(
        metrics_df.columns == ["primary_location_id", "primary_average"]
    )


def test_ensemble_metrics(tmpdir):
    """Test get_metrics method with ensemble metrics."""
    usgs_location = Path(
        TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet"
    )

    secondary_filename = "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
    secondary_filepath = Path(
        TEST_STUDY_DATA_DIR_v0_4,
        "timeseries",
        secondary_filename
    )
    primary_filepath = Path(
        TEST_STUDY_DATA_DIR_v0_4,
        "timeseries",
        "usgs_hefs_06711565.parquet"
    )

    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    ev.locations.load_spatial(
        in_path=usgs_location
    )
    ev.location_crosswalks.load_csv(
        in_path=Path(TEST_STUDY_DATA_DIR_v0_4, "geo", "hefs_usgs_crosswalk.csv")
    )
    ev.configurations.add(
        Configuration(
            name="MEFP",
            type="primary",
            description="MBRFC HEFS Data"
        )
    )
    constant_field_values = {
        "unit_name": "ft^3/s",
        "variable_name": "streamflow_hourly_inst",
    }
    ev.secondary_timeseries.load_fews_xml(
        in_path=secondary_filepath,
        constant_field_values=constant_field_values
    )
    ev.primary_timeseries.load_parquet(
        in_path=primary_filepath
    )
    ev.joined_timeseries.create(execute_scripts=False)

    # Now, metrics.
    crps = ProbabilisticMetrics.CRPS()
    crps.summary_func = np.mean
    crps.estimator = "pwm"
    crps.backend = "numba"

    include_metrics = [crps]

    metrics_df = ev.metrics.query(
        include_metrics=include_metrics,
        group_by=[
            "primary_location_id",
            "reference_time",
            "configuration_name"
        ],
        order_by=["primary_location_id"],
    ).to_pandas()

    assert np.isclose(metrics_df.mean_crps_ensemble.values[0], 35.627174)


def test_persisting(tmpdir):
    """Test get_metrics method with chaining."""
    ev = Evaluation("s3a://ciroh-rti-public-data/teehr-data-warehouse/v0_4_evaluations/e1_camels_daily_streamflow")

    # metrics_ds = ev.metrics.query(
    #     order_by=["primary_location_id"],
    #     group_by=["primary_location_id"],
    #     include_metrics=[
    #         DeterministicMetrics.KlingGuptaEfficiency(),
    #     ]
    # )

    # t0 = time.time()
    # df1 = ev.joined_timeseries.query("primary_location_id = 'usgs-01013500'").to_pandas()
    # print(f"First query: {time.time() - t0}")

    # # usgs-14316700

    # t0 = time.time()
    # df2 = ev.joined_timeseries.query("primary_location_id = 'usgs-14316700'").to_pandas()
    # print(f"Second query: {time.time() - t0}")

    # t0 = time.time()
    # jt_tbl = ev.joined_timeseries
    # df1 = jt_tbl.query("primary_location_id = 'usgs-01013500'").to_pandas()
    # print(f"First query: {time.time() - t0}")

    # # Note: The second query here is 2x faster than the second query above.
    # t0 = time.time()
    # df2 = jt_tbl.query("primary_location_id = 'usgs-14316700'").to_pandas()
    # print(f"Second query: {time.time() - t0}")

    # pass

    # t0 = time.time()
    # locs = ev.locations.to_geopandas()
    # print(f"Time to locations: {time.time() - t0}")

    # import hvplot

    # t0 = time.time()
    # plot1 = metrics_ds.plot_metrics(x_column="primary_location_id", y_column="kling_gupta_efficiency")
    # print(f"Time to first plot: {time.time() - t0}")

    # t0 = time.time()
    # plot2 = metrics_ds.plot_bar_chart(x_column="primary_location_id", y_column="kling_gupta_efficiency", legend=False)
    # print(f"Time to second plot: {time.time() - t0}")

    # Note: Subsequent queries here are slower than re-running the queries
    # from the metrics_ds object. (37, 13.29, 13.88) --> has to re-map the operation?
    # t0 = time.time()
    # metrics_df = ev.metrics.query(
    #     order_by=["primary_location_id"],
    #     group_by=["primary_location_id"],
    #     include_metrics=[
    #         DeterministicMetrics.KlingGuptaEfficiency(),
    #     ]
    # ).to_pandas()
    # print(f"Initial query: {time.time() - t0}")

    pass

    # t0 = time.time()
    # metrics_df = ev.metrics.query(
    #     order_by=["primary_location_id"],
    #     group_by=["primary_location_id"],
    #     include_metrics=[
    #         DeterministicMetrics.KlingGuptaEfficiency(),
    #     ]
    # ).to_pandas()
    # print(f"Second query: {time.time() - t0}")

    # t0 = time.time()
    # metrics_df = ev.metrics.query(
    #     order_by=["primary_location_id"],
    #     group_by=["primary_location_id"],
    #     include_metrics=[
    #         DeterministicMetrics.KlingGuptaEfficiency(),
    #     ]
    # ).to_pandas()
    # print(f"Third query: {time.time() - t0}")

    # Note: Subsequent queries here are much faster than
    # re-running the query from the evaluation. (37, 0.15, 0.11)
    # t0 = time.time()
    # first = metrics_ds.to_pandas()
    # print(f"Initial query: {time.time() - t0}")

    # t0 = time.time()
    # second = metrics_ds.to_pandas()
    # print(f"Second query: {time.time() - t0}")

    # t0 = time.time()
    # third = metrics_ds.to_pandas()
    # print(f"Third query: {time.time() - t0}")

    # pass

    # # Define the evaluation object.
    # ev = setup_v0_3_study(tmpdir)

    # # Test chaining.
    # metrics_obj = ev.metrics.query(
    #     order_by=["primary_location_id", "month"],
    #     group_by=["primary_location_id", "month"],
    #     include_metrics=[
    #         DeterministicMetrics.KlingGuptaEfficiency(),
    #         DeterministicMetrics.NashSutcliffeEfficiency(),
    #         DeterministicMetrics.RelativeBias()
    #     ]
    # ).query(
    #     order_by=["primary_location_id"],
    #     group_by=["primary_location_id"],
    #     include_metrics=[
    #         SignatureMetrics.Average(
    #             input_field_names="relative_bias",
    #             output_field_name="primary_average"
    #         )
    #     ]
    # ).persist()

    # pass

    # assert isinstance(metrics_df, pd.DataFrame)
    # assert metrics_df.index.size == 3
    # assert all(
    #     metrics_df.columns == ["primary_location_id", "primary_average"]
    # )


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        # test_executing_deterministic_metrics(
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
        # test_gumboot_bootstrapping(
        #     tempfile.mkdtemp(
        #         prefix="5-",
        #         dir=tempdir
        #     )
        # )
        # test_metric_chaining(
        #     tempfile.mkdtemp(
        #         prefix="6-",
        #         dir=tempdir
        #     )
        # )
        # test_gumboot_bootstrapping(
        #     tempfile.mkdtemp(
        #         prefix="7-",
        #         dir=tempdir
        #     )
        # )
        # test_metric_chaining(
        #     tempfile.mkdtemp(
        #         prefix="8-",
        #         dir=tempdir
        #     )
        # )
        # test_ensemble_metrics(
        #     tempfile.mkdtemp(
        #         prefix="9-",
        #         dir=tempdir
        #     )
        # )
        test_persisting(
            tempfile.mkdtemp(
                prefix="10-",
                dir=tempdir
            )
        )
