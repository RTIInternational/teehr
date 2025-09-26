"""Test evaluation class."""
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

from teehr.models.filters import JoinedTimeseriesFilter, TableFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap
from teehr.evaluation.evaluation import Evaluation
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm

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


def test_executing_deterministic_metrics(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

    # Test all the metrics.
    include_all_metrics = [
        func() for func in DeterministicMetrics.__dict__.values() if callable(func)  # noqa
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
    assert metrics_df.columns.size == 21


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
    assert metrics_df.columns.size == 9


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
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
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
        kge.func(kge),
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
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot

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
        kge.func(kge),
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
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
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
        kge.func(kge),
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
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4, "geo", "hefs_usgs_crosswalk.csv"
        )
    )
    ev.configurations.add(
        Configuration(
            name="MEFP",
            type="secondary",
            description="MBRFC HEFS Data"
        )
    )
    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            type="primary",
            description="USGS observed test data"
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

    # Calculate annual hourly normals from USGS observations.
    input_ts = TableFilter()
    input_ts.table_name = "primary_timeseries"

    ts_normals = sts.Normals()
    ts_normals.temporal_resolution = "hour_of_year"  # the default
    ts_normals.summary_statistic = "mean"           # the default

    ev.generate.signature_timeseries(
        method=ts_normals,
        input_table_filter=input_ts,
        start_datetime="2024-11-19 12:00:00",
        end_datetime="2024-11-21 13:00:00",
        timestep="1 hour",
        fillna=False
    ).write()

    # Add reference forecast based on climatology.
    ev.configurations.add(
        [
            Configuration(
                name="benchmark_forecast_hourly_normals",
                type="secondary",
                description="Reference forecast based on USGS climatology summarized by hour of year"  # noqa
            )
        ]
    )
    ref_fcst = bm.ReferenceForecast()
    ref_fcst.aggregate_reference_timeseries = True

    reference_ts = TableFilter()
    reference_ts.table_name = "primary_timeseries"
    reference_ts.filters = [
        "variable_name = 'streamflow_hour_of_year_mean'",
        "unit_name = 'ft^3/s'"
    ]

    template_ts = TableFilter()
    template_ts.table_name = "secondary_timeseries"
    template_ts.filters = [
        "variable_name = 'streamflow_hourly_inst'",
        "unit_name = 'ft^3/s'",
        "member = '1993'"
    ]
    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_table_filter=reference_ts,
        template_table_filter=template_ts,
        output_configuration_name="benchmark_forecast_hourly_normals"
    ).write(destination_table="secondary_timeseries")

    ev.joined_timeseries.create(execute_scripts=False)

    # Now, metrics.
    crps = ProbabilisticMetrics.CRPS()
    crps.summary_func = np.mean
    crps.estimator = "pwm"
    crps.backend = "numba"
    crps.reference_configuration = "benchmark_forecast_hourly_normals"

    include_metrics = [crps]
    metrics_df = ev.metrics.query(
        include_metrics=include_metrics,
        group_by=[
            "primary_location_id",
            "configuration_name"
        ],
        order_by=["primary_location_id"],
    ).to_pandas()

    assert np.isclose(metrics_df.mean_crps_ensemble.values[0], 35.555721)
    assert np.isclose(metrics_df.mean_crps_ensemble.values[1], 1.073679)
    assert np.isclose(
        metrics_df.mean_crps_ensemble_skill_score.values[0], -32.115792
    )
    assert np.isnan(metrics_df.mean_crps_ensemble_skill_score.values[1])


def test_metrics_transforms(tmpdir):
    """Test applying metric transforms (non-bootstrap)."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

    # define metric requiring p,s
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge_t = DeterministicMetrics.KlingGuptaEfficiency()
    kge_t.transform = 'log'

    # define metric requiring p,s,t
    mvtd = DeterministicMetrics.MaxValueTimeDelta()
    mvtd_t = DeterministicMetrics.MaxValueTimeDelta()
    mvtd_t.transform = 'log'

    # get metrics_df
    metrics_df_transformed = eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            kge_t,
            mvtd_t
        ]
    ).to_pandas()
    metrics_df = eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            kge,
            mvtd
        ]
    ).to_pandas()

    # get results for comparison
    result_kge = metrics_df.kling_gupta_efficiency.values[0]
    result_kge_t = metrics_df_transformed.kling_gupta_efficiency.values[0]
    result_mvtd = metrics_df.max_value_time_delta.values[0]
    result_mvtd_t = metrics_df_transformed.max_value_time_delta.values[0]

    # metrics_df_transformed is created, transforms are applied
    assert isinstance(metrics_df_transformed, pd.DataFrame)
    assert result_kge != result_kge_t
    assert result_mvtd == result_mvtd_t


def test_bootstrapping_transforms(tmpdir):
    """Test applying metric transforms (bootstrap)."""
    # Define the evaluation object.
    eval = setup_v0_3_study(tmpdir)

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
        kge.func(kge),
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


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_executing_deterministic_metrics(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_executing_signature_metrics(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_metrics_filter_and_geometry(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_unpacking_bootstrap_results(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_circularblock_bootstrapping(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
        test_stationary_bootstrapping(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir
            )
        )
        test_gumboot_bootstrapping(
            tempfile.mkdtemp(
                prefix="7-",
                dir=tempdir
            )
        )
        test_metric_chaining(
            tempfile.mkdtemp(
                prefix="8-",
                dir=tempdir
            )
        )
        test_ensemble_metrics(
            tempfile.mkdtemp(
                prefix="9-",
                dir=tempdir
            )
        )
        test_metrics_transforms(
            tempfile.mkdtemp(
                prefix="10-",
                dir=tempdir)
        )
        test_bootstrapping_transforms(
            tempfile.mkdtemp(
                prefix="11-",
                dir=tempdir)
        )
