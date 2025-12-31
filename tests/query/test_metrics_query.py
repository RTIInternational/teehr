"""Test evaluation class."""
from teehr import Configuration
from teehr import DeterministicMetrics, ProbabilisticMetrics, Signatures
from teehr import Operators as ops
import tempfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np

from teehr.models.filters import JoinedTimeseriesFilter
from teehr.evaluation.evaluation import Evaluation
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm
from teehr import TimeseriesAwareCalculatedFields as tcf

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa
from data.setup_v0_5_ensemble_study import setup_v0_5_ensemble_study  # noqa


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

    # Test all the non-conditional metrics.
    include_nonconditional_metrics = [
        func() for func in DeterministicMetrics.__dict__.values()
        if callable(func) and not func().attrs.get('requires_threshold_field', False)  # noqa
    ]

    # Get the currently available fields to use in the query.
    flds = ev.joined_timeseries.field_enum()

    metrics_df = ev.metrics.query(
        include_metrics=include_nonconditional_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    metrics_df2 = ev.metrics(table_name="joined_timeseries").query(
        include_metrics=include_nonconditional_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    assert metrics_df.equals(metrics_df2)
    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert metrics_df.columns.size == 21

    # Test all the conditional metrics.
    include_conditional_metrics = [
        func(threshold_field_name="quantile_value")
        for func in DeterministicMetrics.__dict__.values()
        if callable(func) and func().attrs.get('requires_threshold_field', True)  # noqa
    ]

    metrics_df = ev.metrics.add_calculated_fields([
        tcf.AbovePercentileEventDetection(
            skip_event_id=True,
            add_quantile_field=True,
        )
    ]).query(
        include_metrics=include_conditional_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
    ).to_pandas()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert metrics_df.columns.size == 6
    ev.spark.stop()


def test_executing_signatures(tmpdir):
    """Test get_metrics method."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

    # Test all the metrics.
    include_all_metrics = [
        func() for func in Signatures.__dict__.values() if callable(func)
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
    ev.spark.stop()


def test_metrics_filter_and_geometry(tmpdir):
    """Test get_metrics method with filter and geometry."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

    # Define some metrics.
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    primary_avg = Signatures.Average()
    mvtd = DeterministicMetrics.MaxValueTimeDelta()
    pmvt = Signatures.MaxValueTime()

    include_metrics = [pmvt, mvtd, primary_avg, kge]

    # Get the currently available fields to use in the query.
    flds = ev.joined_timeseries.field_enum()

    # Define some filters.
    filters = [
        JoinedTimeseriesFilter(
            column=flds.primary_location_id,
            operator=ops.eq,
            value="gage-A"
        )
    ]

    metrics_df = ev.metrics.query(
        include_metrics=include_metrics,
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
        filters=filters,
    ).to_geopandas()

    assert isinstance(metrics_df, gpd.GeoDataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 6
    ev.spark.stop()


def test_metric_chaining(tmpdir):
    """Test get_metrics method with chaining."""
    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)

    # Test chaining.
    metrics_df = ev.metrics.query(
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
            Signatures.Average(
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
    ev.spark.stop()


def test_ensemble_metrics(tmpdir):
    """Test get_metrics method with ensemble metrics."""
    # Define the evaluation object.
    ev = setup_v0_5_ensemble_study(tmpdir)

    # Now, metrics.
    crps = ProbabilisticMetrics.CRPS()
    crps.summary_func = np.mean
    crps.estimator = "pwm"
    crps.backend = "numba"
    crps.reference_configuration = "benchmark_forecast_daily_normals"

    bs = ProbabilisticMetrics.BrierScore()
    bs.threshold = 0.75
    bs.summary_func = np.mean
    bs.backend = "numba"
    bs.reference_configuration = "benchmark_forecast_daily_normals"

    # assemble metrics df
    include_metrics = [crps, bs]
    metrics_df = ev.metrics.query(
        include_metrics=include_metrics,
        group_by=[
            "primary_location_id",
            "configuration_name"
        ],
        order_by=["primary_location_id", "configuration_name"],
    ).to_pandas()

    # check CRPS values
    assert np.isclose(metrics_df.mean_crps_ensemble.values[0], 22.050798)
    assert np.isclose(metrics_df.mean_crps_ensemble.values[1], 22.383705)
    assert np.isclose(
        metrics_df.mean_crps_ensemble_skill_score.values[0], 0.20777595
    )
    assert np.isnan(metrics_df.mean_crps_ensemble_skill_score.values[2])

    # check Brier Score values
    assert np.isclose(metrics_df.mean_brier_score.values[0], 0.18979715)
    assert np.isclose(metrics_df.mean_brier_score.values[1], 0.19405437)
    assert np.isclose(
        metrics_df.mean_brier_score_skill_score.values[0], 0.26453602
    )
    assert np.isnan(
        metrics_df.mean_brier_score_skill_score.values[2]
    )
    assert np.isnan(metrics_df.mean_crps_ensemble_skill_score.values[1])
    ev.spark.stop()


def test_metrics_transforms(tmpdir):
    """Test applying metric transforms (non-bootstrap)."""
    # Define the evaluation object.
    test_eval = setup_v0_3_study(tmpdir)

    # define metric requiring p,s
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge_t = DeterministicMetrics.KlingGuptaEfficiency()
    kge_t.transform = 'log'

    # add epsilon to avoid log(0)
    kge_t_e = DeterministicMetrics.KlingGuptaEfficiency()
    kge_t_e.transform = 'log'
    kge_t_e.add_epsilon = True

    # define metric requiring p,s,t
    mvtd = DeterministicMetrics.MaxValueTimeDelta()
    mvtd_t = DeterministicMetrics.MaxValueTimeDelta()
    mvtd_t.transform = 'log'

    # get metrics_df
    metrics_df_tansformed_e = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            kge_t_e,
            mvtd_t
        ]
    ).to_pandas()
    metrics_df_transformed = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            kge_t,
            mvtd_t
        ]
    ).to_pandas()
    metrics_df = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            kge,
            mvtd
        ]
    ).to_pandas()

    # get results for comparison
    result_kge = metrics_df.kling_gupta_efficiency.values[0]
    result_kge_t = metrics_df_transformed.kling_gupta_efficiency.values[0]
    result_kge_t_e = metrics_df_tansformed_e.kling_gupta_efficiency.values[0]
    result_mvtd = metrics_df.max_value_time_delta.values[0]
    result_mvtd_t = metrics_df_transformed.max_value_time_delta.values[0]

    # metrics_df_transformed is created, transforms are applied
    assert isinstance(metrics_df_tansformed_e, pd.DataFrame)
    assert isinstance(metrics_df_transformed, pd.DataFrame)
    assert result_kge_t != result_kge_t_e
    assert result_kge != result_kge_t
    assert result_mvtd == result_mvtd_t

    # test epsilon on R2 and Pearson
    r2 = DeterministicMetrics.Rsquared()
    r2_e = DeterministicMetrics.Rsquared()
    r2_e.add_epsilon = True
    pearson = DeterministicMetrics.PearsonCorrelation()
    pearson_e = DeterministicMetrics.PearsonCorrelation()
    pearson_e.add_epsilon = True

    # ensure we can obtain a divide by zero error
    sdf = test_eval.joined_timeseries.to_sdf()
    from pyspark.sql.functions import lit
    sdf = sdf.withColumn("primary_value", lit(100.0))
    test_eval.write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace",
    )

    # get metrics df control and assert divide by zero occurs
    metrics_df_e_control = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            r2,
            pearson
        ]
    ).to_pandas()
    assert np.isnan(metrics_df_e_control.r_squared.values).all()
    assert np.isnan(metrics_df_e_control.pearson_correlation.values).all()

    # get metrics df test and ensure no divide by zero occurs
    metrics_df_e_test = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            r2_e,
            pearson_e
        ]
    ).to_pandas()
    assert np.isfinite(metrics_df_e_test.r_squared.values).all()
    assert np.isfinite(metrics_df_e_test.pearson_correlation.values).all()

    # test epsilon on R2 and Pearson
    r2 = DeterministicMetrics.Rsquared()
    r2_e = DeterministicMetrics.Rsquared()
    r2_e.add_epsilon = True
    pearson = DeterministicMetrics.PearsonCorrelation()
    pearson_e = DeterministicMetrics.PearsonCorrelation()
    pearson_e.add_epsilon = True

    # ensure we can obtain a divide by zero error
    sdf = test_eval.joined_timeseries.to_sdf()
    from pyspark.sql.functions import lit
    sdf = sdf.withColumn("primary_value", lit(100.0))
    test_eval.write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace",
    )

    # get metrics df control and assert divide by zero occurs
    metrics_df_e_control = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            r2,
            pearson
        ]
    ).to_pandas()
    assert np.isnan(metrics_df_e_control.r_squared.values).all()
    assert np.isnan(metrics_df_e_control.pearson_correlation.values).all()

    # get metrics df test and ensure no divide by zero occurs
    metrics_df_e_test = test_eval.metrics.query(
        group_by=["primary_location_id", "configuration_name"],
        include_metrics=[
            r2_e,
            pearson_e
        ]
    ).to_pandas()
    assert np.isfinite(metrics_df_e_test.r_squared.values).all()
    assert np.isfinite(metrics_df_e_test.pearson_correlation.values).all()
    test_eval.spark.stop()


def test_adding_calculated_fields(tmpdir):
    """Test adding calculated fields to metrics."""
    from teehr import RowLevelCalculatedFields as rcf

    # Define the evaluation object.
    ev = setup_v0_3_study(tmpdir)
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    metrics_df_calc = (
        ev
        .metrics(table_name="joined_timeseries")
        .add_calculated_fields([
            rcf.Month()
        ])
        .query(
            group_by=["primary_location_id", "month"],
            include_metrics=[kge]
        )
        .to_pandas()
    )
    assert isinstance(metrics_df_calc, pd.DataFrame)
    assert metrics_df_calc.index.size == 3
    assert "month" in metrics_df_calc.columns
    ev.spark.stop()


def test_table_based_metrics(tmpdir):
    """Test table-based metrics."""
    ev = setup_v0_3_study(tmpdir)

    kge = DeterministicMetrics.KlingGuptaEfficiency()

    metrics_df = ev.table(table_name="joined_timeseries").query(
        include_metrics=[kge],
        group_by=["primary_location_id"],
        order_by=["primary_location_id"],
        filters="season = 'winter'",
    ).to_pandas()

    assert isinstance(metrics_df, pd.DataFrame)
    assert metrics_df.index.size == 3
    assert "primary_location_id" in metrics_df.columns

    primary_avg = Signatures.Average()
    primary_avg.input_field_names = ["value"]

    sigs_df = ev.table(table_name="primary_timeseries").query(
        include_metrics=[primary_avg],
        group_by=["location_id"],
        order_by=["location_id"],
        # filters="season = 'winter'",
    ).to_pandas()

    assert isinstance(sigs_df, pd.DataFrame)
    assert sigs_df.index.size == 3
    assert "location_id" in sigs_df.columns

    ev.spark.stop()


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
        # test_executing_signatures(
        #     tempfile.mkdtemp(
        #         prefix="2-",
        #         dir=tempdir
        #     )
        # )
        # test_metrics_filter_and_geometry(
        #     tempfile.mkdtemp(
        #         prefix="3-",
        #         dir=tempdir
        #     )
        # )
        # test_metric_chaining(
        #     tempfile.mkdtemp(
        #         prefix="4-",
        #         dir=tempdir
        #     )
        # )
        # High memory usage?
        test_ensemble_metrics(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
        # High memory usage?
        test_metrics_transforms(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir)
        )
        test_adding_calculated_fields(
            tempfile.mkdtemp(
                 prefix="7-",
                 dir=tempdir)
        )
        test_table_based_metrics(
            tempfile.mkdtemp(
                 prefix="8-",
                 dir=tempdir
            )
        )
