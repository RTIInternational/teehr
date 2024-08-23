"""Test evaluation class."""
from teehr import Evaluation, Metrics
from teehr import Evaluation, Metrics
from teehr import Operators as ops
from pathlib import Path
import shutil
import tempfile
import pandas as pd
import geopandas as gpd

from teehr.models.dataset.filters import JoinedTimeseriesFilter
from teehr.models.metrics.bootstrap_models import Bootstrappers
# from teehr.metrics.bootstrappers import Bootstrappers

TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
JOINED_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "timeseries",
    "test_joined_timeseries_part1.parquet"
)


def test_get_all_metrics(tmpdir):
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
    flds = eval.fields.get_joined_timeseries_fields()

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

    # Test all the metrics.
    include_all_metrics = [
        func() for func in Metrics.__dict__.values() if callable(func)
    ]

    # Get the currently available fields to use in the query.
    flds = eval.fields.get_joined_timeseries_fields()

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

    # TEST:
    # import pandas as pd
    # import numpy as np

    # df = pd.read_parquet(JOINED_TIMESERIES_FILEPATH)
    # p = df["primary_value"]
    # s = df["secondary_value"]

    # boot = Bootstrappers.CircularBlock()
    # boot = Bootstrappers.Stationary()
    boot = Bootstrappers.GumBoots()

    # kge = Metrics.KlingGuptaEfficiency()

    # bs = boot.bootstrapper(365, p, s, seed=1234)
    # results = bs.apply(kge.func, 1000)
    # quantiles = (0.05, 0.50, 0.95)
    # values = np.quantile(results, quantiles)
    # quantiles = [f"KGE_{str(i)}" for i in quantiles]
    # d = dict(zip(quantiles,values))


    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)

    # END TEST

    # Define the metrics to include.

    # kge = Metrics.KlingGuptaEfficiency()
    primary_avg = Metrics.PrimaryAverage()
    mvtd = Metrics.MaxValueTimeDelta()
    pmvt = Metrics.PrimaryMaxValueTime()

    # include_metrics = [pmvt, mvtd, primary_avg, kge]
    include_metrics = [kge]

    # Get the currently available fields to use in the query.
    flds = eval.fields.get_joined_timeseries_fields()

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
        include_geometry=False
    )

    assert isinstance(metrics_df, gpd.GeoDataFrame)
    assert metrics_df.index.size == 1
    assert metrics_df.columns.size == 6


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
        test_metrics_filter_and_geometry(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
