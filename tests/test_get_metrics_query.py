"""Test evaluation class."""
from teehr import Evaluation, Metrics, Bootstrap
from teehr import Operators as ops
from pathlib import Path
import shutil
import tempfile

from teehr.models.dataset.filters import JoinedTimeseriesFilter

TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
JOINED_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "timeseries",
    "test_joined_timeseries_part1.parquet"
)


def test_get_metrics(tmpdir):
    """Test get_metrics method."""
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

    # Define the metrics to include.
    boot = Bootstrap(method="bias_corrected", num_samples=100)
    kge = Metrics.KlingGuptaEfficiency()
    primary_avg = Metrics.PrimaryAverage()

    include_metrics = [kge, primary_avg]
    # include_metrics = [kge]

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
        include_geometry=True
    )

    assert metrics_df.index.size > 0

    print(metrics_df)

    pass


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_get_metrics(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
