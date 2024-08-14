"""Test fetching and loading data into the dataset."""
from pathlib import Path
from datetime import datetime
import tempfile

from teehr import Evaluation


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_study")
GEO_GAGES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "geo",
    "usgs_point_geometry.test.parquet"
)
CROSSWALK_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "geo",
    "usgs_nwm30_crosswalk.test.parquet"
)


def test_fetch_and_load_nwm_retro_points(tmpdir):
    """Test the NWM retro fetch and load."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.load.import_locations(in_path=GEO_GAGES_FILEPATH)

    eval.fetch.usgs_streamflow(
        start_date=datetime(2022, 2, 22),
        end_date=datetime(2022, 2, 23)
    )

    eval.load.import_location_crosswalks(
        in_path=CROSSWALK_FILEPATH
    )

    eval.fetch.nwm_retrospective_points(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2022, 2, 22),
        end_date=datetime(2022, 2, 23)
    )
    # Assert something here.
    pass


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_fetch_and_load_nwm_retro_points(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
