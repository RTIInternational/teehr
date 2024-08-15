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
ZONAL_WEIGHTS = Path(
    "tests", "data", "nwm22", "onehuc10_weights_retro.parquet"
)
ZONAL_LOCATIONS = Path(
    TEST_STUDY_DATA_DIR,
    "geo",
    "one_huc10_conus_1016000606.parquet"
)


def test_fetch_and_load_nwm_retro_points(tmpdir):
    """Test the NWM retro point fetch and load."""
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
    # TODO: Assert something here.
    pass


def test_fetch_and_load_nwm_retro_grids(tmpdir):
    """Test the NWM retro grid fetch and load."""
    eval = Evaluation(dir_path=tmpdir)
    eval.enable_logging()
    eval.clone_template()

    # Add locations corresponding to weights file.
    eval.load.import_locations(in_path=ZONAL_LOCATIONS)

    eval.fetch.nwm_retrospective_grids(
        nwm_version="nwm30",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        location_id_prefix="huc10"
    )
    # TODO: Assert something here.
    pass


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        # test_fetch_and_load_nwm_retro_points(
        #     tempfile.mkdtemp(
        #         prefix="1-",
        #         dir=tempdir
        #     )
        # )
        test_fetch_and_load_nwm_retro_grids(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )