"""Test the import_location_crosswalks function."""
from teehr.loading.location_crosswalks import convert_location_crosswalks
from pathlib import Path
from teehr import Evaluation
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "crosswalk.csv")
CROSSWALK_FILEPATH_NC = Path(
    "tests", "data", "test_study", "geo", "nwm_v3_route_link_subset.nc"
)


def test_convert_location_crosswalk(tmpdir):
    """Test the import_locations function on geojson."""
    output_filepath = Path(tmpdir, "crosswalk.parquet")

    convert_location_crosswalks(
        in_path=CROSSWALK_FILEPATH,
        out_dirpath=tmpdir,
    )
    assert output_filepath.is_file()


def test_validate_and_insert_crosswalks(tmpdir):
    """Test the validate crosswalks function."""
    ev = Evaluation(dir_path=tmpdir, create_dir=True)
    ev.clone_template()

    ev.locations.load_spatial(
        in_path=GEOJSON_GAGES_FILEPATH,
        location_id_prefix="usgs"
    )

    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH,
        primary_location_id_prefix="usgs",
        secondary_location_id_prefix="nwm30",
    )

    df = ev.location_crosswalks.to_pandas()
    assert df["primary_location_id"].tolist() == [
        "usgs-A",
        "usgs-B",
        "usgs-C",
    ]
    assert df["secondary_location_id"].tolist() == [
        "nwm30-1",
        "nwm30-2",
        "nwm30-3",
    ]

    assert True
    ev.spark.stop()


def test_convert_location_crosswalk_netcdf(tmpdir):
    """Test the import_locations function on netcdf."""
    output_filepath = Path(tmpdir, "nwm_v3_route_link_subset.parquet")

    field_mapping = {
        "link": "secondary_location_id",
        "gages": "primary_location_id"
    }

    convert_location_crosswalks(
        in_path=CROSSWALK_FILEPATH_NC,
        out_dirpath=tmpdir,
        field_mapping=field_mapping,
    )
    assert output_filepath.is_file()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_convert_location_crosswalk(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_validate_and_insert_crosswalks(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_convert_location_crosswalk_netcdf(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
