"""Test the import_location_crosswalks function."""
from teehr.pre.location_crosswalks import convert_location_crosswalks
from pathlib import Path
from teehr import Evaluation
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "crosswalk.csv")


def test_convert_location_crosswalk(tmpdir):
    """Test the import_locations function on geojson."""
    output_filepath = Path(tmpdir, "crosswalk.parquet")

    convert_location_crosswalks(
        in_filepath=CROSSWALK_FILEPATH,
        out_filepath=output_filepath,
    )
    assert output_filepath.is_file()


def test_validate_and_insert_crosswalks(tmpdir):
    """Test the validate crosswalks function."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    eval.import_locations(
        in_path=GEOJSON_GAGES_FILEPATH
    )

    eval.import_location_crosswalks(
        in_path=CROSSWALK_FILEPATH
    )

    assert True


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
