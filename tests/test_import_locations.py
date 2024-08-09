"""Test the import locations functionality."""
from teehr.pre.locations import convert_locations
from pathlib import Path
from teehr import Evaluation
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")


def test_convert_locations_geojson(tmpdir):
    """Test the convert_locations function on geojson."""
    output_filepath = Path(tmpdir, "gages.parquet")

    convert_locations(
        in_path=GEOJSON_GAGES_FILEPATH,
        out_dirpath=tmpdir,
    )
    assert output_filepath.is_file()


def test_validate_and_insert_locations(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    eval.import_locations(in_path=GEOJSON_GAGES_FILEPATH)

    assert Path(eval.locations_dir, "gages.parquet").is_file()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_convert_locations_geojson(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_validate_and_insert_locations(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
