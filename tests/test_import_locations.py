import teehr.pre.locations as tpi
from pathlib import Path
from teehr import Evaluation
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")


def test_import_geojson(tmpdir):
    """Test the import_locations function on geojson."""
    output_filepath = Path(tmpdir, "location.parquet")

    tpi.convert_locations(
        input_filepath=GEOJSON_GAGES_FILEPATH,
        output_filepath=output_filepath,
    )
    assert output_filepath.is_file()


def test_validate_and_insert_locations(tmpdir):
    """Test the validate_locations function."""

    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    eval.import_locations(in_filepath=GEOJSON_GAGES_FILEPATH)

    assert True


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_import_geojson(
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
