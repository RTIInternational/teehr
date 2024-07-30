import teehr.pre.locations as tpi
from pathlib import Path
import shutil
from teehr import Evaluation


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_study")
TEMP_DIR = Path("tests", "data", "temp")


def test_import_geojson():
    """Test the import_locations function on geojson."""
    input_filepath = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
    output_filepath = Path(TEMP_DIR, "locations", "location.parquet")
    if output_filepath.is_file():
        output_filepath.unlink()

    tpi.convert_locations(
        input_filepath=input_filepath,
        output_filepath=output_filepath,
    )
    assert output_filepath.is_file()


def test_validate_and_insert_locations():
    """Test the validate_locations function."""
    test_study_dir = Path(TEMP_DIR, "test_validate_and_insert_locations")
    if test_study_dir.is_dir():
        shutil.rmtree(test_study_dir)
    test_study_dir.mkdir()

    eval = Evaluation(dir_path=test_study_dir)
    eval.clone_template()

    geojson_filepath = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
    eval.import_locations(filepath=geojson_filepath)

    expected_output_filepath = Path(
        test_study_dir, "database", "locations", "locations.parquet"
    )
    assert expected_output_filepath.is_file()


if __name__ == "__main__":
    test_import_geojson()
    test_validate_and_insert_locations()