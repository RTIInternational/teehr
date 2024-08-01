# We will test the import_timeseries function in the Evaluation class.
from pathlib import Path
import shutil
from teehr import Evaluation
from teehr.models.dataset import Configuration, Unit


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_study")
TEMP_DIR = Path("tests", "data", "temp")


def test_validate_and_insert_primary_timeseries():
    """Test the validate_locations function."""
    test_study_dir = Path(TEMP_DIR, "test_validate_and_insert_primary_timeseries")
    if test_study_dir.is_dir():
        shutil.rmtree(test_study_dir)
    test_study_dir.mkdir()

    eval = Evaluation(dir_path=test_study_dir)
    eval.clone_template()

    geojson_filepath = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
    eval.import_locations(filepath=geojson_filepath)

    eval.add_configuration(
        Configuration(
            name="test_obs",
            type="primary",
            description="Test Observations Data"
        )
    )

    eval.add_unit(
        Unit(
            name="m^3/s",
            long_name="Cubic Meters per Second",
            aliases=["cms"]
        )
    )

    primary_timeseries_filepath = Path(TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs.parquet")
    eval.import_primary_timeseries(
        path=primary_timeseries_filepath,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration_name": "configuration",
            "unit_name": "measurement_unit",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        })

    assert True


if __name__ == "__main__":
    test_validate_and_insert_primary_timeseries()