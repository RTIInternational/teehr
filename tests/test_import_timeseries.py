"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation
from teehr.models.domain_tables import (
    Configuration,
    Unit,
    Variable
)
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "timeseries", "test_short_fcast.parquet")


def test_validate_and_insert_timeseries(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.import_locations(in_filepath=GEOJSON_GAGES_FILEPATH)

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

    eval.add_variable(
        Variable(
            name="streamflow",
            long_name="Streamflow"
        )
    )

    eval.import_primary_timeseries(
        directory_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        })

    eval.import_location_crosswalks(
        in_filepath=CROSSWALK_FILEPATH
    )

    eval.add_configuration(
        Configuration(
            name="test_short",
            type="secondary",
            description="Test Forecast Data"
        )
    )

    eval.import_secondary_timeseries(
        directory_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        })

    assert True


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_validate_and_insert_timeseries(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
