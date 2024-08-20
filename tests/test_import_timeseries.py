"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation
from teehr.models.dataset.table_models import (
    Configuration,
    # Unit,
    Variable
)
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)


def test_validate_and_insert_timeseries(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.load.import_locations(in_path=GEOJSON_GAGES_FILEPATH)

    eval.load.add_configuration(
        Configuration(
            name="test_obs",
            type="primary",
            description="Test Observations Data"
        )
    )

    # eval.add_unit(
    #     Unit(
    #         name="m^3/s",
    #         long_name="Cubic Meters per Second"
    #     )
    # )

    eval.load.add_variable(
        Variable(
            name="streamflow",
            long_name="Streamflow"
        )
    )

    eval.load.import_primary_timeseries(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        })

    eval.load.import_location_crosswalks(
        in_path=CROSSWALK_FILEPATH
    )

    eval.load.add_configuration(
        Configuration(
            name="test_short",
            type="secondary",
            description="Test Forecast Data"
        )
    )

    eval.load.import_secondary_timeseries(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        }
    )

    assert True


def test_validate_and_insert_timeseries_set_const(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.load.import_locations(in_path=GEOJSON_GAGES_FILEPATH)

    eval.load.import_primary_timeseries(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "usgs_observations"
        }
    )

    eval.load.import_location_crosswalks(
        in_path=CROSSWALK_FILEPATH
    )

    eval.load.import_secondary_timeseries(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "nwm30_retrospective"
        }
    )

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
        test_validate_and_insert_timeseries_set_const(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
