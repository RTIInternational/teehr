"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation
from teehr.models.dataset.table_models import (
    Attribute
)
import tempfile

TEST_DATA_DIR = Path("/home/matt/repos/teehr/tests/data/v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)
GEO_FILEPATH = Path(TEST_DATA_DIR, "geo")


def test_create_joined_timeseries(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    # Enable logging
    eval.enable_logging()

    # Clone the template
    eval.clone_template()

    # Load the location data
    eval.load.import_locations(in_path=GEOJSON_GAGES_FILEPATH)

    # Load the timeseries data and map over the fields and set constants
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

    # Load the crosswalk data
    eval.load.import_location_crosswalks(
        in_path=CROSSWALK_FILEPATH
    )

    # Load the secondary timeseries data and map over the fields and set constants
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
            "configuration_name": "nwm30_retro"
        }
    )

    # Add some attributes
    eval.load.add_attribute(
        [
            Attribute(
                name="drainage_area",
                type="continuous",
                description="Drainage area in square kilometers"
            ),
            Attribute(
                name="ecoregion",
                type="categorical",
                description="Ecoregion"
            ),
            Attribute(
                name="year_2_discharge",
                type="continuous",
                description="2-yr discharge in cubic meters per second"
            ),
        ]
    )

    # Load the location attribute data
    eval.load.import_location_attributes(
        in_path=GEO_FILEPATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
    )

    # Create the joined timeseries
    eval.create_joined_timeseries(execute_udf=True)

    assert True


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_create_joined_timeseries(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
