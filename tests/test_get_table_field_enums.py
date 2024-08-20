from teehr import Evaluation
from pathlib import Path
import tempfile
from teehr.models.dataset.table_enums import (
    ConfigurationFields,
    UnitFields,
    VariableFields,
    AttributeFields,
    LocationFields,
    LocationAttributeFields,
    LocationCrosswalkFields,
    TimeseriesFields,
    JoinedTimeseriesFields
)

TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)


def test_get_configuration_fields(tmpdir):
    """Test the validate location_attributes function."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_configuration_fields()
    for field in fields:
        assert isinstance(field, ConfigurationFields)


def test_get_unit_fields(tmpdir):
    """Test the validate location_attributes function."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_unit_fields()
    for field in fields:
        assert isinstance(field, UnitFields)


def test_get_variable_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_variable_fields()
    for field in fields:
        assert isinstance(field, VariableFields)


def test_get_attribute_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_attribute_fields()
    for field in fields:
        assert isinstance(field, AttributeFields)


def test_get_location_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_location_fields()
    for field in fields:
        assert isinstance(field, LocationFields)


def test_get_location_attribute_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_location_attribute_fields()
    for field in fields:
        assert isinstance(field, LocationAttributeFields)


def test_get_location_crosswalk_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_location_crosswalk_fields()
    for field in fields:
        assert isinstance(field, LocationCrosswalkFields)


def test_get_timeseries_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    fields = eval.fields.get_timeseries_fields()
    for field in fields:
        assert isinstance(field, TimeseriesFields)


def test_get_joined_timeseries_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = Evaluation(dir_path=tmpdir)
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
            "configuration_name": "nwm30_retro"
        }
    )

    eval.create_joined_timeseries()

    fields = eval.fields.get_joined_timeseries_fields()
    for field in fields:
        assert isinstance(field, JoinedTimeseriesFields)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_get_configuration_fields(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_get_unit_fields(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_get_variable_fields(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_get_attribute_fields(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_get_location_fields(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
        test_get_location_attribute_fields(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir
            )
        )
        test_get_location_crosswalk_fields(
            tempfile.mkdtemp(
                prefix="7-",
                dir=tempdir
            )
        )
        test_get_timeseries_fields(
            tempfile.mkdtemp(
                prefix="8-",
                dir=tempdir
            )
        )
        test_get_joined_timeseries_fields(
            tempfile.mkdtemp(
                prefix="9-",
                dir=tempdir
            )
        )

