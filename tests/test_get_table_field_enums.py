import tempfile
from teehr.models.table_enums import (
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

from setup_v0_3_study import setup_v0_3_study


def test_get_configuration_fields(tmpdir):
    """Test the validate location_attributes function."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.configurations.field_enum()
    for field in fields:
        assert isinstance(field, ConfigurationFields)


def test_get_unit_fields(tmpdir):
    """Test the validate location_attributes function."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.units.field_enum()
    for field in fields:
        assert isinstance(field, UnitFields)


def test_get_variable_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.variables.field_enum()
    for field in fields:
        assert isinstance(field, VariableFields)


def test_get_attribute_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.attributes.field_enum()
    for field in fields:
        assert isinstance(field, AttributeFields)


def test_get_location_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.locations.field_enum()
    for field in fields:
        assert isinstance(field, LocationFields)


def test_get_location_attribute_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.location_attributes.field_enum()
    for field in fields:
        assert isinstance(field, LocationAttributeFields)


def test_get_location_crosswalk_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.location_crosswalks.field_enum()
    for field in fields:
        assert isinstance(field, LocationCrosswalkFields)


def test_get_timeseries_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    for field in fields:
        assert isinstance(field, TimeseriesFields)


def test_get_joined_timeseries_fields(tmpdir):
    """Test the validate location_attributes."""
    eval = setup_v0_3_study(tmpdir)
    eval.create_joined_timeseries()
    fields = eval.joined_timeseries.field_enum()
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

