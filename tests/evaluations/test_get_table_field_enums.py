"""Tests related to field enums."""
import pytest
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


@pytest.mark.session_scope_evaluation_template
def test_get_configuration_fields(session_scope_evaluation_template):
    """Test the validate location_attributes function."""
    ev = session_scope_evaluation_template
    fields = ev.configurations.field_enum()
    for field in fields:
        assert isinstance(field, ConfigurationFields)


@pytest.mark.session_scope_evaluation_template
def test_get_unit_fields(session_scope_evaluation_template):
    """Test the validate location_attributes function."""
    ev = session_scope_evaluation_template
    fields = ev.units.field_enum()
    for field in fields:
        assert isinstance(field, UnitFields)


@pytest.mark.session_scope_evaluation_template
def test_get_variable_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    fields = ev.variables.field_enum()
    for field in fields:
        assert isinstance(field, VariableFields)


@pytest.mark.session_scope_evaluation_template
def test_get_attribute_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    fields = ev.attributes.field_enum()
    for field in fields:
        assert isinstance(field, AttributeFields)


@pytest.mark.session_scope_evaluation_template
def test_get_location_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    fields = ev.locations.field_enum()
    for field in fields:
        assert isinstance(field, LocationFields)


@pytest.mark.session_scope_evaluation_template
def test_get_location_attribute_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    fields = ev.location_attributes.field_enum()
    for field in fields:
        assert isinstance(field, LocationAttributeFields)


@pytest.mark.session_scope_evaluation_template
def test_get_location_crosswalk_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    fields = ev.location_crosswalks.field_enum()
    for field in fields:
        assert isinstance(field, LocationCrosswalkFields)


@pytest.mark.session_scope_evaluation_template
def test_get_timeseries_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    fields = ev.primary_timeseries.field_enum()
    for field in fields:
        assert isinstance(field, TimeseriesFields)


@pytest.mark.session_scope_evaluation_template
def test_get_joined_timeseries_fields(session_scope_evaluation_template):
    """Test the validate location_attributes."""
    ev = session_scope_evaluation_template
    ev.joined_timeseries.create()
    fields = ev.joined_timeseries.field_enum()
    for field in fields:
        assert isinstance(field, JoinedTimeseriesFields)
