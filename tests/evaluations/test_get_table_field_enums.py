"""Tests related to field enums."""
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

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa


def test_get_configuration_fields(tmpdir):
    """Test the validate location_attributes function."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.configurations.field_enum()
    for field in fields:
        assert isinstance(field, ConfigurationFields)
    # ev.spark.stop()


def test_get_unit_fields(tmpdir):
    """Test the validate location_attributes function."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.units.field_enum()
    for field in fields:
        assert isinstance(field, UnitFields)
    # ev.spark.stop()


def test_get_variable_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.variables.field_enum()
    for field in fields:
        assert isinstance(field, VariableFields)
    # ev.spark.stop()


def test_get_attribute_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.attributes.field_enum()
    for field in fields:
        assert isinstance(field, AttributeFields)
    # ev.spark.stop()


def test_get_location_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.locations.field_enum()
    for field in fields:
        assert isinstance(field, LocationFields)
    # ev.spark.stop()


def test_get_location_attribute_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.location_attributes.field_enum()
    for field in fields:
        assert isinstance(field, LocationAttributeFields)
    # ev.spark.stop()


def test_get_location_crosswalk_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.location_crosswalks.field_enum()
    for field in fields:
        assert isinstance(field, LocationCrosswalkFields)
    # ev.spark.stop()


def test_get_timeseries_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    fields = ev.primary_timeseries.field_enum()
    for field in fields:
        assert isinstance(field, TimeseriesFields)
    # ev.spark.stop()


def test_get_joined_timeseries_fields(tmpdir):
    """Test the validate location_attributes."""
    tmpdir = Path(tmpdir)
    ev = setup_v0_3_study(tmpdir)
    ev.joined_timeseries.create()
    fields = ev.joined_timeseries.field_enum()
    for field in fields:
        assert isinstance(field, JoinedTimeseriesFields)
    # ev.spark.stop()


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

