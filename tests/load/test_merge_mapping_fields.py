"""Tests for merging of default and custom field mappings."""
from teehr.loading.utils import merge_field_mappings


def test_merge_mapping_fields():
    """Test merging of default and custom field mappings."""
    default_mapping = {
        "reference_time": "reference_time",
        "value_time": "value_time",
        "configuration_name": "configuration_name",
        "unit_name": "unit_name",
        "variable_name": "variable_name",
        "value": "value",
        "location_id": "location_id",
    }
    custom_mapping = {
        "configuration": "configuration_name",
        "unit": "unit_name",
        "variable": "variable_name"
    }
    merged = merge_field_mappings(default_mapping, custom_mapping)

    target = {
        'reference_time': 'reference_time',
        'value_time': 'value_time',
        'configuration': 'configuration_name',
        'unit': 'unit_name',
        'variable': 'variable_name',
        'value': 'value',
        'location_id': 'location_id'
    }
    assert merged == target


if __name__ == "__main__":
    test_merge_mapping_fields()