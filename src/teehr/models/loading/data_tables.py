"""Field names for the data tables in the dataset.

Its purpose is to provide a consistent reference for the field names in the
data tables in the dataset.

Perhaps the field names should be defined as Pydantic models instead of lists.
Or maybe as a structure related to Pandera schemas (if we use that).
"""
location_attributes_field_names = [
    "location_id",
    "attribute_name",
    "value"
]
location_crosswalks_field_names = [
    "primary_location_id",
    "secondary_location_id"
]
locations_field_names = [
    "id",
    "name",
    "geometry"
]
timeseries_field_names = [
    "reference_time",
    "value_time",
    "configuration_name",
    "unit_name",
    "variable_name",
    "value",
    "location_id"
]
