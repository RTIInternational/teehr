"""Data types for the TEEHR schema."""

TIMESERIES_DATA_TYPES = {
    "value": "float32",
    "value_time": "datetime64[ms]",
    "reference_time": "datetime64[ms]",
    "location_id": "string",
    "measurement_unit": "category",
    "variable_name": "category",
    "configuration": "category"
}
