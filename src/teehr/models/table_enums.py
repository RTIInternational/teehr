"""Field enums for each table in the dataset."""
from teehr.models.str_enum import StrEnum


class TableNamesEnum(StrEnum):
    """Table names for the dataset."""

    primary_timeseries = "primary_timeseries"
    secondary_timeseries = "secondary_timeseries"
    locations = "locations"
    units = "units"
    variables = "variables"
    configurations = "configurations"
    attributes = "attributes"
    location_attributes = "location_attributes"
    location_crosswalks = "location_crosswalks"


class TableWriteEnum(StrEnum):
    """Methods to write or update Evaluation tables."""

    insert = "insert"
    append = "append"
    upsert = "upsert"
    overwrite = "overwrite"
    create_or_replace = "create_or_replace"
