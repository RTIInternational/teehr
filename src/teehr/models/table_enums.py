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
    joined_timeseries = "joined_timeseries"


class TableWriteEnum(StrEnum):
    """Methods to write or update Evaluation tables."""

    append = "append"
    upsert = "upsert"
    overwrite = "overwrite"


class ConfigurationFields(StrEnum):
    """Empty class for ConfigurationFieldEnum."""

    pass


class UnitFields(StrEnum):
    """Empty class for UnitFieldEnum."""

    pass


class VariableFields(StrEnum):
    """Empty class for VariableFieldEnum."""

    pass


class AttributeFields(StrEnum):
    """Empty class for AttributeFieldEnum."""

    pass


class LocationFields(StrEnum):
    """Empty class for LocationFieldEnum."""

    pass


class LocationAttributeFields(StrEnum):
    """Empty class for LocationAttributeFieldEnum."""

    pass


class LocationCrosswalkFields(StrEnum):
    """Empty class for LocationCrosswalkFieldEnum."""

    pass


class TimeseriesFields(StrEnum):
    """Empty class for TimeseriesFieldEnum."""

    pass


class JoinedTimeseriesFields(StrEnum):
    """Empty class for JoinedTimeseriesFieldEnum."""

    pass
