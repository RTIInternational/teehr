"""Pydantic models for domain table entries."""
from pydantic import BaseModel
from datetime import datetime


class TableBaseModel(BaseModel):
    """Base model for all tables.

    Adds a class method to get the field names.
    """

    @classmethod
    def get_field_names(cls, by_alias=False) -> list[str]:
        """Get the field names of the model."""
        field_names = []
        for k, v in cls.model_fields.items():
            if by_alias and v.alias:
                field_names.append(v.alias)
            else:
                field_names.append(k)

        return field_names


class Configuration(TableBaseModel):
    """Configuration model."""

    name: str
    type: str
    description: str


class Unit(TableBaseModel):
    """Unit model."""

    name: str
    long_name: str


class Variable(TableBaseModel):
    """Variable model."""

    name: str
    long_name: str


class Attribute(TableBaseModel):
    """Variable model."""

    name: str
    type: str
    description: str


class Location(TableBaseModel):
    """Location model."""

    id: str
    name: str
    geometry: bytes


class LocationAttribute(TableBaseModel):
    """LocationAttribute model."""

    location_id: str
    attribute_name: str
    value: str


class LocationCrosswalk(TableBaseModel):
    """LocationCrosswalk model."""

    primary_location_id: str
    secondary_location_id: str


class Timeseries(TableBaseModel):
    """Timeseries model."""

    reference_time: datetime
    value_time: datetime
    configuration_name: str
    unit_name: str
    variable_name: str
    value: float
    location_id: str

# Note: there is no JoinedTimeseries model in this file because the
# JoinedTimeseries model is dynamic and the fields can be different
# for each evaluation.
