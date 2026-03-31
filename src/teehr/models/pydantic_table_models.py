"""Pydantic models for domain table entries."""
import datetime

from pydantic import BaseModel


class TableBaseModel(BaseModel):
    """Base model for all tables."""

    pass


class Configuration(TableBaseModel):
    """Configuration model."""

    name: str
    timeseries_type: str
    description: str
    properties: dict | None = None
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None


class Unit(TableBaseModel):
    """Unit model."""

    name: str
    long_name: str
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None


class Variable(TableBaseModel):
    """Variable model."""

    name: str
    long_name: str
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None


class Attribute(TableBaseModel):
    """Attribute model."""

    name: str
    type: str
    description: str
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None


# class Location(TableBaseModel):
#     """Location model."""

#     id: str
#     name: str
#     geometry: bytes


# class LocationAttribute(TableBaseModel):
#     """LocationAttribute model."""

#     location_id: str
#     attribute_name: str
#     value: str


# class LocationCrosswalk(TableBaseModel):
#     """LocationCrosswalk model."""

#     primary_location_id: str
#     secondary_location_id: str


# class Timeseries(TableBaseModel):
#     """Timeseries model."""

#     reference_time: datetime
#     value_time: datetime
#     configuration_name: str
#     unit_name: str
#     variable_name: str
#     value: float
#     location_id: str

# Note: there is no JoinedTimeseries model in this file because the
# JoinedTimeseries model is dynamic and the fields can be different
# for each evaluation.
