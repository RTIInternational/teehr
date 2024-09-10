"""Module for parquet-based query models."""
from collections.abc import Iterable
from typing import List, Union
from pydantic import BaseModel as BaseModel
from pydantic import FieldValidationInfo, field_validator
from datetime import datetime
import logging
from teehr.models.str_enum import StrEnum
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
from teehr.models.tables import (
    TableBaseModel
)


logger = logging.getLogger(__name__)


class FilterOperators(StrEnum):
    """Filter symbols."""

    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class FilterBaseModel(BaseModel):
    """Base model for filters."""

    column: TableBaseModel
    operator: FilterOperators
    value: Union[str, List[str]]

    def is_iterable_not_str(obj):
        """Check if is type Iterable and not str.

        We should not have the case where a string is provided, but doesn't
        hurt to check.
        """
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @field_validator("value")
    def in_operator_must_have_iterable(cls, v, info: FieldValidationInfo):
        """Ensure that an 'in' operator has an iterable type."""
        if cls.is_iterable_not_str(v) and info.data["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if info.data["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )
        return v

    @field_validator("column", mode='before')
    def coerce_column_to_enum(cls, v, info: FieldValidationInfo):
        """Column name must exist in the database table."""
        if not isinstance(v, StrEnum):
            fields = info.context.get("fields_enum")
            v = fields[v]
        return v


class ConfigurationFilter(FilterBaseModel):
    """Configuration filter model."""

    column: ConfigurationFields


class UnitFilter(FilterBaseModel):
    """Unit filter model."""

    column: UnitFields


class VariableFilter(FilterBaseModel):
    """Variable filter model."""

    column: VariableFields


class AttributeFilter(FilterBaseModel):
    """Attribute filter model."""

    column: AttributeFields


class LocationFilter(FilterBaseModel):
    """Location filter model.

    ToDo: Add a geometry filter: lat, lon, intersect, etc.
    """

    column: LocationFields


class LocationAttributeFilter(FilterBaseModel):
    """Location attribute filter model.

    ToDo: How should we handle values types that are say a number stored
    as a string?
    """

    column: LocationAttributeFields


class LocationCrosswalkFilter(FilterBaseModel):
    """Location crosswalk filter model."""

    column: LocationCrosswalkFields


class TimeseriesFilter(FilterBaseModel):
    """Timeseries filter model."""

    column: TimeseriesFields
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]


class JoinedTimeseriesFilter(FilterBaseModel):
    """Joined timeseries filter model."""

    column: JoinedTimeseriesFields
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]
