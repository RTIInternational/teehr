"""Module for parquet-based query models."""
from collections.abc import Iterable
from typing import List, Union
from pydantic import BaseModel as BaseModel
from pydantic import ValidationInfo, field_validator
from datetime import datetime
try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:  # pragma: no cover
    from enum import Enum  # pragma: no cover

    class StrEnum(str, Enum):  # pragma: no cover
        """Enum with string values."""

        pass  # pragma: no cover

from teehr.models.dataset.table_enums import (
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


class FilterOperatorEnum(StrEnum):
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

    def is_iterable_not_str(obj):
        """Check if is type Iterable and not str.

        We should not have the case where a string is provided, but doesn't
        hurt to check.
        """
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @field_validator("value", check_fields=False)
    def in_operator_must_have_iterable(cls, v, info: ValidationInfo):
        """Ensure that an 'in' operator has an iterable type."""
        if cls.is_iterable_not_str(v) and info.data["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if info.data["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )
        return v


class ConfigurationFilter(FilterBaseModel):
    """Configuration filter model."""

    column: ConfigurationFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class UnitFilter(FilterBaseModel):
    """Unit filter model."""

    column: UnitFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class VariableFilter(FilterBaseModel):
    """Variable filter model."""

    column: VariableFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class AttributeFilter(FilterBaseModel):
    """Attribute filter model."""

    column: AttributeFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class LocationFilter(FilterBaseModel):
    """Location filter model.

    ToDo: Add a geometry filter: lat, lon, intersect, etc.
    """

    column: LocationFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class LocationAttributeFilter(FilterBaseModel):
    """Location attribute filter model.

    ToDo: How should we handle values types that are say a number stored
    as a string?
    """

    column: LocationAttributeFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class LocationCrosswalkFilter(FilterBaseModel):
    """Location crosswalk filter model."""

    column: LocationCrosswalkFields
    operator: FilterOperatorEnum
    value: Union[str, List[str]]


class TimeseriesFilter(FilterBaseModel):
    """Timeseries filter model."""

    column: TimeseriesFields
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]


class JoinedTimeseriesFilter(FilterBaseModel):
    """Joined timeseries filter model."""

    column: JoinedTimeseriesFields
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]
