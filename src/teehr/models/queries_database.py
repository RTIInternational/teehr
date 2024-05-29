"""Module for database query models."""
from collections.abc import Iterable
from datetime import datetime
try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:  # pragma: no cover
    from enum import Enum  # pragma: no cover

    class StrEnum(str, Enum):  # pragma: no cover
        pass  # pragma: no cover

from typing import List, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ValidationInfo, field_validator, model_validator
from pathlib import Path

from teehr.models.queries import FilterOperatorEnum, MetricEnum


class BaseModel(PydanticBaseModel):
    """Basemodel configuration."""

    class ConfigDict:
        """ConfigDict."""
        arbitrary_types_allowed = True
        # smart_union = True # deprecated in v2


class FieldTypeEnum(StrEnum):
    """Allowable duckdb data types."""

    BIGINT = "BIGINT"
    BIT = "BIT"
    BOOLEAN = "BOOLEAN"
    BLOB = "BLOB"
    DATE = "DATE"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    HUGEINT = "HUGEINT"
    INTEGER = "INTEGER"
    INTERVAL = "INTEGER"
    REAL = "REAL"
    SMALLINT = "SMALLINT"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TINYINT = "TINYINT"
    UBIGINT = "UBIGINT"
    UINTEGER = "UINTEGER"
    USMALLINT = "USMALLINT"
    UTINYINT = "UTINYINT"
    UUID = "UUID"
    VARCHAR = "VARCHAR"


class JoinedFieldNameEnum(StrEnum):
    """Names of fields in base joined_timeseries table."""

    reference_time = "reference_time"
    value_time = "value_time"
    secondary_location_id = "secondary_location_id"
    secondary_value = "secondary_value"
    configuration = "configuration"
    measurement_unit = "measurement_unit"
    variable_name = "variable_name"
    primary_value = "primary_value"
    primary_location_id = "primary_location_id"
    geometry = "geometry"


class TimeseriesNameEnum(StrEnum):
    """Timeseries Names."""

    primary = "primary"
    secondary = "secondary"


class JoinedTimeseriesFieldName(BaseModel):
    """Joined Timeseries Field Name model."""

    field_name: str

    @field_validator("field_name")
    def field_name_must_exist_in_timeseries_table(cls, v, info: ValidationInfo): # noqa
        """Field name must exist in the database table."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            if v not in existing_fields:
                raise ValueError(
                    f"The field name {v} does not exist in"
                    "the joined_timseries table"
                )
        return v


class CalculateField(BaseModel):
    """Calculate field model."""

    parameter_names: List[str]
    new_field_name: str
    new_field_type: FieldTypeEnum

    # TODO: Add field_name validator? (has already been sanitized)
    @field_validator("new_field_name")
    def field_name_must_be_valid(cls, v):
        """Must not contain special characters."""
        return v

    @field_validator("parameter_names")
    def parameter_names_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Parameter name must exist in the database table."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val not in existing_fields:
                    raise ValueError(
                        f"The function parameter {val} does not exist in the database"  # noqa
                    )
        return v


class Filter(BaseModel):
    """Filter model."""

    column: str
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]

    def is_iterable_not_str(obj):
        """Check if obj is iterable and not str."""
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @field_validator("value")
    def in_operator_must_have_iterable(
        cls, v: str, info: ValidationInfo
    ) -> str:
        """Ensure the 'in' operator has an iterable."""
        if cls.is_iterable_not_str(v) and info.data["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if info.data["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )

        return v


class InsertJoinedTimeseriesQuery(BaseModel):
    """InsertJoinedTimeseriesQuery model."""

    primary_filepath: Union[str, Path, List[Union[str, Path]]]
    secondary_filepath: Union[str, Path, List[Union[str, Path]]]
    crosswalk_filepath: Union[str, Path, List[Union[str, Path]]]
    order_by: Optional[List[JoinedFieldNameEnum]] = []
    filters: Optional[List[Filter]] = []


class JoinedTimeseriesQuery(BaseModel):
    """JoinedTimeseriesQuery model."""

    order_by: List[str]
    filters: Optional[List[Filter]] = []
    return_query: Optional[bool] = False
    include_geometry: bool

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        """Filter must be a list."""
        if v is None:
            return []
        return v

    @field_validator("order_by")
    def order_by_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Order_by fields must currently exist in the database."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val not in existing_fields:
                    raise ValueError(
                        f"The order_by field '{val}' does not"
                        "exist in the database"
                    )
        return v

    @field_validator("filters")
    def filters_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Filter fields must currently exist in the database."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val.column not in existing_fields:
                    raise ValueError(
                        f"The filters field {val.column} does not"
                        "exist in the database"
                    )
        return v


class TimeseriesQuery(BaseModel):
    """TimeseriesQuery model."""

    order_by: List[str]
    filters: Optional[List[Filter]] = []
    return_query: Optional[bool] = False
    timeseries_name: TimeseriesNameEnum

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        """Filter must be a list."""
        if v is None:
            return []
        return v

    @field_validator("order_by")
    def order_by_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Order_by fields must be part one of the selected fields or
        its alias."""
        validation_fields = [
            "location_id",
            "reference_time",
            "value",
            "primary_value",
            "secondary_value",
            "value_time",
            "primary_location_id",
            "secondary_location_id",
            "configuration",
            "measurement_unit",
            "variable_name"
        ]
        for val in v:
            if val not in validation_fields:
                raise ValueError(
                    f"The order_by field '{val}' must be a timeseries"
                    f" field or its alias: {validation_fields}"
                )
        return v

    @field_validator("filters")
    def filters_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Filter fields must currently exist in the database."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val.column not in existing_fields:
                    raise ValueError(
                        f"The filters field {val.column} does not"
                        "exist in the database"
                    )
        return v


class TimeseriesCharQuery(BaseModel):
    """Timeseries char query model."""

    order_by: List[str]
    group_by: List[str]
    filters: Optional[List[Filter]] = []
    return_query: Optional[bool] = False
    timeseries_name: TimeseriesNameEnum

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        """Filter must be a list."""
        if v is None:
            return []
        return v

    @field_validator("order_by")
    def order_by_must_exist_as_fields_or_chars(cls, v, info: ValidationInfo):
        """Order_by fields must currently exist in the database or be one of
        the calculated stats."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            existing_fields.extend(["count",
                                    "min",
                                    "max",
                                    "average",
                                    "sum",
                                    "variance"])
            for val in v:
                if val not in existing_fields:
                    raise ValueError(
                        f"The order_by or group_by field '{val}' does not"
                        "exist in the database"
                    )
        return v

    @field_validator("group_by")
    def group_by_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Group_by fields must currently exist in the database."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val not in existing_fields:
                    raise ValueError(
                        f"The order_by or group_by field '{val}' does not"
                        "exist in the database"
                    )
        return v

    @field_validator("group_by")
    def group_by_must_contain_primary_or_secondary_id(cls, v):
        """Group_by must contain primary or secondary id."""
        id_list = ["primary_location_id", "secondary_location_id"]
        if not any([val in id_list for val in v]):
            raise ValueError(
                "Group By must contain primary or secondary"
                " location id"
            )
        return v

    @field_validator("filters")
    def filters_must_exist_as_fields(cls, v, info: ValidationInfo):
        """Filter fields must currently exist in the database."""
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val.column not in existing_fields:
                    raise ValueError(
                        f"The filters field {val.column} does not"
                        "exist in the database"
                    )
        return v


class MetricQuery(BaseModel):
    """Metric query model."""

    include_geometry: bool
    group_by: List[str]
    order_by: List[str]
    include_metrics: Union[List[MetricEnum], MetricEnum, str]
    filters: Optional[List[Filter]] = None
    return_query: Optional[bool] = False

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        """Filter must be a list."""
        if v is None:
            return []
        return v

    @model_validator(mode="before")
    @classmethod
    def validate_include_geometry_and_specified_fields(
        cls, data, info: ValidationInfo
    ):
        """Validate 'include_geometry' and order_by, group_by
        and filters fields.
        """
        if data["include_geometry"]:
            # If geometry is included, group_by must
            # contain 'primary_location_id'
            if JoinedFieldNameEnum.primary_location_id not in data["group_by"]:
                raise ValueError(
                    "`group_by` must contain `primary_location_id` "
                    "to include geometry in returned data"
                )

        # order_by, group_by, and filter fields must
        # currently exist in the database
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in data["group_by"]:
                if val not in existing_fields:
                    raise ValueError(
                        f"The group_by field '{val}' does not"
                        "exist in the database"
                    )
            for val in data["order_by"]:
                if val not in existing_fields:
                    raise ValueError(
                        f"The order_by field '{val}' does not"
                        "exist in the database"
                    )
            if data["filters"]:
                for val in data["filters"]:
                    if val["column"] not in existing_fields:
                        raise ValueError(
                            f"The filter field '{val}' does not"
                            "exist in the database"
                        )

        return data
