from collections.abc import Iterable
from datetime import datetime
from enum import Enum  # , StrEnum  if 3.11
from typing import List, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ValidationInfo, field_validator, model_validator
from pathlib import Path

from teehr.models.queries import FilterOperatorEnum, MetricEnum


class BaseModel(PydanticBaseModel):
    class ConfigDict:
        arbitrary_types_allowed = True
        # smart_union = True # deprecated in v2


class FieldTypeEnum(str, Enum):
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


class JoinedFieldNameEnum(str, Enum):
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
    lead_time = "lead_time"
    absolute_difference = "absolute_difference"
    geometry = "geometry"


class TimeseriesNameEnum(str, Enum):
    primary = "primary"
    secondary = "secondary"


class JoinedTimeseriesFieldName(BaseModel):
    field_name: str

    @field_validator("field_name")
    def field_name_must_exist_in_timeseries_table(cls, v, info: ValidationInfo): # noqa
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
    parameter_names: List[str]
    new_field_name: str
    new_field_type: FieldTypeEnum

    # TODO: Add field_name validator? (has already been sanitized)
    @field_validator("new_field_name")
    def field_name_must_be_valid(cls, v):
        # Must not contain special characters
        return v

    @field_validator("parameter_names")
    def parameter_names_must_exist_as_fields(cls, v, info: ValidationInfo):
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
    column: str
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]

    def is_iterable_not_str(obj):
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @model_validator(mode="before")
    @classmethod
    def in_operator_must_have_iterable(
        cls, data, info: ValidationInfo
    ) -> str:
        value = data["value"]
        operator = data["operator"]
        if cls.is_iterable_not_str(value) and operator != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if operator == "in" and not cls.is_iterable_not_str(value):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )

        return data


class InsertJoinedTimeseriesQuery(BaseModel):
    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    order_by: Optional[List[JoinedFieldNameEnum]] = []


class JoinedTimeseriesQuery(BaseModel):
    order_by: List[str]
    filters: Optional[List[Filter]] = []
    return_query: Optional[bool] = False
    include_geometry: bool

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v

    @field_validator("order_by")
    def order_by_must_exist_as_fields(cls, v, info: ValidationInfo):
        """order_by fields must currently exist in the database"""
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
        """filters fields must currently exist in the database"""
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
    order_by: List[str]
    filters: Optional[List[Filter]] = []
    return_query: Optional[bool] = False
    timeseries_name: TimeseriesNameEnum

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v

    @field_validator("order_by")
    def order_by_must_exist_as_fields(cls, v, info: ValidationInfo):
        """order_by fields must be part one of the selected fields or
        its alias"""
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
        """filters fields must currently exist in the database"""
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
    order_by: List[str]
    group_by: List[str]
    filters: Optional[List[Filter]] = []
    return_query: Optional[bool] = False
    timeseries_name: TimeseriesNameEnum

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v

    @field_validator("order_by")
    def order_by_must_exist_as_fields_or_chars(cls, v, info: ValidationInfo):
        """order_by fields must currently exist in the database or be one of
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
        """group_by fields must currently exist in the database"""
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
        id_list = ["primary_location_id", "secondary_location_id"]
        if not any([val in id_list for val in v]):
            raise ValueError(
                "Group By must contain primary or secondary"
                " location id"
            )
        return v

    @field_validator("filters")
    def filters_must_exist_as_fields(cls, v, info: ValidationInfo):
        """filters fields must currently exist in the database"""
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
    include_geometry: bool
    group_by: List[str]
    order_by: List[str]
    include_metrics: Union[List[MetricEnum], MetricEnum, str]
    filters: Optional[List[Filter]] = None
    return_query: Optional[bool] = False

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v

    @model_validator(mode="before")
    @classmethod
    def validate_include_geometry_and_specified_fields(
        cls, data, info: ValidationInfo
    ):
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
