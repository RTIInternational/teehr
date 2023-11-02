from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import validator, ValidationInfo, field_validator
from pathlib import Path


class BaseModel(PydanticBaseModel):
    class Config:
        arbitrary_types_allowed = True
        # smart_union = True # deprecated in v2


class FilterOperatorEnum(str, Enum):
    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class MetricEnum(str, Enum):
    primary_count = "primary_count"
    secondary_count = "secondary_count"
    primary_minimum = "primary_minimum"
    secondary_minimum = "secondary_minimum"
    primary_maximum = "primary_maximum"
    secondary_maximum = "secondary_maximum"
    primary_average = "primary_average"
    secondary_average = "secondary_average"
    primary_sum = "primary_sum"
    secondary_sum = "secondary_sum"
    primary_variance = "primary_variance"
    secondary_variance = "secondary_variance"
    max_value_delta = "max_value_delta"
    bias = "bias"
    nash_sutcliffe_efficiency = "nash_sutcliffe_efficiency"
    kling_gupta_efficiency = "kling_gupta_efficiency"
    mean_error = "mean_error"
    mean_squared_error = "mean_squared_error"
    root_mean_squared_error = "root_mean_squared_error"
    primary_max_value_time = "primary_max_value_time"
    secondary_max_value_time = "secondary_max_value_time"
    max_value_timedelta = "max_value_timedelta"


class JoinedFilterFieldEnum(str, Enum):
    value_time = "value_time"
    reference_time = "reference_time"
    secondary_location_id = "secondary_location_id"
    secondary_value = "secondary_value"
    configuration = "configuration"
    measurement_unit = "measurement_unit"
    variable_name = "variable_name"
    primary_value = "primary_value"
    primary_location_id = "primary_location_id"
    lead_time = "lead_time"
    geometry = "geometry"


class TimeseriesFilterFieldEnum(str, Enum):
    value_time = "value_time"
    reference_time = "reference_time"
    location_id = "location_id"
    value = "value"
    configuration = "configuration"
    measurement_unit = "measurement_unit"
    variable_name = "variable_name"
    lead_time = "lead_time"
    geometry = "geometry"


class ChunkByEnum(str, Enum):
    day = "day"
    site = "site"


class FieldTypeEnum(str, Enum):
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
    """Names of fields in base joined_timeseries table"""

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


class CalculateFieldDB(BaseModel):
    parameter_names: List[str]
    new_field_name: str
    new_field_type: FieldTypeEnum

    # TODO: Add field_name validator
    @field_validator("new_field_name")
    @classmethod
    def field_name_must_be_valid(cls, v):
        # Must not contain special characters
        return v

    @field_validator("parameter_names")
    @classmethod
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


class JoinedFilterDB(BaseModel):
    column: str
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]

    def is_iterable_not_str(obj):
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @validator("value")
    def in_operator_must_have_iterable(cls, v, values):
        if cls.is_iterable_not_str(v) and values["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if values["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )

        return v


class JoinedFilter(BaseModel):
    column: JoinedFilterFieldEnum
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]

    def is_iterable_not_str(obj):
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @validator("value")
    def in_operator_must_have_iterable(cls, v, values):
        if cls.is_iterable_not_str(v) and values["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if values["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )

        return v


class TimeseriesFilter(BaseModel):
    column: TimeseriesFilterFieldEnum
    operator: FilterOperatorEnum
    value: Union[
        str, int, float, datetime, List[Union[str, int, float, datetime]]
    ]

    def is_iterable_not_str(obj):
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @validator("value")
    def in_operator_must_have_iterable(cls, v, values):
        if cls.is_iterable_not_str(v) and values["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if values["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )
        return v


class MetricQueryDB(BaseModel):
    group_by: List[str]
    order_by: List[str]
    include_metrics: Union[List[MetricEnum], MetricEnum, str]
    filters: Optional[List[JoinedFilterDB]] = []

    @field_validator("filters")
    @classmethod
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v

    # Need to validate contents of group_by, order_by, and filters
    @field_validator("group_by", "order_by")
    @classmethod
    def order_and_groupby_fields_must_exist(cls, v, info: ValidationInfo):
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val not in existing_fields:
                    raise ValueError(
                        f"The field '{val}' does not exist in the database"
                    )
        return v

    @field_validator("filters")
    @classmethod
    def filter_fields_must_exist(cls, v, info: ValidationInfo):
        context = info.context
        if context:
            existing_fields = context.get("existing_fields", set())
            for val in v:
                if val.column not in existing_fields:
                    raise ValueError(
                        f"The field '{val}' does not exist in the database"
                    )
        return v


class MetricQuery(BaseModel):
    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    group_by: List[JoinedFilterFieldEnum]
    order_by: List[JoinedFilterFieldEnum]
    include_metrics: Union[List[MetricEnum], MetricEnum, str]
    filters: Optional[List[JoinedFilter]] = []
    return_query: bool
    geometry_filepath: Optional[Union[str, Path]]
    include_geometry: bool

    @validator("include_geometry")
    def include_geometry_must_group_by_primary_location_id(cls, v, values):
        if (
            v is True
            and JoinedFilterFieldEnum.primary_location_id
            not in values["group_by"]  # noqa
        ):
            raise ValueError(
                "`group_by` must contain `primary_location_id` "
                "to include geometry in returned data"
            )

        if v is True and not values["geometry_filepath"]:
            raise ValueError(
                "`geometry_filepath` must be provided to include geometry "
                "in returned data"
            )

        if JoinedFilterFieldEnum.geometry in values["group_by"] and v is False:
            raise ValueError(
                "group_by contains `geometry` field but `include_geometry` "
                "is False, must be True"
            )

        return v

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v


class JoinedTimeseriesQuery(BaseModel):
    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    order_by: List[JoinedFilterFieldEnum]
    filters: Optional[List[JoinedFilter]] = []
    return_query: bool
    geometry_filepath: Optional[Union[str, Path]]
    include_geometry: bool

    @validator("include_geometry")
    def include_geometry_must_group_by_primary_location_id(cls, v, values):
        if v is True and not values["geometry_filepath"]:
            raise ValueError(
                "`geometry_filepath` must be provided to include geometry "
                "in returned data"
            )

        return v

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v


class JoinedTimeseriesQueryDB(BaseModel):
    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    order_by: Optional[List[JoinedFilterFieldEnum]] = []


class TimeseriesQuery(BaseModel):
    timeseries_filepath: Union[str, Path]
    order_by: List[TimeseriesFilterFieldEnum]
    filters: Optional[List[TimeseriesFilter]] = []
    return_query: bool

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v


class TimeseriesCharQuery(BaseModel):
    timeseries_filepath: Union[str, Path]
    order_by: List[TimeseriesFilterFieldEnum]
    group_by: List[TimeseriesFilterFieldEnum]
    filters: Optional[List[TimeseriesFilter]] = []
    return_query: bool

    @field_validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v
