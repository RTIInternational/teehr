from collections.abc import Iterable
from datetime import datetime
from enum import Enum  # StrEnum
from typing import List, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ValidationInfo, field_validator
from pathlib import Path


class BaseModel(PydanticBaseModel):
    class ConfigDict:
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

    @field_validator("value")
    def in_operator_must_have_iterable(cls, v, info: ValidationInfo):
        if cls.is_iterable_not_str(v) and info.data["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if info.data["operator"] == "in" and not cls.is_iterable_not_str(v):
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

    @field_validator("value")
    def in_operator_must_have_iterable(cls, v, info: ValidationInfo):
        if cls.is_iterable_not_str(v) and info.data["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if info.data["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
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
    remove_duplicates: Optional[bool] = True

    @field_validator("include_geometry")
    def include_geometry_must_group_by_primary_location_id(
        cls, v, info: ValidationInfo
    ):
        if (
            v is True
            and JoinedFilterFieldEnum.primary_location_id
            not in info.data["group_by"]  # noqa
        ):
            raise ValueError(
                "`group_by` must contain `primary_location_id` "
                "to include geometry in returned data"
            )

        if v is True and not info.data["geometry_filepath"]:
            raise ValueError(
                "`geometry_filepath` must be provided to include geometry "
                "in returned data"
            )

        if (
            JoinedFilterFieldEnum.geometry in info.data["group_by"]
            and v is False
        ):
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
    remove_duplicates: Optional[bool] = True

    @field_validator("include_geometry")
    def include_geometry_must_group_by_primary_location_id(
        cls, v, info: ValidationInfo
    ):
        if v is True and not info.data["geometry_filepath"]:
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
