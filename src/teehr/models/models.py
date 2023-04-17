from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import validator
from pathlib import Path


class BaseModel(PydanticBaseModel):
    class Config:
        arbitrary_types_allowed = True
        smart_union = True


class OperatorEnum(str, Enum):
    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class MetricFilterFieldEnum(str, Enum):
    value_time = "value_time"
    reference_time = "reference_time"
    secondary_location_id = "secondary_location_id"
    secondary_value = "secondary_value"
    configuration = "configuration"
    measurement_unit = "measurement_unit"
    variable_name = "variable_name"
    observed_value = "observed_value"
    primary_location_id = "primary_location_id"
    lead_time = "lead_time"
    geometry = "geometry"


# class MetricEnum(str, Enum):
#     value_time = "value_time"
#     reference_time = "reference_time"
#     secondary_location_id = "secondary_location_id"
#     secondary_value = "secondary_value"
#     configuration = "configuration"
#     measurement_unit = "measurement_unit"
#     variable_name = "variable_name"
#     observed_value = "observed_value"
#     primary_location_id = "primary_location_id"
#     lead_time = "lead_time"
#     geometry = "geometry"

class Filter(BaseModel):
    column: MetricFilterFieldEnum
    operator: OperatorEnum
    value: Union[
        str, int, float, datetime,
        List[Union[str, int, float, datetime]]
    ]

    def is_iterable_not_str(obj):
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @validator('value')
    def in_operator_must_have_iterable(cls, v, values):
        if cls.is_iterable_not_str(v) and values["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if values["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )

        return v


class MetricQuery(BaseModel):
    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    group_by: List[MetricFilterFieldEnum]
    order_by: List[MetricFilterFieldEnum]
    filters: List[Filter] = []
    return_query: bool
    geometry_filepath: Optional[Union[str, Path]]
    include_geometry: bool

    @validator('include_geometry')
    def include_geometry_must_group_by_primary_location_id(cls, v, values):
        if (
            v is True
            and MetricFilterFieldEnum.primary_location_id not in values["group_by"]  # noqa
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

        if MetricFilterFieldEnum.geometry in values["group_by"] and v is False:
            raise ValueError(
                "group_by contains `geometry` field but `include_geometry` "
                "is False, must be True"
            )

        return v

    @validator('order_by')
    def must_include_one_order_by(cls, v, values):
        if len(v) < 1:
            raise ValueError("order_by must contain at least one field name.")

        return v

    @validator('group_by')
    def must_include_one_group_by(cls, v, values):
        if len(v) < 1:
            raise ValueError("group_by must contain at least one field name.")

        return v


class JoinedTimeseriesQuery(BaseModel):
    primary_filepath: Union[str, Path]
    secondary_filepath: Union[str, Path]
    crosswalk_filepath: Union[str, Path]
    order_by: List[MetricFilterFieldEnum]
    filters: List[Filter] = []
    return_query: bool
    geometry_filepath: Optional[Union[str, Path]]
    include_geometry: bool

    @validator('include_geometry')
    def include_geometry_must_group_by_primary_location_id(cls, v, values):
        if v is True and not values["geometry_filepath"]:
            raise ValueError(
                "`geometry_filepath` must be provided to include geometry "
                "in returned data"
            )

        return v

    @validator('order_by')
    def must_include_one_order_by(cls, v, values):
        if len(v) < 1:
            raise ValueError("order_by must contain at least one field name.")

        return v
