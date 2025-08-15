"""Base model for Normals class."""
import abc

from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic import Field
from teehr.models.str_enum import StrEnum
# import pyspark.sql as ps

from teehr.models.fetching.utils import TimeseriesTypeEnum
# from teehr.models.generate.base import (
#     NormalsResolutionEnum,
#     NormalsStatisticEnum,
#     TimeseriesModel
# )


class GeneratorABC(abc.ABC):
    """Abstract base class for generating synthetic timeseries."""

    @abc.abstractmethod
    def generate(self):
        """Generate synthetic timeseries."""
        pass


class ReferenceForecastBaseModel(PydanticBaseModel):
    """Base model for reference forecast generator classes."""

    # temporal_resolution: NormalsResolutionEnum = Field(default=None)
    # reference_tsm: TimeseriesModel = Field(default=None)
    # template_tsm: TimeseriesModel = Field(default=None)
    # output_tsm: TimeseriesModel = Field(default=None)
    # aggregate_reference_timeseries: bool = Field(default=False)
    # aggregation_time_window: str = Field(default=None)
    # df: ps.DataFrame = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )


class SummaryTimeseriesBaseModel(PydanticBaseModel):
    """Base model for summary timeseries generator classes."""

    # temporal_resolution: NormalsResolutionEnum = Field(default=None)
    # summary_statistic: NormalsStatisticEnum = Field(default=None)
    # input_tsm: TimeseriesModel = Field(default=None)
    # df: ps.DataFrame = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )


class TimeseriesModel(PydanticBaseModel):
    """A class for uniquely identifying a timeseries."""

    configuration_name: str = Field(default="usgs_observations")
    variable_name: str = Field(default="streamflow_hourly_inst")
    unit_name: str = Field(default="ft^3/s")
    timeseries_type: TimeseriesTypeEnum = Field(
        default=TimeseriesTypeEnum.primary
    )
    member: str = Field(default=None)

    def to_query(self) -> str:
        """Generate a SQL query to select the timeseries."""
        if self.member is None:
            return f"""
                SELECT * FROM {self.timeseries_type}_timeseries
                WHERE configuration_name = '{self.configuration_name}'
                AND variable_name = '{self.variable_name}'
                AND unit_name = '{self.unit_name}'
            ;"""
        else:
            return f"""
                SELECT * FROM {self.timeseries_type}_timeseries
                WHERE configuration_name = '{self.configuration_name}'
                AND variable_name = '{self.variable_name}'
                AND unit_name = '{self.unit_name}'
                AND member = '{self.member}'
            ;"""


class NormalsResolutionEnum(StrEnum):
    """Temporal resolutions for calculating normals."""

    hour_of_year = "hour_of_year"
    day_of_year = "day_of_year"
    month = "month"
    season = "season"
    year = "year"
    water_year = "water_year"


class NormalsStatisticEnum(StrEnum):
    """Statistics for calculating normals."""

    mean = "mean"
    median = "median"
    min = "min"
    max = "max"