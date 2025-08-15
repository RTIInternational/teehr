"""Base model for Normals class."""
import abc

from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic import Field
from teehr.models.str_enum import StrEnum
from teehr.models.fetching.utils import TimeseriesTypeEnum


class GeneratorABC(abc.ABC):
    """Abstract base class for generating synthetic timeseries."""

    @abc.abstractmethod
    def generate(self):
        """Generate synthetic timeseries."""
        pass


class TimeseriesGeneratorBaseModel(PydanticBaseModel):
    """Synthetic timeseries generator base model configuration."""

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

    def to_query(self) -> str:
        """Generate a SQL query to select the timeseries."""
        return f"""
            SELECT * FROM {self.timeseries_type}_timeseries
            WHERE configuration_name = '{self.configuration_name}'
            AND variable_name = '{self.variable_name}'
            AND unit_name = '{self.unit_name}'
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