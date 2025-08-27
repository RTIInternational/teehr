"""Base model for Normals class."""
import abc
from typing import Union, List

from teehr.models.filters import FilterBaseModel

from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic import Field

from teehr.models.str_enum import StrEnum


class TimeseriesTableNamesEnum(StrEnum):
    """Table names for timeseries."""

    primary_timeseries = "primary_timeseries"
    secondary_timeseries = "secondary_timeseries"


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


class GeneratorABC(abc.ABC):
    """Abstract base class for generating synthetic timeseries."""

    @abc.abstractmethod
    def generate(self):
        """Generate synthetic timeseries."""
        pass


class BenchmarkGeneratorBaseModel(PydanticBaseModel):
    """Base model for benchmark forecast generator classes."""

    aggregate_reference_timesteps: bool = Field(default=False)
    aggregation_time_window: str = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )


class SignatureGeneratorBaseModel(PydanticBaseModel):
    """Base model for summary timeseries generator classes.

    Notes
    -----
    These can handle configurations with multiple unit_names and
    variable_names, since there is grouping within the method.
    They assume reference_time is None (should they?).
    """

    table_name: TimeseriesTableNamesEnum = Field(
        default=TimeseriesTableNamesEnum.primary_timeseries
    )
    filters: Union[
        str, dict, FilterBaseModel,
        List[Union[str, dict, FilterBaseModel]]
    ] = Field(default=None)
    temporal_resolution: NormalsResolutionEnum = Field(default=None)
    summary_statistic: NormalsStatisticEnum = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )
