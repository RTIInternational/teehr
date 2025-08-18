"""Base model for Normals class."""
import abc
from typing import Optional

from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic import Field
import pyspark.sql as ps

from teehr.models.str_enum import StrEnum
# from teehr.models.fetching.utils import TimeseriesTypeEnum


class TimeseriesTableNamesEnum(StrEnum):
    """Table names for timeseries."""

    primary_timeseries = "primary_timeseries"
    secondary_timeseries = "secondary_timeseries"


class TimeseriesFilter(PydanticBaseModel, validate_assignment=True):
    """A class for uniquely identifying a timeseries from an evaluation."""

    configuration_name: Optional[str] = Field(default=None)
    variable_name: Optional[str] = Field(default=None)
    unit_name: Optional[str] = Field(default=None)
    table_name: TimeseriesTableNamesEnum = Field(
        default=TimeseriesTableNamesEnum.primary_timeseries
    )
    member: Optional[str] = Field(default=None)
    # TODO: Include all table fields?
    # location_id, value, value_time, reference_time

    def to_query(self) -> str:
        """Generate an SQL query to select the timeseries."""
        query = f"SELECT * FROM {self.table_name}"
        for i, (field_name, field_value) in enumerate(
            self.model_dump(exclude_none=True, exclude={"table_name"}).items()
        ):
            if i == 0:
                query += f" WHERE {field_name} = '{field_value}'"
            else:
                query += f" AND {field_name} = '{field_value}'"
        return query


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


class ReferenceForecastBaseModel(PydanticBaseModel):
    """Base model for reference forecast generator classes."""

    temporal_resolution: NormalsResolutionEnum = Field(default=None)
    reference_tsm: TimeseriesFilter = Field(default=None)
    template_tsm: TimeseriesFilter = Field(default=None)
    output_tsm: TimeseriesFilter = Field(default=None)
    aggregate_reference_timeseries: bool = Field(default=False)
    aggregation_time_window: str = Field(default=None)
    df: ps.DataFrame = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )


class SignatureTimeseriesBaseModel(PydanticBaseModel):
    """Base model for summary timeseries generator classes.

    Notes
    -----
    These can handle configurations with multiple unit_names and
    variable_names, since there is grouping within the method.
    They assume reference_time is None (should they?).
    """

    temporal_resolution: NormalsResolutionEnum = Field(default=None)
    summary_statistic: NormalsStatisticEnum = Field(default=None)
    df: ps.DataFrame = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )
