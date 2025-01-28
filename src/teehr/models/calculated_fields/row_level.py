"""Classes representing UDFs."""
from typing import List, Union
from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict
import pandas as pd
import pyspark.sql.types as T
from pyspark.sql.functions import pandas_udf
import pyspark.sql as ps
from teehr.models.calculated_fields.base import CalculatedFieldABC, CalculatedFieldBaseModel


class Month(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the month from a timestamp column.

    Properties
    ----------

    - input_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - output_field_name:
        The name of the column to store the month.
        Default: "month"
    """

    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="month"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            return col.dt.month

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class Year(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the year from a timestamp column.

    Properties
    ----------
    - input_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - output_field_name:
        The name of the column to store the year.
        Default: "year"

    """
    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            return col.dt.year

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class WaterYear(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the water year from a timestamp column.

    Properties
    ----------
    - input_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - output_field_name:
        The name of the column to store the water year.
        Default: "water_year"

    Water year is defined as the year of the date plus one if the month is October or later.
    """
    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="water_year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            return col.dt.year + (col.dt.month >= 10).astype(int)

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class NormalizedFlow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Normalize flow values by drainage area.

    Properties
    ----------
    - primary_value_field_name:
        The name of the column containing the flow values.
        Default: "primary_value"
    - drainage_area_field_name:
        The name of the column containing the drainage area.
        Default: "drainage_area"
    - output_field_name:
        The name of the column to store the normalized flow values.
        Default: "normalized_flow"

    """
    primary_value_field_name: str = Field(
        default="primary_value"
    )
    drainage_area_field_name: str = Field(
        default="drainage_area"
    )
    output_field_name: str = Field(
        default="normalized_flow"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        @pandas_udf(returnType=T.FloatType())
        def func(value: pd.Series, area: pd.Series) -> pd.Series:
            return value.astype(float) / area.astype(float)

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.primary_value_field_name, self.drainage_area_field_name)
        )
        return sdf

class Seasons(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the season from a timestamp column.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - season_months:
        A dictionary mapping season names to the months that define them.

        .. code-block:: python

            Default: {
                "winter": [12, 1, 2],
                "spring": [3, 4, 5],
                "summer": [6, 7, 8],
                "fall": [9, 10, 11]
            }

    - output_field_name:
        The name of the column to store the season.
        Default: "season"

    """
    value_time_field_name: str = Field(
        default="value_time"
    )
    season_months: dict = Field(
        default={
            "winter": [12, 1, 2],
            "spring": [3, 4, 5],
            "summer": [6, 7, 8],
            "fall": [9, 10, 11]
        }
    )
    output_field_name: str = Field(
        default="season"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        @pandas_udf(returnType=T.StringType())
        def func(value_time: pd.Series) -> pd.Series:
            return value_time.dt.month.apply(
                lambda x: next(
                    (season for season, months in self.season_months.items() if x in months),
                    None
                )
            )

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.value_time_field_name)
        )
        return sdf


class RowLevelCalculatedFields:
    """Row level Calculated Fields.

    Notes
    -----
    Row level CFs are applied to each row in the table based on data that is in one or more
    existing fields.  These are applied per row and are not aware of the data in any other
    row (e.g., are not aware of any other timeseries values in a "timeseries").  This can be
    used for adding fields such as a field based on the data/time (e.g., month, year, season, etc.)
    or based on the value field (e.g., normalized flow, log flow, etc.) and many other uses.

    Available Calculated Fields:

    - Month
    - Year
    - WaterYear
    - NormalizedFlow
    - Seasons
    """

    Month = Month
    Year = Year
    WaterYear = WaterYear
    NormalizedFlow = NormalizedFlow
    Seasons = Seasons