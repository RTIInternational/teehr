"""Classes representing UDFs."""
import calendar
from typing import List, Union
from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict
import pandas as pd
import pyspark.sql.types as T
from pyspark.sql.functions import pandas_udf
import pyspark.sql as ps
from teehr.models.calculated_fields.base import CalculatedFieldABC
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel


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
        """Apply the calculated field to the Spark DataFrame."""
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
        """Apply the calculated field to the Spark DataFrame."""
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

    Water year is defined as the year of the date plus one if the month is
    October or later.
    """

    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="water_year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
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
        """Apply the calculated field to the Spark DataFrame."""
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
        """Apply the calculated field to the Spark DataFrame."""
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


class ForecastLeadTime(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the forecast lead time in seconds from a timestamp column.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - reference_time_field_name:
        The name of the column containing the forecast time.
        Default: "reference_time"
    - output_field_name:
        The name of the column to store the forecast lead time.
        Default: "forecast_lead_time"

    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    reference_time_field_name: str = Field(
        default="reference_time"
    )
    output_field_name: str = Field(
        default="forecast_lead_time"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        @pandas_udf(returnType=T.DayTimeIntervalType())
        def func(value_time: pd.Series,
                 reference_time: pd.Series
                 ) -> pd.Series:
            difference = value_time - reference_time
            return difference

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.value_time_field_name, self.reference_time_field_name)
        )
        return sdf


class ThresholdValueExceeded(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds boolean column indicating if the input value exceeds a threshold.

    Properties
    ----------
    - input_field_name:
        The name of the column containing the primary value.
        Default: "primary_value"
    - threshold_field_name:
        The name of the column containing the threshold value.
        Default: "secondary_value"
    - output_field_name:
        The name of the column to store the boolean value.
        Default: "threshold_value_exceeded"

    """

    input_field_name: str = Field(
        default="primary_value"
    )
    threshold_field_name: str = Field(
        default="secondary_value"
    )
    output_field_name: str = Field(
        default="threshold_value_exceeded"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        @pandas_udf(returnType=T.BooleanType())
        def func(input_value: pd.Series,
                 threshold_value: pd.Series
                 ) -> pd.Series:
            mask = input_value.astype(float) > threshold_value.astype(float)
            return mask

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name,
                 self.threshold_field_name)
        )
        return sdf


class ThresholdValueNotExceeded(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds boolean column indicating if the input value is less than or equal to a threshold.

    Properties
    ----------
    - input_field_name:
        The name of the column containing the primary value.
        Default: "primary_value"
    - threshold_field_name:
        The name of the column containing the threshold value.
        Default: "secondary_value"
    - output_field_name:
        The name of the column to store the boolean value.
        Default: "threshold_value_not_exceeded"

    """ # noqa

    input_field_name: str = Field(
        default="primary_value"
    )
    threshold_field_name: str = Field(
        default="secondary_value"
    )
    output_field_name: str = Field(
        default="threshold_value_not_exceeded"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        @pandas_udf(returnType=T.BooleanType())
        def func(input_value: pd.Series,
                 threshold_value: pd.Series
                 ) -> pd.Series:
            mask = input_value.astype(float) <= threshold_value.astype(float)
            return mask

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name,
                 self.threshold_field_name)
        )
        return sdf


class DayOfYear(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the day of the year from a timestamp column.

    Properties
    ----------
    - input_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - output_field_name:
        The name of the column to store the day of the year.
        Default: "day_of_year"

    Notes
    -----
    - February 29th in leap years is set to None.
    - All days after February 29th are adjusted to correspond to the same day
      of the year as in a non-leap year.
    """

    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="day_of_year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            def adjust_day_of_year(date):
                if calendar.isleap(date.year):
                    if date.month == 2 and date.day == 29:
                        return 59  # Assign to Feb.28 during leap years
                    elif date.month > 2:
                        return date.dayofyear - 1
                    else:
                        return date.dayofyear
                else:
                    return date.dayofyear

            return col.apply(adjust_day_of_year)

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class HourOfYear(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the hour from a timestamp column.

    Properties
    ----------

    - input_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - output_field_name:
        The name of the column to store the month.
        Default: "hour_of_year"
    """
    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="hour_of_year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            def adjust_hour_of_year(date):
                if calendar.isleap(date.year):
                    if date.month == 2 and date.day == 29:
                        # Assign to Feb.28 during leap years
                        return 58 * 24 + date.hour
                    elif date.month > 2:
                        return (date.dayofyear - 2) * 24 + date.hour
                    else:
                        return (date.dayofyear - 1) * 24 + date.hour
                else:
                    return (date.dayofyear - 1) * 24 + date.hour

            return col.apply(adjust_hour_of_year)

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class RowLevelCalculatedFields:
    """Row level Calculated Fields.

    Notes
    -----
    Row level CFs are applied to each row in the table based on data that is
    in one or more existing fields.  These are applied per row and are not
    aware of the data in any other row (e.g., are not aware of any other
    timeseries values in a "timeseries").  This can be used for adding fields
    such as a field based on the data/time (e.g., month, year, season, etc.)
    or based on the value field (e.g., normalized flow, log flow, etc.) and
    many other uses.

    Available Calculated Fields:

    - Month
    - Year
    - WaterYear
    - NormalizedFlow
    - Seasons
    - ForecastLeadTime
    - ThresholdValueExceeded
    - DayOfYear
    - HourOfYear
    """

    Month = Month
    Year = Year
    WaterYear = WaterYear
    NormalizedFlow = NormalizedFlow
    Seasons = Seasons
    ForecastLeadTime = ForecastLeadTime
    ThresholdValueExceeded = ThresholdValueExceeded
    ThresholdValueNotExceeded = ThresholdValueNotExceeded
    DayOfYear = DayOfYear
    HourOfYear = HourOfYear
