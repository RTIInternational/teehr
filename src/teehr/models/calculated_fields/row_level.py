"""Classes representing UDFs."""
import calendar
from typing import Union
from pydantic import Field
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
                    (season for season,
                     months in self.season_months.items() if x in months),
                    None
                )
            )

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.value_time_field_name)
        )
        return sdf


class ForecastLeadTime(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds the forecast lead time from a timestamp column.

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


class ForecastLeadTimeBins(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds ID for grouped forecast lead time bins.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - reference_time_field_name:
        The name of the column containing the forecast time.
        Default: "reference_time"
    - lead_time_field_name:
        The name of the column containing the forecast lead time.
        Default: "forecast_lead_time"
    - output_field_name:
        The name of the column to store the lead time bin ID.
        Default: "forecast_lead_time_bin"
    - bin_size:
        Either a single pd.Timedelta for uniform bin sizes, or a dict mapping
        threshold pd.Timedelta values to pd.Timedelta bin sizes. Keys
        represent the upper bound of each threshold range. Keys can also be
        string or pd.Timestamp values, which will be converted to pd.Timedelta
        relative to the minimum value_time in the DataFrame.
        Default: 5 days

        Example dict 1: {
            pd.Timedelta(days=1): pd.Timedelta(hours=6),
            pd.Timedelta(days=2): pd.Timedelta(hours=12),
            pd.Timedelta(days=3): pd.Timedelta(days=1)
        }

        Example dict 2: {
            "2024-11-20 12:00:00": pd.Timedelta(hours=6),
            "2024-11-21 12:00:00": pd.Timedelta(hours=12),
            "2024-11-22 12:00:00": pd.Timedelta(days=1)
        }

        Example dict 3: {
            pd.Timestamp("2024-11-20 12:00:00"): pd.Timedelta(hours=6),
            pd.Timestamp("2024-11-21 12:00:00"): pd.Timedelta(hours=12),
            pd.Timestamp("2024-11-22 12:00:00"): pd.Timedelta(days=1)
        }
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    reference_time_field_name: str = Field(
        default="reference_time"
    )
    lead_time_field_name: str = Field(
        default="forecast_lead_time"
    )
    output_field_name: str = Field(
        default="forecast_lead_time_bin"
    )
    bin_size: Union[pd.Timedelta, dict] = Field(
        default=pd.Timedelta(days=5)
    )

    @ staticmethod
    def _validate_bin_size_dict(
            self, sdf: ps.DataFrame) -> Union[pd.Timedelta, dict]:
        """Validate and normalize bin_size dict.

        Validates that bin_size dict keys are of correct, uniform type and
        converts them to pd.Timedelta if needed. Validates that bin_size dict
        values are of type pd.Timedelta. Validates that dict is not empty.

        If bin_size is already a pd.Timedelta or dict with pd.Timedelta keys,
        returns it unchanged. Otherwise, converts string/datetime keys to
        pd.Timedelta by calculating the difference from the minimum value_time.
        """
        # If not a dict or already has Timedelta keys, return as-is
        if not isinstance(self.bin_size, dict) and isinstance(
            self.bin_size, pd.Timedelta
        ):
            return self.bin_size

        # raise error if dict is empty
        if not self.bin_size:
            raise ValueError("bin_size dict cannot be empty")

        # Ensure all keys are of the same type
        first_key = next(iter(self.bin_size.keys()))
        for key in self.bin_size.keys():
            if not isinstance(key, type(first_key)):
                raise TypeError(
                    "All bin_size dict keys must be of the same type"
                )

        # Ensure all values are pd.Timedelta
        for value in self.bin_size.values():
            if not isinstance(value, pd.Timedelta):
                raise TypeError(
                    "All bin_size dict values must be of type pd.Timedelta"
                )

        # If already Timedelta keys, return as-is
        if isinstance(first_key, pd.Timedelta):
            return self.bin_size

        # Convert string or datetime keys to Timedelta
        if isinstance(first_key, (str, pd.Timestamp)):
            # Get minimum value_time from the dataframe
            min_value_time_row = sdf.select(
                self.value_time_field_name
            ).orderBy(self.value_time_field_name).first()

            # Extract value_time and convert to datetime
            min_value_time = pd.to_datetime(
                min_value_time_row[self.value_time_field_name]
            )

            # Convert dict keys from string/datetime to Timedelta
            converted_dict = {}
            for key, value in self.bin_size.items():
                # Convert key to datetime if it's a string
                if isinstance(key, str):
                    try:
                        key_datetime = pd.to_datetime(key)
                    except Exception as e:
                        raise ValueError(
                            f"Error converting key '{key}' to datetime: {e}"
                            )
                else:
                    key_datetime = key

                # Calculate Timedelta from min_value_time
                timedelta_key = key_datetime - min_value_time
                converted_dict[timedelta_key] = value

            return converted_dict

        else:
            # raise error for unsupported key types
            raise TypeError(
                "bin_size dict keys must be pd.Timedelta, str, or pd.Timestamp"
            )

    @staticmethod
    def _add_forecast_lead_time(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Calculate forecast lead time if not already present."""
        if self.lead_time_field_name not in sdf.columns:
            flt_cf = ForecastLeadTime(
                value_time_field_name=self.value_time_field_name,
                reference_time_field_name=self.reference_time_field_name,
                output_field_name=self.lead_time_field_name
            )
            sdf = flt_cf.apply_to(sdf)
        return sdf

    @staticmethod
    def _add_forecast_lead_time_bin(
        self,
        sdf: ps.DataFrame
    ) -> ps.DataFrame:
        """Add forecast lead time bin column."""

        @pandas_udf(returnType=T.StringType())
        def func(lead_time: pd.Series, value_time: pd.Series) -> pd.Series:
            if isinstance(self.bin_size, dict):
                # Sort thresholds for consistent processing
                sorted_thresholds = sorted(self.bin_size.items(),
                                           key=lambda x: x[0].total_seconds())

                bin_ids = pd.Series("", index=lead_time.index)
                prev_thresh_sec = 0

                for i, (threshold, bin_size) in enumerate(sorted_thresholds):
                    threshold_seconds = threshold.total_seconds()
                    bin_size_seconds = bin_size.total_seconds()

                    is_last = (i == len(sorted_thresholds) - 1)

                    # Mask for values in this threshold range
                    if is_last:
                        mask = lead_time.dt.total_seconds() >= prev_thresh_sec
                    else:
                        mask = (
                            lead_time.dt.total_seconds() >= prev_thresh_sec
                        ) & (
                            lead_time.dt.total_seconds() < threshold_seconds
                        )

                    if mask.any():
                        # Calculate which bin each value belongs to
                        bins_in_range = (
                            (lead_time[mask].dt.total_seconds() -
                             prev_thresh_sec)
                            // bin_size_seconds
                        ).astype(int)

                        # create bin ID from actual timestamps
                        for bin_num in bins_in_range.unique():
                            bin_mask = mask & (bins_in_range == bin_num)

                            if bin_mask.any():
                                # Get the unique value_time values for this bin
                                bin_value_times = value_time[bin_mask].unique()

                                # Convert to datetime and find min/max
                                bin_value_times_dt = pd.to_datetime(
                                    bin_value_times
                                    )
                                start_timestamp = bin_value_times_dt.min()
                                end_timestamp = start_timestamp + bin_size

                                # Format as ISO 8601 timestamp range
                                bin_id = f"{start_timestamp} - {end_timestamp}"
                                bin_ids[bin_mask] = bin_id

                    prev_thresh_sec = threshold_seconds

                return bin_ids

            else:
                # Uniform bin size logic
                bin_size_seconds = self.bin_size.total_seconds()

                # Calculate which bin each value belongs to
                bin_numbers = (
                    lead_time.dt.total_seconds() // bin_size_seconds
                ).astype(int)

                bin_ids = pd.Series("", index=lead_time.index)

                # For each unique bin, create bin ID from actual timestamps
                for bin_num in bin_numbers.unique():
                    bin_mask = bin_numbers == bin_num

                    if bin_mask.any():
                        # Get the unique value_time values for this bin
                        bin_value_times = value_time[bin_mask].unique()

                        # Convert to datetime if needed and find min/max
                        bin_value_times_dt = pd.to_datetime(bin_value_times)
                        start_timestamp = bin_value_times_dt.min()
                        end_timestamp = start_timestamp + self.bin_size

                        # Format as ISO 8601 timestamp range
                        bin_id = f"{start_timestamp} - {end_timestamp}"
                        bin_ids[bin_mask] = bin_id

                return bin_ids

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.lead_time_field_name, self.value_time_field_name)
        )
        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        self.bin_size = self._validate_bin_size_dict(self, sdf)
        sdf = self._add_forecast_lead_time(self, sdf)
        sdf = self._add_forecast_lead_time_bin(self, sdf)
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
    - ForecastLeadTimeBins
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
    ForecastLeadTimeBins = ForecastLeadTimeBins
    ThresholdValueExceeded = ThresholdValueExceeded
    ThresholdValueNotExceeded = ThresholdValueNotExceeded
    DayOfYear = DayOfYear
    HourOfYear = HourOfYear
