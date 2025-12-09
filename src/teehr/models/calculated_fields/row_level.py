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
        Defines how forecast lead times are binned. Three input formats are
        supported:

        1. **Single pd.Timedelta** (uniform binning):
           Creates equal-width bins of the specified duration.

           Example:
               pd.Timedelta(hours=6)

           Output bin IDs:
               "PT0H_PT6H", "PT6H_PT12H", "PT12H_PT18H", ...

        2. **List of dicts** (variable binning with auto-generated IDs):
           Creates bins with custom ranges. Bin IDs are auto-generated as
           ISO 8601 duration ranges.

           Example:
               [
                   {'start_inclusive': pd.Timedelta(hours=0),
                   'end_exclusive': pd.Timedelta(hours=6)},
                   {'start_inclusive': pd.Timedelta(hours=6),
                   'end_exclusive': pd.Timedelta(hours=12)},
                   {'start_inclusive': pd.Timedelta(hours=12),
                   'end_exclusive': pd.Timedelta(days=1)},
                   {'start_inclusive': pd.Timedelta(days=1),
                   'end_exclusive': pd.Timedelta(days=2)},
               ]

           Output bin IDs:
               "PT0H_PT6H", "PT6H_PT12H", "PT12H_P1D", "P1D_P2D"

        3. **Dict of dicts** (variable binning with custom IDs):
           Creates bins with custom ranges and user-defined bin identifiers.

           Example:
               {
                   'short_range': {'start_inclusive': pd.Timedelta(hours=0),
                                   'end_exclusive': pd.Timedelta(hours=6)},
                   'medium_range': {'start_inclusive': pd.Timedelta(hours=6),
                                    'end_exclusive': pd.Timedelta(days=1)},
                   'long_range': {'start_inclusive': pd.Timedelta(days=1),
                                  'end_exclusive': pd.Timedelta(days=3)},
               }

           Output bin IDs:
               "short_range", "medium_range", "long_range"

        Default: pd.Timedelta(days=5)

    Notes
    -----
    - Bin ranges are [start_inclusive, end_exclusive), except for the final
      bin which is inclusive of all remaining lead times.
    - If the maximum lead time in the data exceeds the last user-defined bin,
      an overflow bin is automatically created:
      - For auto-generated IDs: Uses ISO 8601 duration format
      - For custom IDs: Appends "overflow" as the bin ID
    - Bin IDs use ISO 8601 duration format (e.g., "PT6H" for 6 hours, "P1DT12H"
      for 1 day and 12 hours) for auto-generated bins.
    - Custom bin IDs can use any string format.

    Examples
    --------
    Uniform 6-hour bins:

    .. code-block:: python

        fcst_bins = ForecastLeadTimeBins(bin_size=pd.Timedelta(hours=6))
        # Creates bins: PT0H_PT6H, PT6H_PT12H, PT12H_PT18H, ...

    Variable bins with auto-generated IDs:

    .. code-block:: python

        fcst_bins = ForecastLeadTimeBins(
            bin_size=[
                {'start_inclusive': pd.Timedelta(hours=0),
                'end_exclusive': pd.Timedelta(hours=6)},
                {'start_inclusive': pd.Timedelta(hours=6),
                'end_exclusive': pd.Timedelta(days=1)},
                {'start_inclusive': pd.Timedelta(days=1),
                'end_exclusive': pd.Timedelta(days=3)},
            ]
        )
        # Creates bins: PT0H_PT6H, PT6H_P1D, P1D_P3D

    Variable bins with custom IDs:

    .. code-block:: python

        fcst_bins = ForecastLeadTimeBins(
            bin_size={
                'nowcast': {'start_inclusive': pd.Timedelta(hours=0),
                            'end_exclusive': pd.Timedelta(hours=6)},
                'short_term': {'start_inclusive': pd.Timedelta(hours=6),
                               'end_exclusive': pd.Timedelta(days=1)},
                'medium_term': {'start_inclusive': pd.Timedelta(days=1),
                                'end_exclusive': pd.Timedelta(days=5)},
            }
        )
        # Creates bins: nowcast, short_term, medium_term
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
    bin_size: Union[pd.Timedelta, list, dict] = Field(
        default=pd.Timedelta(days=5)
    )

    @staticmethod
    def _validate_bin_size_dict(self) -> Union[pd.Timedelta, list, dict]:
        """Validate and normalize bin_size input.

        Validates and converts bin_size to a standardized format:
        - Single pd.Timedelta: returns as-is
        - List of dicts: validates structure and converts to internal format
        - Dict of dicts: validates structure and keeps custom bin IDs

        Returns a normalized structure for internal processing.
        """
        # Single Timedelta - return as-is
        if isinstance(self.bin_size, pd.Timedelta):
            return self.bin_size

        # List of dicts format
        if isinstance(self.bin_size, list):
            if not self.bin_size:
                raise ValueError("bin_size list cannot be empty")

            # Validate each dict has required keys
            for i, bin_dict in enumerate(self.bin_size):
                if not isinstance(bin_dict, dict):
                    raise TypeError(
                        f"Item {i} in bin_size list must be a dict"
                        )

                required_keys = {'start_inclusive', 'end_exclusive'}
                if not required_keys.issubset(bin_dict.keys()):
                    raise ValueError(
                        f"Item {i} missing required keys: {required_keys}"
                    )

                # Validate that values are Timedelta
                if not isinstance(bin_dict['start_inclusive'], pd.Timedelta):
                    raise TypeError(
                        f"Item {i} 'start_inclusive' must be pd.Timedelta"
                    )
                if not isinstance(bin_dict['end_exclusive'], pd.Timedelta):
                    raise TypeError(
                        f"Item {i} 'end_exclusive' must be pd.Timedelta"
                    )

            # Convert to internal format: list of tuples (start, end, bin_id)
            # For list format, bin_id is None
            normalized = []
            for bin_dict in self.bin_size:
                start = bin_dict['start_inclusive']
                end = bin_dict['end_exclusive']
                normalized.append((start, end, None))

            return normalized

        # Dict of dicts format
        if isinstance(self.bin_size, dict):
            if not self.bin_size:
                raise ValueError("bin_size dict cannot be empty")

            # Validate structure
            for key, value in self.bin_size.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Dict keys must be strings (custom bin IDs), got \
                          {type(key)}"
                    )

                if not isinstance(value, dict):
                    raise TypeError(
                        "Dict values must be dicts with bin specification"
                    )

                required_keys = {'start_inclusive', 'end_exclusive'}
                if not required_keys.issubset(value.keys()):
                    raise ValueError(
                        f"Bin '{key}' missing required keys. Must have: \
                          {required_keys}"
                    )

                if not isinstance(value['start_inclusive'], pd.Timedelta):
                    raise TypeError(
                        f"Bin '{key}' 'start_inclusive' must be pd.Timedelta"
                    )
                if not isinstance(value['end_exclusive'], pd.Timedelta):
                    raise TypeError(
                        f"Bin '{key}' 'end_exclusive' must be pd.Timedelta"
                    )

            # Convert to internal format: list of tuples
            normalized = []
            for custom_id, bin_dict in self.bin_size.items():
                start = bin_dict['start_inclusive']
                end = bin_dict['end_exclusive']
                normalized.append((start, end, custom_id))

            return normalized

        raise TypeError(
            "bin_size must be pd.Timedelta, list of dicts, or dict of dicts"
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

        def _timedelta_to_iso_duration(td: pd.Timedelta) -> str:
            """Convert pd.Timedelta to ISO 8601 duration string."""
            iso_str = td.isoformat()
            # Remove trailing 0M0S, 0S, etc. for cleaner output
            iso_str = iso_str.replace(
                '0M0S', '').replace(
                '0S', '').replace(
                '0M', '')
            # Handle edge case where we removed everything after 'T'
            if iso_str.endswith('T'):
                iso_str = iso_str[:-1] + 'T0S'
            return iso_str

        @pandas_udf(returnType=T.StringType())
        def func(lead_time: pd.Series) -> pd.Series:
            # Single Timedelta - uniform binning
            if isinstance(self.bin_size, pd.Timedelta):
                bin_size_seconds = self.bin_size.total_seconds()

                bin_numbers = (
                    lead_time.dt.total_seconds() // bin_size_seconds
                ).astype(int)

                bin_ids = pd.Series("", index=lead_time.index)

                for bin_num in bin_numbers.unique():
                    bin_mask = bin_numbers == bin_num

                    if bin_mask.any():
                        start_td = pd.Timedelta(
                            seconds=bin_num * bin_size_seconds
                            )
                        end_td = pd.Timedelta(
                            seconds=(bin_num + 1) * bin_size_seconds
                            )

                        # Convert to ISO duration format
                        start_iso = _timedelta_to_iso_duration(start_td)
                        end_iso = _timedelta_to_iso_duration(end_td)
                        bin_id = f"{start_iso}_{end_iso}"

                        bin_ids[bin_mask] = bin_id

                return bin_ids

            # List/Dict format - dynamic binning with explicit ranges
            # self.bin_size is now a list of tuples: (start, end, bin_id)
            # bin_id is None for auto-generated, or a string for custom
            else:
                bin_ids = pd.Series("", index=lead_time.index)
                lead_time_seconds = lead_time.dt.total_seconds()

                # Check if we need to add an overflow bin
                max_lead_time = lead_time.max()
                last_bin_end = self.bin_size[-1][1]

                # Create working copy of bin_size
                bins_to_use = []

                # Convert all bins, generating ISO format for None bin_ids
                for start_td, end_td, bin_id in self.bin_size:
                    if bin_id is None:
                        # Auto-generated: create ISO duration format
                        start_iso = _timedelta_to_iso_duration(start_td)
                        end_iso = _timedelta_to_iso_duration(end_td)
                        final_bin_id = f"{start_iso}_{end_iso}"
                    else:
                        # Custom ID: use as-is
                        final_bin_id = bin_id

                    bins_to_use.append((start_td, end_td, final_bin_id))

                # If max lead time exceeds last bin, create overflow bin
                if max_lead_time >= last_bin_end:
                    overflow_start = last_bin_end
                    overflow_end = max_lead_time

                    # Determine overflow bin_id
                    if self.bin_size[-1][2] is None:
                        # Auto-generated format: use ISO duration strings
                        start_iso = _timedelta_to_iso_duration(overflow_start)
                        end_iso = _timedelta_to_iso_duration(overflow_end)
                        overflow_bin_id = f"{start_iso}_{end_iso}"
                    else:
                        # Custom ID format: append suffix
                        overflow_bin_id = "overflow"

                    bins_to_use.append(
                        (overflow_start, overflow_end, overflow_bin_id)
                        )

                for i, (start_td, end_td, bin_id) in enumerate(bins_to_use):
                    start_seconds = start_td.total_seconds()
                    end_seconds = end_td.total_seconds()

                    # Check if this is the last bin (including overflow bin)
                    is_last_bin = (i == len(bins_to_use) - 1)

                    if is_last_bin:
                        # Last bin is inclusive of end_exclusive
                        mask = lead_time_seconds >= start_seconds
                    else:
                        # All other bins are [start, end)
                        mask = (
                            (lead_time_seconds >= start_seconds) &
                            (lead_time_seconds < end_seconds)
                        )

                    if mask.any():
                        bin_ids[mask] = bin_id

                return bin_ids

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.lead_time_field_name)
        )
        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        self.bin_size = self._validate_bin_size_dict(self)
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
