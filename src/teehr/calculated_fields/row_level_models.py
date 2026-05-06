"""Classes representing UDFs."""
from typing import Union
from pydantic import Field
import pandas as pd
from datetime import timedelta
import pyspark.sql as ps
from teehr.calculated_fields.base_models import CalculatedFieldABC
from teehr.calculated_fields.base_models import CalculatedFieldBaseModel
from teehr.calculated_fields.row_level_pandas_spark import apply_day_of_year
from teehr.calculated_fields.row_level_pandas_spark import apply_forecast_lead_time
from teehr.calculated_fields.row_level_pandas_spark import apply_forecast_lead_time_bins
from teehr.calculated_fields.row_level_pandas_spark import apply_generic_sql
from teehr.calculated_fields.row_level_pandas_spark import apply_hour_of_year
from teehr.calculated_fields.row_level_pandas_spark import apply_month
from teehr.calculated_fields.row_level_pandas_spark import apply_normalized_flow
from teehr.calculated_fields.row_level_pandas_spark import apply_seasons
from teehr.calculated_fields.row_level_pandas_spark import apply_threshold_value_exceeded
from teehr.calculated_fields.row_level_pandas_spark import apply_threshold_value_not_exceeded
from teehr.calculated_fields.row_level_pandas_spark import apply_water_year
from teehr.calculated_fields.row_level_pandas_spark import apply_year


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
        return apply_month(
            sdf,
            input_field_name=self.input_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_year(
            sdf,
            input_field_name=self.input_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_water_year(
            sdf,
            input_field_name=self.input_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_normalized_flow(
            sdf,
            primary_value_field_name=self.primary_value_field_name,
            drainage_area_field_name=self.drainage_area_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_seasons(
            sdf,
            value_time_field_name=self.value_time_field_name,
            season_months=self.season_months,
            output_field_name=self.output_field_name,
        )


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
        return apply_forecast_lead_time(
            sdf,
            value_time_field_name=self.value_time_field_name,
            reference_time_field_name=self.reference_time_field_name,
            output_field_name=self.output_field_name,
        )


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
        Defines how forecast lead times are binned. Accepts pd.Timedelta,
        datetime.timedelta, or timedelta strings (e.g., '6 hours', '1 day').
        Three input formats are supported:

        1. **Single timedelta** (uniform binning):
           Creates equal-width bins of the specified duration.

           Examples:
               pd.Timedelta(hours=6)
               timedelta(hours=6)
               '6 hours'
               '6h'

           Output bin IDs:
               "PT0H_PT6H", "PT6H_PT12H", "PT12H_PT18H", ...

        2. **List of dicts** (variable binning with auto-generated IDs):
           Creates bins with custom ranges. Bin IDs are auto-generated as
           ISO 8601 duration ranges. Values can be pd.Timedelta,
           datetime.timedelta, or timedelta strings.

           Examples:
               [
                   {'start_inclusive': pd.Timedelta(hours=0),
                   'end_exclusive': pd.Timedelta(hours=6)},
                   {'start_inclusive': '6 hours',
                   'end_exclusive': '12 hours'},
                   {'start_inclusive': timedelta(hours=12),
                   'end_exclusive': '1 day'},
                   {'start_inclusive': '1 day',
                   'end_exclusive': '2 days'},
               ]

           Output bin IDs:
               "PT0H_PT6H", "PT6H_PT12H", "PT12H_P1D", "P1D_P2D"

        3. **Dict of dicts** (variable binning with custom IDs):
           Creates bins with custom ranges and user-defined bin identifiers.
           Values can be pd.Timedelta, datetime.timedelta, or timedelta
           strings.

           Examples:
               {
                   'short_range': {'start_inclusive': '0 hours',
                                   'end_exclusive': '6 hours'},
                   'medium_range': {'start_inclusive': pd.Timedelta(hours=6),
                                    'end_exclusive': timedelta(days=1)},
                   'long_range': {'start_inclusive': '1 day',
                                  'end_exclusive': '3 days'},
               }

           Output bin IDs:
               "short_range", "medium_range", "long_range"

        Default: pd.Timedelta(days=5)

    Notes
    -----
    - Timedelta values can be specified as:
      - pd.Timedelta objects (e.g., pd.Timedelta(hours=6))
      - datetime.timedelta objects (e.g., timedelta(hours=6))
      - Strings (e.g., '6 hours', '1 day', '1d 12h', 'PT6H')
    - All timedelta inputs are internally converted to pd.Timedelta for
      processing.
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
    Uniform 6-hour bins using different input types:

    .. code-block:: python

        # Using pd.Timedelta
        fcst_bins = ForecastLeadTimeBins(bin_size=pd.Timedelta(hours=6))

        # Using datetime.timedelta
        from datetime import timedelta
        fcst_bins = ForecastLeadTimeBins(bin_size=timedelta(hours=6))

        # Using string
        fcst_bins = ForecastLeadTimeBins(bin_size='6 hours')

        # All create bins: PT0H_PT6H, PT6H_PT12H, PT12H_PT18H, ...

    Variable bins with auto-generated IDs using mixed types:

    .. code-block:: python

        fcst_bins = ForecastLeadTimeBins(
            bin_size=[
                {'start_inclusive': '0 hours',
                'end_exclusive': '6 hours'},
                {'start_inclusive': pd.Timedelta(hours=6),
                'end_exclusive': '1 day'},
                {'start_inclusive': timedelta(days=1),
                'end_exclusive': '3 days'},
            ]
        )
        # Creates bins: PT0H_PT6H, PT6H_P1D, P1D_P3D

    Variable bins with custom IDs using strings:

    .. code-block:: python

        fcst_bins = ForecastLeadTimeBins(
            bin_size={
                'nowcast': {'start_inclusive': '0 hours',
                'end_exclusive': '6 hours'},
                'short_term': {'start_inclusive': '6 hours',
                'end_exclusive': '1 day'},
                'medium_term': {'start_inclusive': '1 day',
                'end_exclusive': '5 days'},
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
    bin_size: Union[pd.Timedelta, timedelta, str, list, dict] = Field(
        default=pd.Timedelta(days=5)
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        return apply_forecast_lead_time_bins(
            sdf,
            value_time_field_name=self.value_time_field_name,
            reference_time_field_name=self.reference_time_field_name,
            lead_time_field_name=self.lead_time_field_name,
            output_field_name=self.output_field_name,
            bin_size=self.bin_size,
        )


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
        return apply_threshold_value_exceeded(
            sdf,
            input_field_name=self.input_field_name,
            threshold_field_name=self.threshold_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_threshold_value_not_exceeded(
            sdf,
            input_field_name=self.input_field_name,
            threshold_field_name=self.threshold_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_day_of_year(
            sdf,
            input_field_name=self.input_field_name,
            output_field_name=self.output_field_name,
        )


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
        return apply_hour_of_year(
            sdf,
            input_field_name=self.input_field_name,
            output_field_name=self.output_field_name,
        )


class GenericSQL(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds a column computed from a SQL expression.

    Properties
    ----------
    - output_field_name:
        The name of the column to add.
    - sql_statement:
        A SQL expression string that will be evaluated using Spark's
        ``expr()`` function.  The expression may reference any column
        that exists in the current DataFrame.

    Examples
    --------
    .. code-block:: python

        from teehr import RowLevelCalculatedFields as rcf

        ev.joined_timeseries.add_calculated_fields([
            rcf.GenericSQL(
                output_field_name="log_primary_value",
                sql_statement="log(primary_value)"
            )
        ]).write()

    """

    output_field_name: str = Field(...)
    sql_statement: str = Field(...)

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        """Apply the calculated field to the Spark DataFrame."""
        return apply_generic_sql(
            sdf,
            output_field_name=self.output_field_name,
            sql_statement=self.sql_statement,
        )


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
    - GenericSQL

    Examples
    --------
    Add row level calculated fields to the joined timeseries table and write
    to the warehouse.

    >>> import teehr
    >>> from teehr import RowLevelCalculatedFields as rcf

    >>> ev.joined_timeseries.add_calculated_fields([
    ...    rcf.Month(),
    ...    rcf.Year(),
    ...    rcf.WaterYear(),
    ...    rcf.Seasons()
    ... ]).write()

    We can also use these calculated fields in metrics calculations.

    >>> ev.metrics(table_name="joined_timeseries").add_calculated_fields([
    ...     rcf.Month()
    ... ]).aggregate(
    ...     metrics=[fdc],
    ...     group_by=[flds.primary_location_id, "month"],
    ... ).order_by([flds.primary_location_id, "month"]).to_pandas()
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
    GenericSQL = GenericSQL
