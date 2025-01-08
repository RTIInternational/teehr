"""Classes representing UDFs."""
from typing import List, Union
from pydantic import Field
import pandas as pd
import pyspark.sql.types as T
import pyspark.sql as ps
from teehr.models.calculated_fields.base import CalculatedFieldABC, CalculatedFieldBaseModel

class PercentileEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds an "event" and "event_id" column to the DataFrame based on a percentile threshold.

    The "event" column (bool) indicates whether the value is above the XXth percentile.
    The "event_id" column (string) groups continuous segments of events and assigns a
    unique ID to each segment in the format "startdate-enddate".

    Properties
    ----------
    - quantile:
        The percentile threshold to use for event detection.
        Default: 0.85
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to detect events on.
        Default: "primary_value"
    - output_event_field_name:
        The name of the column to store the event detection.
        Default: "event"
    - output_event_id_field_name:
        The name of the column to store the event ID.
        Default: "event_id"
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """
    quantile: float = Field(
        default=0.85
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_event_field_name: str = Field(
        default="event"
    )
    output_event_id_field_name: str = Field(
        default="event_id"
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=[
            'reference_time',
            'primary_location_id',
            'configuration_name',
            'variable_name',
            'unit_name'
        ]
    )

    @staticmethod
    def add_is_event(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        quantile,
        group_by,
        return_type=T.BooleanType()

    ):
        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def is_event(pdf, input_field, quantile, output_field) -> pd.DataFrame:
            pvs = pdf[input_field]

            # Calculate the XXth percentile
            percentile = pvs.quantile(quantile)

            # Create a new column indicating whether each value is above the XXth percentile
            pdf[output_field] = pvs > percentile

            return pdf

        def wrapper(pdf, input_field, quantile, output_field):
            return is_event(pdf, input_field, quantile, output_field)

        # Group the data and apply the UDF
        # lambda pdf: wrapper_function(pdf, threshold_value)
        sdf = sdf.groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, quantile, output_field),
            schema=output_schema
        )

        return sdf

    @staticmethod
    def add_event_ids(
        sdf,
        output_field,
        input_field,
        time_field,
        group_by,
        return_type=T.StringType(),

    ):
        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def event_ids(pdf: pd.DataFrame, input_field, time_field, output_field) -> pd.DataFrame:
            # Create a new column for continuous segments
            pdf['segment'] = (pdf[input_field] != pdf[input_field].shift()).cumsum()

            # Filter only the segments where values are over the 90th percentile
            segments = pdf[pdf[input_field]]

            # Group by segment and create startdate-enddate string
            segment_ranges = segments.groupby('segment').agg(
                startdate=(time_field, 'min'),
                enddate=(time_field, 'max')
            ).reset_index()

            # Merge the segment ranges back to the original DataFrame
            pdf = pdf.merge(segment_ranges[['segment', 'startdate', 'enddate']], on='segment', how='left')

            # Create the startdate-enddate string column
            pdf[output_field] = pdf.apply(
                lambda row: f"{row['startdate']}-{row['enddate']}" if pd.notnull(row['startdate']) else None,
                axis=1
            )

            # Drop the 'segment', 'startdate', and 'enddate' columns before returning
            pdf.drop(columns=['segment', 'startdate', 'enddate'], inplace=True)

            return pdf

        def wrapper(pdf, input_field, time_field, output_field):
            return event_ids(pdf, input_field, time_field, output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_is_event(
            sdf=sdf,
            input_field=self.value_field_name,
            quantile=self.quantile,
            output_field=self.output_event_field_name,
            group_by=self.uniqueness_fields
        )
        sdf = self.add_event_ids(
            sdf=sdf,
            input_field=self.output_event_field_name,
            time_field=self.value_time_field_name,
            output_field=self.output_event_id_field_name,
            group_by=self.uniqueness_fields
        )

        return sdf


class TimeseriesAwareCalculatedFields():
    """Timeseries aware calculated fields.

    Notes
    -----
    Timeseries aware CFs are aware of ordered groups of data (e.g., a timeseries).
    This is useful for things such as event detection, base flow separation, and
    other fields that need to be calculated based on a entire timeseries.  The
    definition of what creates a unique set of timeseries (i.e., a timeseries) can
    be specified.

    Available Calculated Fields:

    - PercentileEventDetection
    """

    PercentileEventDetection = PercentileEventDetection