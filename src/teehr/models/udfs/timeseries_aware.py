"""Classes representing UDFs."""
from typing import List, Union
from pydantic import Field
import pandas as pd
import pyspark.sql.types as T
import pyspark.sql as ps

from teehr.models.udfs.udf_base import UDF_ABC, UDFBasemodel


class PercentileEventDetection(UDF_ABC, UDFBasemodel):
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


class TimeseriesAwareUDF():
    """Class to hold timeseries aware UDFs.

    Timeseries aware UDFs are applied to each row in a DataFrame
    and are aware of the timeseries data.  These can be used for
    event detection, etc.
    """
    PercentileEventDetection = PercentileEventDetection