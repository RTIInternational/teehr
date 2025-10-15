"""Classes representing UDFs."""
from typing import List, Union
from pydantic import Field
import pandas as pd
import numpy as np
import pyspark.sql.types as T
import pyspark.sql as ps
from scipy.stats import rankdata
from teehr.models.calculated_fields.base import CalculatedFieldABC, CalculatedFieldBaseModel

UNIQUENESS_FIELDS = [
            'reference_time',
            'primary_location_id',
            'configuration_name',
            'variable_name',
            'unit_name'
        ]


class AbovePercentileEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds "event" and "event_id" columns to the DataFrame based on a percentile threshold.

    The "event" column (bool) indicates whether the value is above the XXth
    percentile. For the "event" column, True values indicate that the
    corresponding value exceeds the specified percentile threshold, while
    False values indicate that the value is below or equal to the threshold.
    The "event_id" column (string) groups continuous segments of events and
    assigns a unique ID to each segment in the format "startdate-enddate".

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
    - output_quantile_field_name:
        The name of the column to store the quantile value.
        Default: "quantile_value"
    - add_quantile_field:
        Whether to add the quantile field.
        Default: False
    - skip_event_id:
        Whether to skip the event ID generation.
        Default: False
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
        default="event_above"
    )
    output_event_id_field_name: str = Field(
        default="event_above_id"
    )
    output_quantile_field_name: str = Field(
        default="quantile_value"
    )
    add_quantile_field: bool = Field(
        default=False
    )
    skip_event_id: bool = Field(
        default=False
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_quantile_value(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        quantile,
        group_by,
        return_type=T.DoubleType()
    ):
        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def compute_quantile(pdf, input_field, quantile, output_field) -> pd.DataFrame:
            pvs = pdf[input_field]

            # Calculate the XXth percentile
            percentile = pvs.quantile(quantile)

            # Create a new column with the XXth percentile value
            pdf[output_field] = percentile

            return pdf

        def wrapper(pdf, input_field, quantile, output_field):
            return compute_quantile(pdf, input_field, quantile, output_field)

        # Group the data and apply the UDF
        # lambda pdf: wrapper_function(pdf, threshold_value)
        sdf = sdf.groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, quantile, output_field),
            schema=output_schema
        )

        return sdf

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
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        if self.add_quantile_field:
            sdf = self.add_quantile_value(
                sdf=sdf,
                input_field=self.value_field_name,
                quantile=self.quantile,
                output_field=self.output_quantile_field_name,
                group_by=self.uniqueness_fields
            )
        sdf = self.add_is_event(
            sdf=sdf,
            input_field=self.value_field_name,
            quantile=self.quantile,
            output_field=self.output_event_field_name,
            group_by=self.uniqueness_fields
        )
        if not self.skip_event_id:
            sdf = self.add_event_ids(
                sdf=sdf,
                input_field=self.output_event_field_name,
                time_field=self.value_time_field_name,
                output_field=self.output_event_id_field_name,
                group_by=self.uniqueness_fields
            )

        return sdf


class BelowPercentileEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds "event" and "event_id" columns to the DataFrame based on a percentile threshold.

    The "event" column (bool) indicates whether the value is below the XXth
    percentile. For the "event" column, True values indicate that the
    corresponding value is below the specified percentile threshold, while
    False values indicate that the value is above or equal to the threshold.
    The "event_id" column (string) groups continuous segments of events and
    assigns a unique ID to each segment in the format "startdate-enddate".

    Properties
    ----------
    - quantile:
        The percentile threshold to use for event detection.
        Default: 0.15
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
    - output_quantile_field_name:
        The name of the column to store the quantile value.
        Default: "quantile_value"
    - add_quantile_field:
        Whether to add the quantile field.
        Default: False
    - skip_event_id:
        Whether to skip the event ID generation.
        Default: False
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
        default=0.15
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_event_field_name: str = Field(
        default="event_below"
    )
    output_event_id_field_name: str = Field(
        default="event_below_id"
    )
    output_quantile_field_name: str = Field(
        default="quantile_value"
    )
    add_quantile_field: bool = Field(
        default=False
    )
    skip_event_id: bool = Field(
        default=False
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_quantile_value(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        quantile,
        group_by,
        return_type=T.DoubleType()
    ):
        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def compute_quantile(pdf, input_field, quantile, output_field) -> pd.DataFrame:
            pvs = pdf[input_field]

            # Calculate the XXth percentile
            percentile = pvs.quantile(quantile)

            # Create a new column with the XXth percentile value
            pdf[output_field] = percentile

            return pdf

        def wrapper(pdf, input_field, quantile, output_field):
            return compute_quantile(pdf, input_field, quantile, output_field)

        # Group the data and apply the UDF
        # lambda pdf: wrapper_function(pdf, threshold_value)
        sdf = sdf.groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, quantile, output_field),
            schema=output_schema
        )

        return sdf

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
            pdf[output_field] = pvs < percentile

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
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        if self.add_quantile_field:
            sdf = self.add_quantile_value(
                sdf=sdf,
                input_field=self.value_field_name,
                quantile=self.quantile,
                output_field=self.output_quantile_field_name,
                group_by=self.uniqueness_fields
            )
        sdf = self.add_is_event(
            sdf=sdf,
            input_field=self.value_field_name,
            quantile=self.quantile,
            output_field=self.output_event_field_name,
            group_by=self.uniqueness_fields
        )
        if not self.skip_event_id:
            sdf = self.add_event_ids(
                sdf=sdf,
                input_field=self.output_event_field_name,
                time_field=self.value_time_field_name,
                output_field=self.output_event_id_field_name,
                group_by=self.uniqueness_fields
            )

        return sdf


class ExceedanceProbability(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Calculates exceedance probability for a flow duration curve.

    This class computes exceedance probability statistics for a given
    timeseries of streamflow data. It adds the column 'exceedance_probability'
    to the DataFrame, representing the probability of exceedance for each flow
    value. The returned column is returned as a float between 0 and 1 unless
    otherwise specified using the as_percentile property (False by default).

    Properties
    ----------
    - as_percentile: bool
        Whether to return the exceedance probability as a percentile (0-100)
        or a probability (0-1). Returns probability by default.
        Default: False
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the streamflow values.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the exceedance probability information.
        Default: "exceedance_probability"
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

    as_percentile: bool = Field(
        default=False
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="exceedance_probability"
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_EP(
        sdf,
        as_percentile,
        output_field,
        input_field,
        time_field,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def exceedance_probability(pdf: pd.DataFrame,
                                   input_field,
                                   time_field,
                                   output_field,
                                   as_percentile) -> pd.DataFrame:
            # Convert to numpy array
            values = pdf[input_field].values

            # negative ranks for descending order
            ranks = rankdata(-values, method='ordinal')

            # Calculate exceedance probability directly
            n = len(values)
            if as_percentile:
                pdf[output_field] = (ranks / (n + 1)) * 100
            else:
                pdf[output_field] = ranks / (n + 1)

            return pdf

        def wrapper(pdf, input_field, time_field, output_field, as_percentile):
            return exceedance_probability(pdf,
                                          input_field,
                                          time_field,
                                          output_field,
                                          as_percentile)

        # group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                as_percentile
                                ),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_EP(
            sdf=sdf,
            as_percentile=self.as_percentile,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            group_by=self.uniqueness_fields
        )

        return sdf


class BaseflowPeriodDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Determines baseflow dominated periods.

    This class identifies periods where baseflow is dominant in the streamflow
    timeseries by adding two columns. The 'baseflow_period' column (bool)
    indicates whether the baseflow portion of the streamflow timeseries exceeds
    the quickflow portion, and the 'baseflow_period_id' column (string) groups
    continuous segments of baseflow dominated periods and assigns a unique ID
    to each segment in the format "startdate-enddate". Users can define a
    custom 'event_threshold' value to adjust the sensitivity of baseflow
    detection by applying a multiplier to the quickflow portion of the
    streamflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to compare with baseflow.
        Default: "primary_value"
    - baseflow_field_name:
        The name of the column containing the baseflow values.
        Default: None
    - event_threshold:
        The threshold multiplier value to determine event periods. The
        multiplier is applied to the quickflow portion of the streamflow
        timeseries when determining if the streamflow timeseries is
        dominated by baseflow.
        Default: 1.0
    - output_baseflow_period_field_name:
        The name of the column to store the baseflow period information.
        Default: "baseflow_period"
    - output_baseflow_period_id_field_name:
        The name of the column to store the baseflow period ID information.
        Default: "baseflow_period_id"
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    baseflow_field_name: str = Field(
        default=None
    )
    event_threshold: float = Field(
        default=1.0
    )
    output_baseflow_period_field_name: str = Field(
        default="baseflow_period"
    )
    output_baseflow_period_id_field_name: str = Field(
        default="baseflow_period_id"
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_is_baseflow_period(
        sdf: ps.DataFrame,
        input_field,
        baseflow_field,
        event_threshold,
        output_field,
        group_by,
        return_type=T.BooleanType()
    ):
        # Check for baseflow_field_name
        if baseflow_field is None:
            raise ValueError("baseflow_field_name must be specified.")

        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def is_baseflow_period(
                 pdf: pd.DataFrame,
                 input_field,
                 baseflow_field,
                 event_threshold,
                 output_field
        ) -> pd.DataFrame:
            # isolate timeseries
            streamflows = pdf[input_field]
            baseflows = pdf[baseflow_field]
            quickflows = streamflows - baseflows

            # apply event_threshold
            quickflows_adj = quickflows * event_threshold

            # create boolean and add to dataframe
            pdf[output_field] = baseflows > quickflows_adj

            return pdf

        def wrapper(pdf,
                    input_field,
                    baseflow_field,
                    event_threshold,
                    output_field):
            return is_baseflow_period(
                pdf, input_field, baseflow_field, event_threshold, output_field
            )

        sdf = sdf.groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                baseflow_field,
                                event_threshold,
                                output_field),
            schema=output_schema
        )

        return sdf

    @staticmethod
    def add_baseflow_period_ids(
        sdf: ps.DataFrame,
        input_field,
        time_field,
        output_field,
        group_by,
        return_type=T.StringType()
    ):
        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, return_type, True)])

        def baseflow_period_id(pdf: pd.DataFrame,
                               input_field,
                               time_field,
                               output_field
                               ) -> pd.DataFrame:
            # Create a new column for continuous segments
            pdf['segment'] = (pdf[input_field] != pdf[input_field].shift()).cumsum()

            # Filter only the segments where baseflow exceeds streamflow
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

            # Drop the 'segment', 'startdate', and 'enddate' columns
            pdf.drop(columns=['segment', 'startdate', 'enddate'], inplace=True)

            return pdf

        def wrapper(pdf, input_field, time_field, output_field):
            return baseflow_period_id(pdf,
                                      input_field,
                                      time_field,
                                      output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_is_baseflow_period(
            sdf=sdf,
            input_field=self.value_field_name,
            baseflow_field=self.baseflow_field_name,
            event_threshold=self.event_threshold,
            output_field=self.output_baseflow_period_field_name,
            group_by=self.uniqueness_fields
        )
        sdf = self.add_baseflow_period_ids(
            sdf=sdf,
            input_field=self.output_baseflow_period_field_name,
            time_field=self.value_time_field_name,
            output_field=self.output_baseflow_period_id_field_name,
            group_by=self.uniqueness_fields
        )

        return sdf


class LyneHollickBaseflow(CalculatedFieldABC,
                          CalculatedFieldBaseModel):
    """Baseflow separation using the Lyne-Hollick method.

    This class implements the Lyne-Hollick digital filter method, which
    separates baseflow from quickflow using a timeseries of streamflow data.
    Adds a column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "lyne_hollick_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="lyne_hollick_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_lyne_hollick_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        group_by,
        return_type=T.DoubleType()
    ):
        # Get the schema of the input DataFrame
        input_schema = sdf.schema

        # Create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def lyne_hollick_baseflow(pdf: pd.DataFrame,
                                  input_field,
                                  time_field,
                                  output_field,
                                  beta) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.methods import LH

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Lyne-Hollick method
            date, flow = clean_streamflow(input_streamflow)
            result_df = pd.DataFrame(np.nan, index=date, columns=['LH'])
            result_df['LH'] = LH(Q=flow,
                                 beta=beta,
                                 return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['LH'].values

            return pdf

        def wrapper(pdf, input_field, time_field, output_field, beta):
            return lyne_hollick_baseflow(pdf,
                                         input_field,
                                         time_field,
                                         output_field,
                                         beta)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_lyne_hollick_baseflow(
            sdf=sdf,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            output_field=self.output_field_name,
            beta=self.beta,
            group_by=self.uniqueness_fields
        )

        return sdf


class ChapmanBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Chapman method.

    This class implements the Chapman filter method, which separates baseflow
    from quickflow using a timeseries of streamflow data. Adds a column to
    the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "chapman_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Chapman filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="chapman_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_chapman_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        a,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def chapman_baseflow(pdf: pd.DataFrame,
                             input_field,
                             time_field,
                             output_field,
                             beta,
                             a) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient
            from baseflow.methods import LH, Chapman

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Chapman method
            date, flow = clean_streamflow(input_streamflow)
            strict_filter = strict_baseflow(Q=flow,
                                            ice=None)
            if not a:
                a = recession_coefficient(Q=flow,
                                          strict=strict_filter)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            result_df = pd.DataFrame(np.nan, index=date, columns=['Chapman'])
            result_df['Chapman'] = Chapman(Q=flow,
                                           b_LH=b_LH,
                                           a=a,
                                           return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['Chapman'].values

            return pdf

        def wrapper(pdf, input_field, time_field, output_field, beta, a):
            return chapman_baseflow(pdf,
                                    input_field,
                                    time_field,
                                    output_field,
                                    beta,
                                    a)

        # group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                a),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_chapman_baseflow(
            sdf=sdf,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            output_field=self.output_field_name,
            beta=self.beta,
            a=self.a,
            group_by=self.uniqueness_fields
        )

        return sdf


class ChapmanMaxwellBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Chapman-Maxwell method.

    This class implements the Chapman-Maxwell filter method, which separates
    baseflow from quickflow using a timeseries of streamflow data. Adds a
    column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "chapman_maxwell_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Chapman-Maxwell filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="chapman_maxwell_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_chapman_maxwell_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        a,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def chapman_maxwell_baseflow(pdf: pd.DataFrame,
                                     input_field,
                                     time_field,
                                     output_field,
                                     beta,
                                     a) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient
            from baseflow.methods import LH, CM

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Chapman-Maxwell method
            date, flow = clean_streamflow(input_streamflow)
            strict_filter = strict_baseflow(Q=flow,
                                            ice=None)
            if not a:
                a = recession_coefficient(Q=flow,
                                          strict=strict_filter)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            result_df = pd.DataFrame(np.nan, index=date, columns=['CM'])
            result_df['CM'] = CM(Q=flow,
                                 b_LH=b_LH,
                                 a=a,
                                 return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['CM'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field, beta, a):
            return chapman_maxwell_baseflow(pdf,
                                            input_field,
                                            time_field,
                                            output_field,
                                            beta,
                                            a)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                a),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_chapman_maxwell_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
            a=self.a,
            group_by=self.uniqueness_fields
        )

        return sdf


class BoughtonBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Boughton method.

    This class implements the Boughton double-parameter filter method,
    which separates baseflow from quickflow using a timeseries of streamflow
    data. Adds a column to the joined timeseries table with the baseflow
    timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "boughton_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Boughton filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - c: float
        The shape parameter for the Boughton filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="boughton_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    c: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_boughton_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        a,
        c,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def boughton_baseflow(pdf: pd.DataFrame,
                              input_field,
                              time_field,
                              output_field,
                              beta,
                              a,
                              c) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient
            from baseflow.param_estimate import param_calibrate
            from baseflow.methods import LH, Boughton

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Boughton method
            date, flow = clean_streamflow(input_streamflow)
            strict_filter = strict_baseflow(Q=flow,
                                            ice=None)
            if not a:
                a = recession_coefficient(Q=flow,
                                          strict=strict_filter)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            if not c:
                param_range = np.arange(0.0001, 0.1, 0.0001)
                c = param_calibrate(param_range=param_range,
                                    method=Boughton,
                                    Q=flow,
                                    b_LH=b_LH,
                                    a=a)
            result_df = pd.DataFrame(np.nan, index=date, columns=['Boughton'])
            result_df['Boughton'] = Boughton(Q=flow,
                                             b_LH=b_LH,
                                             a=a,
                                             C=c,
                                             return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['Boughton'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field, beta, a, c):
            return boughton_baseflow(pdf,
                                     input_field,
                                     time_field,
                                     output_field,
                                     beta,
                                     a,
                                     c)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                a,
                                c),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_boughton_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
            a=self.a,
            c=self.c,
            group_by=self.uniqueness_fields
        )

        return sdf


class FureyBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Furey method.

    This class implements the Furey digital filter method, which separates
    baseflow from quickflow using a timeseries of streamflow data. Adds a
    column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "furey_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Furey filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - c: float
        The shape parameter for the Furey filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="furey_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    c: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_furey_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        a,
        c,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def furey_baseflow(pdf: pd.DataFrame,
                           input_field,
                           time_field,
                           output_field,
                           beta,
                           a,
                           c) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient
            from baseflow.param_estimate import param_calibrate
            from baseflow.methods import LH, Furey

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Furey method
            date, flow = clean_streamflow(input_streamflow)
            strict_filter = strict_baseflow(Q=flow,
                                            ice=None)
            if not a:
                a = recession_coefficient(Q=flow,
                                          strict=strict_filter)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            if not c:
                param_range = np.arange(0.01, 10, 0.01)
                c = param_calibrate(param_range=param_range,
                                    method=Furey,
                                    Q=flow,
                                    b_LH=b_LH,
                                    a=a)
            result_df = pd.DataFrame(np.nan, index=date, columns=['Furey'])
            result_df['Furey'] = Furey(Q=flow,
                                       b_LH=b_LH,
                                       a=a,
                                       A=c,
                                       return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['Furey'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field, beta, a, c):
            return furey_baseflow(pdf,
                                  input_field,
                                  time_field,
                                  output_field,
                                  beta,
                                  a,
                                  c)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                a,
                                c),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_furey_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
            a=self.a,
            c=self.c,
            group_by=self.uniqueness_fields
        )

        return sdf


class EckhardtBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Eckhardt method.

    This class implements the Eckhardt filter method, which separates baseflow
    from quickflow using a timeseries of streamflow data. Adds a column to
    the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "eckhardt_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Eckhardt filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - BFImax: float
        The maximum baseflow index for the Eckhardt filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="eckhardt_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    BFImax: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_eckhardt_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        a,
        BFImax,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def eckhardt_baseflow(pdf: pd.DataFrame,
                              input_field,
                              time_field,
                              output_field,
                              beta,
                              a,
                              BFImax) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient
            from baseflow.param_estimate import param_calibrate
            from baseflow.methods import LH, Eckhardt

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Eckhardt method
            date, flow = clean_streamflow(input_streamflow)
            strict_filter = strict_baseflow(Q=flow,
                                            ice=None)
            if not a:
                a = recession_coefficient(Q=flow,
                                          strict=strict_filter)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            if not BFImax:
                param_range = np.arange(0.001, 1, 0.001)
                BFImax = param_calibrate(param_range=param_range,
                                         method=Eckhardt,
                                         Q=flow,
                                         b_LH=b_LH,
                                         a=a)
            result_df = pd.DataFrame(np.nan, index=date, columns=['Eckhardt'])
            result_df['Eckhardt'] = Eckhardt(Q=flow,
                                             b_LH=b_LH,
                                             a=a,
                                             BFImax=BFImax,
                                             return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['Eckhardt'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf,
                    input_field,
                    time_field,
                    output_field,
                    beta,
                    a,
                    BFImax):
            return eckhardt_baseflow(pdf,
                                     input_field,
                                     time_field,
                                     output_field,
                                     beta,
                                     a,
                                     BFImax)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                a,
                                BFImax),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_eckhardt_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
            a=self.a,
            BFImax=self.BFImax,
            group_by=self.uniqueness_fields
        )

        return sdf


class EWMABaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the EWMA method.

    This class implements the exponential moving average (EWMA) filter method,
    which separates baseflow from quickflow using a timeseries of streamflow
    data. Adds a column to the joined timeseries table with the baseflow
    timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "ewma_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - e: float
        The smoothing parameter for the EWMA filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="ewma_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    e: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_ewma_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        e,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def ewma_baseflow(pdf: pd.DataFrame,
                          input_field,
                          time_field,
                          output_field,
                          beta,
                          e) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.param_estimate import param_calibrate
            from baseflow.methods import LH, EWMA

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the EWMA method
            date, flow = clean_streamflow(input_streamflow)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            if not e:
                param_range = np.arange(0.0001, 0.1, 0.0001)
                e = param_calibrate(param_range=param_range,
                                    method=EWMA,
                                    Q=flow,
                                    b_LH=b_LH,
                                    a=None)
            result_df = pd.DataFrame(np.nan, index=date, columns=['EWMA'])
            result_df['EWMA'] = EWMA(Q=flow,
                                     b_LH=b_LH,
                                     a=None,
                                     e=e,
                                     return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['EWMA'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field, beta, e):
            return ewma_baseflow(pdf,
                                 input_field,
                                 time_field,
                                 output_field,
                                 beta,
                                 e)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                e),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_ewma_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
            e=self.e,
            group_by=self.uniqueness_fields
        )

        return sdf


class WillemsBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Willems method.

    This class implements the Willems digital filter method, which separates
    baseflow from quickflow using a timeseries of streamflow data. Adds a
    column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "willems_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Willems filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - w: float
        The case-specific average quickflow proportion of the streamflow
        used in the Willems filter method. If not provided, it will be
        estimated using the input timeseries data.
        Default: None
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="willems_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    w: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_willems_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        a,
        w,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def willems_baseflow(pdf: pd.DataFrame,
                             input_field,
                             time_field,
                             output_field,
                             beta,
                             a,
                             w) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient
            from baseflow.param_estimate import param_calibrate
            from baseflow.methods import LH, Willems

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the Willems method
            date, flow = clean_streamflow(input_streamflow)
            strict_filter = strict_baseflow(Q=flow,
                                            ice=None)
            if not a:
                a = recession_coefficient(Q=flow,
                                          strict=strict_filter)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            if not w:
                param_range = np.arange(0.001, 1, 0.001)
                w = param_calibrate(param_range=param_range,
                                    method=Willems,
                                    Q=flow,
                                    b_LH=b_LH,
                                    a=a)
            result_df = pd.DataFrame(np.nan, index=date, columns=['Willems'])
            result_df['Willems'] = Willems(Q=flow,
                                           b_LH=b_LH,
                                           a=a,
                                           w=w,
                                           return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['Willems'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field, beta, a, w):
            return willems_baseflow(pdf,
                                    input_field,
                                    time_field,
                                    output_field,
                                    beta,
                                    a,
                                    w)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field
            ).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta,
                                a,
                                w),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_willems_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
            a=self.a,
            w=self.w,
            group_by=self.uniqueness_fields
        )

        return sdf


class UKIHBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the UKIH method.

    This class implements the United Kingdom Institute of Hydrology (UKIH)
    filter method (also referred to as the smoothed minima method), which
    separates baseflow from quickflow using a timeseries of streamflow data.
    Adds a column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "ukih_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
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

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="ukih_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    @staticmethod
    def add_ukih_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
        beta,
        group_by,
        return_type=T.DoubleType()
    ):
        # get the schema of the input DataFrame
        input_schema = sdf.schema

        # create a copy of the schema and add the new column
        output_schema = T.StructType(
            input_schema.fields + [T.StructField(output_field,
                                                 return_type,
                                                 True)]
        )

        def ukih_baseflow(pdf: pd.DataFrame,
                          input_field,
                          time_field,
                          output_field,
                          beta) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            from baseflow.utils import clean_streamflow
            from baseflow.methods import LH, UKIH

            # create a new column for baseflow
            pdf[output_field] = None

            # obtain the input streamflow series
            input_streamflow = pd.Series(
                pdf[input_field].values,
                index=pd.to_datetime(pdf[time_field])
            )

            # ensure data has >= 120 timesteps
            if len(input_streamflow) < 120:
                raise ValueError(
                    "Input streamflow series must have at least 120 timesteps."
                    )

            # obtain the baseflow separation using the UKIH method
            date, flow = clean_streamflow(input_streamflow)
            b_LH = LH(Q=flow,
                      beta=beta,
                      return_exceed=False)
            result_df = pd.DataFrame(np.nan, index=date, columns=['UKIH'])
            result_df['UKIH'] = UKIH(Q=flow,
                                     b_LH=b_LH,
                                     return_exceed=False)

            # assign the baseflow values to the new column
            pdf[output_field] = result_df['UKIH'].values

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field, beta):
            return ukih_baseflow(pdf,
                                 input_field,
                                 time_field,
                                 output_field,
                                 beta)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(
            *group_by,
            time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf,
                                input_field,
                                time_field,
                                output_field,
                                beta),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        if self.uniqueness_fields is None:
            self.uniqueness_fields = UNIQUENESS_FIELDS
        sdf = self.add_ukih_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            beta=self.beta,
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

    - AbovePercentileEventDetection
    - BelowPercentileEventDetection
    - ExceedanceProbability
    - BaseflowPeriodDetection
    - LyneHollickBaseflow
    - ChapmanBaseflow
    - ChapmanMaxwellBaseflow
    - BoughtonBaseflow
    - FureyBaseflow
    - EckhardtBaseflow
    - EWMABaseflow
    - WillemsBaseflow
    - UKIHBaseflow
    """

    AbovePercentileEventDetection = AbovePercentileEventDetection
    BelowPercentileEventDetection = BelowPercentileEventDetection
    ExceedanceProbability = ExceedanceProbability
    BaseflowPeriodDetection = BaseflowPeriodDetection
    LyneHollickBaseflow = LyneHollickBaseflow
    ChapmanBaseflow = ChapmanBaseflow
    ChapmanMaxwellBaseflow = ChapmanMaxwellBaseflow
    BoughtonBaseflow = BoughtonBaseflow
    FureyBaseflow = FureyBaseflow
    EckhardtBaseflow = EckhardtBaseflow
    EWMABaseflow = EWMABaseflow
    WillemsBaseflow = WillemsBaseflow
    UKIHBaseflow = UKIHBaseflow
