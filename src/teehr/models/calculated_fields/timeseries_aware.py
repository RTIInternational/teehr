"""Classes representing UDFs."""
from typing import List, Union
from pydantic import Field
import pandas as pd
import numpy as np
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
    def add_lyne_hollick_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                                  output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='LH',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['LH'].values, 2)

            return pdf

        def wrapper(pdf, input_field, time_field, output_field):
            return lyne_hollick_baseflow(pdf,
                                         input_field,
                                         time_field,
                                         output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_lyne_hollick_baseflow(
            sdf=sdf,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            output_field=self.output_field_name,
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
    def add_chapman_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                             output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='Chapman',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['Chapman'].values, 2)

            return pdf

        def wrapper(pdf, input_field, time_field, output_field):
            return chapman_baseflow(pdf, input_field, time_field, output_field)

        # group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_chapman_baseflow(
            sdf=sdf,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
            output_field=self.output_field_name,
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
    def add_chapman_maxwell_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                                     output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='CM',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['CM'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return chapman_maxwell_baseflow(pdf,
                                            input_field,
                                            time_field,
                                            output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_chapman_maxwell_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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
    def add_boughton_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                              output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='Boughton',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['Boughton'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return boughton_baseflow(pdf,
                                     input_field,
                                     time_field,
                                     output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_boughton_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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
    def add_furey_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                           output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='Furey',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['Furey'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return furey_baseflow(pdf,
                                  input_field,
                                  time_field,
                                  output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_furey_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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
    def add_eckhardt_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                              output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='Eckhardt',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['Eckhardt'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return eckhardt_baseflow(pdf,
                                     input_field,
                                     time_field,
                                     output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_eckhardt_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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
    def add_ewma_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                          output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='EWMA',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['EWMA'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return ewma_baseflow(pdf,
                                 input_field,
                                 time_field,
                                 output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_ewma_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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
    def add_willems_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                             output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='Willems',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['Willems'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return willems_baseflow(pdf,
                                    input_field,
                                    time_field,
                                    output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_willems_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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
    def add_ukih_baseflow(
        sdf: ps.DataFrame,
        output_field,
        input_field,
        time_field,
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
                          output_field) -> pd.DataFrame:
            # lazy load the BYU-baseflow library
            import baseflow as bf

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
            result = bf.single(series=input_streamflow,
                               area=None,
                               ice=None,
                               method='UKIH',
                               return_kge=False)
            result_df = result[0]

            # assign the baseflow values to the new column
            pdf[output_field] = np.round(result_df['UKIH'].values, 2)

            return pdf

        # Define the UDF for baseflow separation
        def wrapper(pdf, input_field, time_field, output_field):
            return ukih_baseflow(pdf,
                                 input_field,
                                 time_field,
                                 output_field)

        # Group the data and apply the UDF
        sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
            lambda pdf: wrapper(pdf, input_field, time_field, output_field),
            schema=output_schema
        )

        return sdf

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        sdf = self.add_ukih_baseflow(
            sdf,
            output_field=self.output_field_name,
            input_field=self.value_field_name,
            time_field=self.value_time_field_name,
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

    PercentileEventDetection = PercentileEventDetection
    LyneHollickBaseflow = LyneHollickBaseflow
    ChapmanBaseflow = ChapmanBaseflow
    ChapmanMaxwellBaseflow = ChapmanMaxwellBaseflow
    BoughtonBaseflow = BoughtonBaseflow
    FureyBaseflow = FureyBaseflow
    EckhardtBaseflow = EckhardtBaseflow
    EWMABaseflow = EWMABaseflow
    WillemsBaseflow = WillemsBaseflow
    UKIHBaseflow = UKIHBaseflow
