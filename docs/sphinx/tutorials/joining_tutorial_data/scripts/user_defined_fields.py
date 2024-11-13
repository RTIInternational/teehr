"""Functions for adding user defined fields to joined timeseries data.

This file gets dynamically imported to the TEEHR
create_joined_timeseries_dataset() function and is used to add user
defined fields when a user runs ev.create_joined_timeseries().

Users can modify this script to add or remove user defined fields to
the joined timeseries data.

WARNING: Do not change the name of this file or the functions it contains.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType, BooleanType, StructType, StructField, StringType
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def add_is_above_quantile(
    sdf,
    output_field,
    input_field, 
    quantile=0.90,
    return_type=BooleanType(),
    group_by=[
        'reference_time',
        'primary_location_id',
        'configuration_name',
        'variable_name',
        'unit_name'
    ]

):
    # Get the schema of the input DataFrame
    input_schema = sdf.schema
        
    # Create a copy of the schema and add the new column
    output_schema = StructType(input_schema.fields + [StructField(output_field, return_type, True)])

    def is_above_quantile(pdf, output_field, input_field, quantile) -> pd.DataFrame:
        pvs = pdf[input_field]
        
        # Calculate the 90th percentile
        percentile = pvs.quantile(quantile)
        
        # Create a new column indicating whether each value is above the 90th percentile
        pdf[output_field] = pvs > percentile
        
        return pdf
    
    def wrapper(pdf, output_field, input_field, quantile):
        return is_above_quantile(pdf, output_field, input_field, quantile)
    
    # Group the data and apply the UDF
    # lambda pdf: wrapper_function(pdf, threshold_value)
    sdf = sdf.groupby(group_by).applyInPandas(
        lambda pdf: wrapper(pdf, output_field, input_field, quantile),
        schema=output_schema
    )
    
    return sdf


def add_segment_ids(
    sdf,
    output_field,
    input_field, 
    time_field,
    return_type=StringType(),
    group_by=[
        'reference_time',
        'primary_location_id',
        'configuration_name',
        'variable_name',
        'unit_name'
    ]

):
    # Get the schema of the input DataFrame
    input_schema = sdf.schema
        
    # Create a copy of the schema and add the new column
    output_schema = StructType(input_schema.fields + [StructField(output_field, return_type, True)])

    def segment(pdf: pd.DataFrame, output_field, input_field, time_field) -> pd.DataFrame:        
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
        
    def wrapper(pdf, output_field, input_field, time_field):
        return segment(pdf, output_field, input_field, time_field)
    
    # Group the data and apply the UDF
    sdf = sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
        lambda pdf: wrapper(pdf, output_field, input_field, time_field),
        schema=output_schema
    )
    
    return sdf


def add_user_defined_fields(
    joined_df: DataFrame
):
    """Add user defined fields to the joined timeseries data.

    Do not change the name of this function.

    Parameters
    ----------
    joined_df: DataFrame
        The joined timeseries data.

    Returns
    -------
    DataFrame
        The joined timeseries data with user defined fields.
    """
    logger.info("Adding user defined fields")

    # Add a month field to the joined timeseries data
    logger.info("Adding month from date")

    @udf(returnType=IntegerType())
    def month_from_date(date):
        return date.month

    joined_df = joined_df.withColumn(
        "month",
        month_from_date("value_time")
    )

    # Add a year field to the joined timeseries data
    logger.info("Adding water year from date")

    @udf(returnType=IntegerType())
    def year_from_date(date):
        return date.year

    joined_df = joined_df.withColumn(
        "year",
        year_from_date("value_time")
    )

    # Add a water year field to the joined timeseries data
    logger.info("Adding water year from date")

    @udf(returnType=IntegerType())
    def water_year_from_date(date):
        if date.month >= 10:
            return date.year + 1
        else:
            return date.year

    joined_df = joined_df.withColumn(
        "water_year",
        water_year_from_date("value_time")
    )

    # Add a normalized flow for primary and secondary values
    # to the joined timeseries data.
    logger.info("Adding normalized flow")

    @udf(returnType=FloatType())
    def normalized_flow(flow, area):
        return float(flow) / float(area)

    joined_df = joined_df.withColumn(
        "primary_normalized_flow",
        normalized_flow("primary_value", "drainage_area")
    )

    joined_df = joined_df.withColumn(
        "secondary_normalized_flow",
        normalized_flow("secondary_value", "drainage_area")
    )

    joined_df = add_is_above_quantile(joined_df, "over_90th_pct", "primary_value")
    joined_df = add_segment_ids(joined_df, "over_90th_pct_event_id", "over_90th_pct", "value_time")

    # Return the joined timeseries data with user defined fields
    return joined_df

