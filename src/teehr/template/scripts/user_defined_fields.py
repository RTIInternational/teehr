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
from pyspark.sql.types import IntegerType, FloatType
import logging

logger = logging.getLogger(__name__)


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
    # logger.info("Adding normalized flow")

    # @udf(returnType=FloatType())
    # def normalized_flow(flow, area):
    #     return float(flow) / float(area)

    # joined_df = joined_df.withColumn(
    #     "primary_normalized_flow",
    #     normalized_flow("primary_value", "drainage_area")
    # )

    # joined_df = joined_df.withColumn(
    #     "secondary_normalized_flow",
    #     normalized_flow("secondary_value", "drainage_area")
    # )

    # Return the joined timeseries data with user defined fields
    return joined_df