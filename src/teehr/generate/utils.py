"""Utilities for generating synthetic timeseries."""
from typing import List, Union
import sys
from functools import reduce
from datetime import timedelta, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pyspark.sql as ps
from pyspark.sql import Window

from teehr.models.generate.base import (
    NormalsResolutionEnum
)
from teehr.models.calculated_fields.row_level import (
    RowLevelCalculatedFields as rlc
)


def construct_signature_dataframe(
    spark: ps.SparkSession,
    input_dataframe: ps.DataFrame,
    start_datetime: Union[str, datetime],
    end_datetime: Union[str, datetime],
    timestep: Union[str, timedelta]
) -> ps.DataFrame:
    """Construct a template signature DataFrame for the given input.

    In a signature dataframe, reference_time is assigned None. Remaining
    columns other than ``value`` are populated from the input dataframe.

    Parameters
    ----------
    spark : ps.SparkSession
        The Spark session object.
    input_dataframe : ps.DataFrame
        The input Spark DataFrame to generate the signature DataFrame.
    start_datetime : Union[str, datetime]
        The start datetime for the time series value_time.
        If provided as a str, the format must be supported by PySpark's
        ``to_timestamp`` function, such as "yyyy-MM-dd HH:mm:ss".
    end_datetime : Union[str, datetime]
        The end datetime for the time series value_time.
        If provided as a str, the format must be supported by PySpark's
        ``to_timestamp`` function, such as "yyyy-MM-dd HH:mm:ss".
    timestep : Union[str, timedelta]
        The time step for the time series.
    """
    sdf = spark.createDataFrame(
        [(start_datetime, end_datetime)],
        ["start_ts_str", "end_ts_str"]) \
        .withColumn("start_ts", F.to_timestamp(F.col("start_ts_str"))) \
        .withColumn("end_ts", F.to_timestamp(F.col("end_ts_str")))

    if isinstance(timestep, timedelta):
        timestep = f"{timestep.total_seconds()} seconds"

    output_sdf = sdf.withColumn(
        "timestamp_array",
        F.sequence(
                F.col("start_ts"),
                F.col("end_ts"),
                F.expr(f"interval {timestep}")
            )
        ) \
        .withColumn("value_time", F.explode(F.col("timestamp_array"))) \
        .select("value_time")

    output_sdf = output_sdf.withColumn("reference_time", F.lit(None))

    groupby_field_list = [
        "location_id",
        "variable_name",
        "unit_name",
        "configuration_name"
    ]
    concat_df_list = []
    for row in input_dataframe.select(
        *groupby_field_list
    ).distinct().toLocalIterator():
        for key, val in row.asDict().items():
            output_sdf = output_sdf.withColumn(key, F.lit(val))
        concat_df_list.append(output_sdf)

    return reduce(DataFrame.unionByName, concat_df_list)


def get_time_period_rlc(
    temporal_resolution: NormalsResolutionEnum
) -> rlc:
    """Get the time period row level calculated field based on resolution."""
    if temporal_resolution == NormalsResolutionEnum.day_of_year:
        return rlc.DayOfYear()
    elif temporal_resolution == NormalsResolutionEnum.hour_of_year:
        return rlc.HourOfYear()
    elif temporal_resolution == NormalsResolutionEnum.month:
        return rlc.Month()
    elif temporal_resolution == NormalsResolutionEnum.year:
        return rlc.Year()
    elif temporal_resolution == NormalsResolutionEnum.water_year:
        return rlc.WaterYear()
    elif temporal_resolution == NormalsResolutionEnum.season:
        return rlc.Seasons()
    else:
        raise ValueError(
            f"Unsupported temporal resolution: {temporal_resolution}"
        )


def calculate_rolling_average(
    spark: ps.SparkSession,
    sdf: ps.DataFrame,
    partition_by: List[str] = [
        'reference_time',
        'location_id',
        'configuration_name',
        'variable_name',
        'unit_name'
    ],
    statistic: str = "mean",
    time_window: str = "6 hours",
    input_column: str = "value",
    output_column: str = "agg_value"
) -> ps.DataFrame:
    """Calculate rolling average for a given time period.

    Notes
    -----
    This function summarizes values (``mean`` by default) over a specified
    look-back period defined by the ``time_window`` parameter.

    The input table is grouped by the specified ``partition_by``
    columns, which is the set of columns defining a unique time series.
    """
    sdf.createOrReplaceTempView("temp_df")
    col_list = sdf.columns
    col_list.remove(input_column)
    return spark.sql(
        f"""
        WITH cte AS (
            SELECT *, {statistic}({input_column}) OVER (
                PARTITION BY {", ".join(partition_by)}
                ORDER BY CAST(value_time AS timestamp)
                RANGE BETWEEN INTERVAL {time_window} PRECEDING AND CURRENT ROW
            ) AS {output_column} FROM temp_df
        )
        SELECT
            {", ".join(col_list)},
            {output_column} AS {input_column}
        FROM cte
        """
    )


def ffill_and_bfill_nans(
    sdf: ps.DataFrame,
    partition_by: List[str] = ["location_id", "variable_name", "unit_name", "configuration_name", "reference_time"],
    order_by: str = "value_time"
) -> ps.DataFrame:
    """Forward fill and backward fill NaN values in the DataFrame."""
    sdf = sdf.withColumn(
        "value",
        F.last("value", ignorenulls=True).
        over(
            Window.partitionBy(*partition_by).
            orderBy(order_by).
            rowsBetween(-sys.maxsize, 0)
        )
    )
    sdf = sdf.withColumn(
        "value",
        F.first("value", ignorenulls=True).
        over(
            Window.partitionBy(*partition_by).
            orderBy(order_by).
            rowsBetween(0, sys.maxsize)
        )
    )
    return sdf
