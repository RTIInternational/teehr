from typing import Union, List
import sys

from pyspark.sql import functions as F
import pyspark.sql as ps
from pyspark.sql import Window

# from teehr.evaluation.evaluation import Evaluation
from teehr.models.filters import FilterBaseModel
from teehr.querying.utils import group_df
from teehr.models.generate.base import (
    NormalsResolutionEnum,
    NormalsStatisticEnum,
    TimeseriesModel
)
from teehr.models.calculated_fields.row_level import (
    RowLevelCalculatedFields as rlc
)


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
    self,
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
    return self.ev.spark.sql(
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
