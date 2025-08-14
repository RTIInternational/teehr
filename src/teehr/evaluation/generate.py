"""A component class for generating synthetic time series."""
import logging
from typing import Union, List
import sys

from pyspark.sql import functions as F
import pyspark.sql as ps
from pyspark.sql import Window

from teehr.models.filters import FilterBaseModel
from teehr.querying.utils import group_df
from teehr.models.metrics.basemodels import (
    ClimatologyResolutionEnum,
    ClimatologyStatisticEnum
)
from teehr.models.calculated_fields.row_level import (
    RowLevelCalculatedFields as rlc
)

logger = logging.getLogger(__name__)


class Generate:
    """Component class for generating synthetic time series."""

    def __init__(self, ev) -> None:
        """Initialize the Generate class."""
        self.ev = ev

    def _calculate_rolling_average(
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

    @staticmethod
    def _ffill_and_bfill_nans(
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

    @staticmethod
    def _get_time_period_rlc(
        temporal_resolution: ClimatologyResolutionEnum
    ) -> rlc:
        """Get the time period row level calculated field based on resolution."""
        if temporal_resolution == ClimatologyResolutionEnum.day_of_year:
            return rlc.DayOfYear()
        elif temporal_resolution == ClimatologyResolutionEnum.hour_of_year:
            return rlc.HourOfYear()
        elif temporal_resolution == ClimatologyResolutionEnum.month:
            return rlc.Month()
        elif temporal_resolution == ClimatologyResolutionEnum.year:
            return rlc.Year()
        elif temporal_resolution == ClimatologyResolutionEnum.water_year:
            return rlc.WaterYear()
        elif temporal_resolution == ClimatologyResolutionEnum.season:
            return rlc.Seasons()
        else:
            raise ValueError(
                f"Unsupported temporal resolution: {temporal_resolution}"
            )

    def _calculate_climatology(
        self,
        input_timeseries_filter: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ],
        temporal_resolution: ClimatologyResolutionEnum,
        summary_statistic: ClimatologyStatisticEnum,
    ) -> ps.DataFrame:
        """Calculate climatology."""
        time_period = self._get_time_period_rlc(temporal_resolution)

        if summary_statistic == "mean":
            summary_func = F.mean
        elif summary_statistic == "median":
            summary_func = F.expr("percentile_approx(value, 0.5)")
        elif summary_statistic == "max":
            summary_func = F.max
        elif summary_statistic == "min":
            summary_func = F.min

        # Get the configuration to use for reference calculation.
        input_timeseries_sdf = (
            self.
            query(filters=input_timeseries_filter).
            to_sdf()
        )
        groupby_field_list = [
            "location_id",
            "variable_name",
            "unit_name",
            "configuration_name"
        ]
        # Add the time period as a calculated field.
        input_timeseries_sdf = time_period.apply_to(input_timeseries_sdf)
        # Aggregate values based on the time period and summary statistic.
        groupby_field_list.append(time_period.output_field_name)
        summary_sdf = (
            group_df(input_timeseries_sdf, groupby_field_list).
            agg(summary_func("value").alias("value"))
        )
        clim_sdf = input_timeseries_sdf.drop("value").join(
            summary_sdf,
            on=groupby_field_list,
            how="left"
        ).drop(time_period.output_field_name)
        return clim_sdf

    def climatology(
        self,
        input_timeseries_filter: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ],
        output_configuration_name: str,
        output_variable_name: str = "streamflow_daily_climatology",
        temporal_resolution: ClimatologyResolutionEnum = "day_of_year",
        summary_statistic: ClimatologyStatisticEnum = "mean"
    ):
        """Calculate climatology and add as a new timeseries.

        Parameters
        ----------
        input_timeseries_filter : Union[str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]]
            Filter to apply to the input timeseries to use in the
            climatology calculation.
        output_configuration_name : str
            Configuration name for the output climatology timeseries.
        output_variable_name : str, optional
            Variable name for the output climatology timeseries,
            by default "streamflow_daily_climatology".
        temporal_resolution : ClimatologyResolutionEnum, optional
            Temporal resolution for the climatology calculation,
            by default "day_of_year".
        summary_statistic : ClimatologyStatisticEnum, optional
            Summary statistic for the climatology calculation,
            by default "mean".
        """
        clim_sdf = self._calculate_climatology(
            input_timeseries_filter=input_timeseries_filter,
            temporal_resolution=temporal_resolution,
            summary_statistic=summary_statistic
        )
        self.load_dataframe(
            df=clim_sdf,
            constant_field_values={
                "configuration_name": output_configuration_name,
                "variable_name": output_variable_name
            },
            write_mode="overwrite"
        )
