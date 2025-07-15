"""Timeseries table base class."""
from teehr.evaluation.tables.base_table import BaseTable
# from teehr.evaluation.tables.configuration_table import Configuration
from teehr.loading.utils import (
    validate_input_is_xml,
    validate_input_is_csv,
    validate_input_is_netcdf,
    validate_input_is_parquet
)
from teehr.models.filters import TimeseriesFilter, FilterBaseModel
from teehr.querying.utils import join_geometry, group_df
from teehr.models.table_enums import TableWriteEnum
from teehr.const import MAX_CPUS
from teehr.models.metrics.basemodels import (
    ClimatologyResolutionEnum,
    ClimatologyStatisticEnum
)
from teehr.models.calculated_fields.row_level import (
    RowLevelCalculatedFields as rlc
)
from pyspark.sql import functions as F
import pyspark.sql as ps
from pyspark.sql import Window

from pathlib import Path
from typing import Union, List
import sys
import logging

logger = logging.getLogger(__name__)


class TimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.format = "parquet"
        self.partition_by = [
            "configuration_name",
            "variable_name",
            "reference_time"
        ]
        self.filter_model = TimeseriesFilter
        self.unique_column_set = [
            "location_id",
            "value_time",
            "reference_time",
            "variable_name",
            "unit_name",
            "configuration_name"
        ]

    def to_pandas(self):
        """Return Pandas DataFrame for Timeseries."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = self.name
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        return join_geometry(self.df, self.ev.locations.to_sdf())

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        max_workers: Union[int, None] = MAX_CPUS,
        persist_dataframe: bool = False,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries parquet data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are
            overwritten.
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        persist_dataframe : bool, optional (default: False)
            Whether to repartition and persist the pyspark dataframe after
            reading from the cache. This can improve performance when loading
            a large number of files from the cache.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the dataframe.
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Includes validation and importing data to database.

        Notes
        -----
        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading timeseries parquet data: {in_path}")

        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            max_workers=max_workers,
            persist_dataframe=persist_dataframe,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_table()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        max_workers: Union[int, None] = MAX_CPUS,
        persist_dataframe: bool = False,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries csv data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            csv file format.
        pattern : str, optional (default: "**/*.csv")
            The pattern to match files. Controls which files are loaded from
            the directory. If in_path is a file, this parameter is ignored.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are overwritten
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        persist_dataframe : bool, optional (default: False)
            Whether to repartition and persist the pyspark dataframe after
            reading from the cache. This can improve performance when loading
            a large number of files from the cache.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the dataframe.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv().

        Includes validation and importing data to database.

        Notes
        -----
        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading timeseries csv data: {in_path}")

        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            max_workers=max_workers,
            persist_dataframe=persist_dataframe,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_table()

    def load_netcdf(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.nc",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        max_workers: Union[int, None] = MAX_CPUS,
        persist_dataframe: bool = False,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries netcdf data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            netcdf file format.
        pattern : str, optional (default: "**/*.nc")
            The pattern to match files. Controls which files are loaded from
            the directory. If in_path is a file, this parameter is ignored.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are overwritten
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        persist_dataframe : bool, optional (default: False)
            Whether to repartition and persist the pyspark dataframe after
            reading from the cache. This can improve performance when loading
            a large number of files from the cache.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the dataframe.
        **kwargs
            Additional keyword arguments are passed to xr.open_dataset().

        Includes validation and importing data to database.

        Notes
        -----
        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading timeseries netcdf data: {in_path}")

        validate_input_is_netcdf(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            max_workers=max_workers,
            persist_dataframe=persist_dataframe,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_table()

    def load_fews_xml(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.xml",
        field_mapping: dict = {
            "locationId": "location_id",
            "forecastDate": "reference_time",
            "parameterId": "variable_name",
            "units": "unit_name",
            "ensembleId": "configuration_name",
            "ensembleMemberIndex": "member",
            "forecastDate": "reference_time"
        },
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        max_workers: Union[int, None] = MAX_CPUS,
        persist_dataframe: bool = False,
        drop_duplicates: bool = True,
    ):
        """Import timeseries from XML data format.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            xml file format.
        pattern : str, optional (default: "**/*.xml")
            The pattern to match files. Controls which files are loaded from
            the directory. If in_path is a file, this parameter is ignored.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
            Default mapping:
            {
                "locationId": "location_id",
                "forecastDate": "reference_time",
                "parameterId": "variable_name",
                "units": "unit_name",
                "ensembleId": "configuration_name",
                "ensembleMemberIndex": "member",
                "forecastDate": "reference_time"
            }
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are overwritten
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        persist_dataframe : bool, optional (default: False)
            Whether to repartition and persist the pyspark dataframe after
            reading from the cache. This can improve performance when loading
            a large number of files from the cache.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the dataframe.

        Includes validation and importing data to database.

        Notes
        -----
        This function follows the Delft-FEWS Published Interface (PI)
        XML format.

        reference: https://publicwiki.deltares.nl/display/FEWSDOC/Dynamic+data

        The ``value`` and ``value_time`` fields are parsed automatically.

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        - member
        """
        logger.info(f"Loading timeseries xml data: {in_path}")

        validate_input_is_xml(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            max_workers=max_workers,
            persist_dataframe=persist_dataframe,
            drop_duplicates=drop_duplicates,
        )
        self._load_table()

    def _calculate_rolling_average(
        self,
        sdf: ps.DataFrame,
        partition_by: List[str],
        statistic: str = "mean",
        time_window: str = "6 hours",
        input_column: str = "value",
        output_column: str = "agg_value"
    ) -> ps.DataFrame:
        """Calculate rolling average for a given time period."""
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

    def calculate_climatology(
        self,
        input_timeseries_filter: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ],
        output_configuration_name: str,
        output_variable_name: str = "streamflow_hourly_climatology",
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
        pass