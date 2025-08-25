"""Timeseries table base class."""
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.utils import (
    validate_input_is_xml,
    validate_input_is_csv,
    validate_input_is_netcdf,
    validate_input_is_parquet
)
from teehr.models.filters import TimeseriesFilter
from teehr.querying.utils import join_geometry
from teehr.models.table_enums import TableWriteEnum
from teehr.const import MAX_CPUS

from pathlib import Path
from typing import Union
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
            # "reference_time"
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
        """Import timeseries from FEWS PI-XML data format.

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

            .. code-block:: python

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
