"""Timeseries table base class."""
from pathlib import Path
from typing import Union

import pyspark.sql as ps
import pandas as pd
import logging

from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.utils import (
    validate_input_is_xml,
    validate_input_is_csv,
    validate_input_is_netcdf,
    validate_input_is_parquet
)
from teehr.const import MAX_CPUS
from teehr.loading.timeseries import convert_single_timeseries

logger = logging.getLogger(__name__)


class TimeseriesTable(BaseTable):
    """Access methods to timeseries table.

    Base class for primary and secondary timeseries tables.
    """

    # Common defaults for all timeseries tables
    strict_validation = True
    validate_filter_field_types = True
    extraction_func = staticmethod(convert_single_timeseries)
    primary_location_id_field = "location_id"

    def __init__(
        self,
        ev,
        table_name: str = None,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)
        self._load = ev.load

    def load_parquet(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: str = "append",
        parallel: bool = False,
        max_workers: Union[int, None] = MAX_CPUS,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries parquet data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            parquet file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.parquet' to search for all parquet files recursively.
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
        write_mode : str, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        parallel : bool, optional (default: False)
            Whether to process files in parallel. Default is False.
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
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
        extraction_function = extraction_function or self.extraction_func

        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        table_name = self.table_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            parallel=parallel,
            max_workers=max_workers,
            drop_duplicates=drop_duplicates,
            primary_location_id_field=self.primary_location_id_field,
            **kwargs
        )
        self._load_sdf()

    def load_csv(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: str = "append",
        parallel: bool = False,
        max_workers: Union[int, None] = MAX_CPUS,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries csv data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            csv file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
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
        write_mode : str, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        parallel : bool, optional (default: False)
            Whether to process files in parallel. Default is False.
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
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
        extraction_function = extraction_function or self.extraction_func

        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        table_name = self.table_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            parallel=parallel,
            max_workers=max_workers,
            drop_duplicates=drop_duplicates,
            primary_location_id_field=self.primary_location_id_field,
            **kwargs
        )
        self._load_sdf()

    def load_netcdf(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = "**/*.nc",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: str = "append",
        parallel: bool = False,
        max_workers: Union[int, None] = MAX_CPUS,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries netcdf data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            netcdf file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
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
        write_mode : str, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        parallel : bool, optional (default: False)
            Whether to process files in parallel. Default is False.
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
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
        extraction_function = extraction_function or self.extraction_func

        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        table_name = self.table_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            parallel=parallel,
            max_workers=max_workers,
            drop_duplicates=drop_duplicates,
            primary_location_id_field=self.primary_location_id_field,
            **kwargs
        )
        self._load_sdf()

    def load_fews_xml(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
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
        write_mode: str = "append",
        parallel: bool = False,
        max_workers: Union[int, None] = MAX_CPUS,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries from FEWS PI-XML data format.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            xml file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
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
        write_mode : str, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        parallel : bool, optional (default: False)
            Whether to process files in parallel. Default is False.
        max_workers : Union[int, None], optional
            The maximum number of workers to use for parallel processing when
            in_path is a directory. This gets passed to the concurrent.futures
            ProcessPoolExecutor. If in_path is a file, this parameter is ignored.
            The default value is max(os.cpu_count() - 1, 1).
            If None, os.process_cpu_count() is used.
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
        extraction_function = extraction_function or self.extraction_func

        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        table_name = self.table_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            parallel=parallel,
            max_workers=max_workers,
            drop_duplicates=drop_duplicates,
            primary_location_id_field=self.primary_location_id_field,
            **kwargs
        )
        self._load_sdf()

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        namespace_name: str = None,
        catalog_name: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
    ):
        """Load data from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame]
            Pandas or PySparkDataFrame to load into the table.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : str, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the dataframe.
        """ # noqa
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        table_name = self.table_name

        self._load.dataframe(
            df=df,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=location_id_prefix,
            primary_location_id_field=self.primary_location_id_field,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
        )
        self._load_sdf()
