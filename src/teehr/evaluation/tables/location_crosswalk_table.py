"""Location Crosswalk Table."""
from teehr.evaluation.tables.base_table import Table
from teehr.loading.utils import (
    validate_input_is_csv,
    validate_input_is_parquet
)
from teehr.querying.utils import join_geometry
from pathlib import Path
from typing import Union
import logging
from teehr.models.table_enums import TableWriteEnum
from teehr.loading.location_crosswalks import (
    convert_single_location_crosswalks
)
import pyspark.sql as ps
import pandas as pd

logger = logging.getLogger(__name__)


class LocationCrosswalkTable(Table):
    """Access methods to location crosswalks table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self._load = ev.load

    def __call__(
        self,
        table_name: str = "location_crosswalks",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the location crosswalks table.

        Note
        ----
        Creates an instance of a Table class with 'location_crosswalks'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = join_geometry(
            self.sdf, self._ev.locations.to_sdf(),
            "primary_location_id"
        )
        gdf.attrs['table_type'] = self.table_name
        gdf.attrs['fields'] = self.fields()
        return gdf

    def load_parquet(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = convert_single_location_crosswalks,
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import location crosswalks from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A function to extract and transform the data from the input files
            to the TEEHR data model.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.parquet' to search for all parquet files recursively.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        primary_location_id_prefix : str, optional
            The prefix to add to primary location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        secondary_location_id_prefix : str, optional
            The prefix to add to secondary location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location Crosswalk table schema includes fields:

        - primary_location_id
        - secondary_location_id
        """
        validate_input_is_parquet(in_path)
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            primary_location_id_prefix=primary_location_id_prefix,
            primary_location_id_field="primary_location_id",
            secondary_location_id_prefix=secondary_location_id_prefix,
            secondary_location_id_field="secondary_location_id",
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_table()

    def load_csv(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = convert_single_location_crosswalks,
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import location crosswalks from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A function to extract and transform the data from the input files
            to the TEEHR data model.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.csv' to search for all CSV files recursively.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        primary_location_id_prefix : str, optional
            The prefix to add to primary location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        secondary_location_id_prefix : str, optional
            The prefix to add to secondary location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location Crosswalk table schema includes fields:

        - primary_location_id
        - secondary_location_id
        """ # noqa
        validate_input_is_csv(in_path)
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        self._load.file(
            in_path=in_path,
            pattern=pattern,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            extraction_function=extraction_function,
            field_mapping=field_mapping,
            primary_location_id_prefix=primary_location_id_prefix,
            primary_location_id_field="primary_location_id",
            secondary_location_id_prefix=secondary_location_id_prefix,
            secondary_location_id_field="secondary_location_id",
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            **kwargs
        )
        self._load_table()

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        namespace_name: str = None,
        catalog_name: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
    ):
        """Import data from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame]
            DataFrame to load into the table.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        primary_location_id_prefix : str, optional
            The prefix to add to primary location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        secondary_location_id_prefix : str, optional
            The prefix to add to secondary location IDs.
            Used to ensure unique location IDs across configurations.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table.
            Options are "append", "upsert", and "create_or_replace".
            If "append", the table will be appended without checking
            existing data.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "create_or_replace", a new table will be created or an existing
            table will be replaced.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        """ # noqa
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name
        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name

        self._load.dataframe(
            df=df,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=primary_location_id_prefix,
            secondary_location_id_prefix=secondary_location_id_prefix,
            primary_location_id_field="primary_location_id",
            secondary_location_id_field="secondary_location_id",
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
        )
        self._load_table()