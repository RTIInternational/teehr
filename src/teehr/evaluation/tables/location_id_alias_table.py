"""Location ID Alias Table."""
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.utils import (
    validate_input_is_csv,
    validate_input_is_parquet
)
from teehr.models.pandera_dataframe_schemas import location_id_aliases_schema
from pathlib import Path
from typing import List, Dict, Union
import logging
from teehr.loading.location_id_aliases import (
    convert_single_location_id_aliases
)
import pyspark.sql as ps
import pandas as pd

logger = logging.getLogger(__name__)


class LocationIdAliasTable(BaseTable):
    """Access methods to location ID aliases table.

    Maps location ID aliases (e.g., the native IDs of additional primary
    data sources) to a ``locations.id``. Primary timeseries may use either
    the location ID or an alias.
    """

    # Table metadata
    table_name = "location_id_aliases"
    uniqueness_fields = ["location_id_alias"]
    foreign_keys: List[Dict[str, str]] = [
        {
            "column": "location_id",
            "domain_table": "locations",
            "domain_column": "id",
        },
        {
            "column": "location_id_alias",
            "domain_table": "locations",
            "domain_column": "id",
            "exclude": True,
        },
    ]
    schema_func = staticmethod(location_id_aliases_schema)
    strict_validation = True
    validate_filter_field_types = True
    extraction_func = staticmethod(convert_single_location_id_aliases)
    primary_location_id_field = "location_id"
    # Load uses the secondary prefix slot for the alias column.
    secondary_location_id_field = "location_id_alias"

    def __init__(
        self,
        ev,
        table_name: str = "location_id_aliases",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBaseModel
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on. Defaults to 'location_id_aliases'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)
        self._load = ev._load

    def load_parquet(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = None,
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        location_id_prefix: str = None,
        location_id_alias_prefix: str = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import location ID aliases from parquet file format.

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
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.parquet' to search for all parquet files recursively.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        location_id_alias_prefix : str, optional
            The prefix to add to location ID aliases.
        write_mode : str, optional (default: "append")
            The write mode for the table. Options include:

            - "insert": Insert new data without checking for duplicates.
            - "append": Insert new data, skipping rows that already exist.
            - "upsert": Update existing data, insert new data.
            - "overwrite": Update table with new snapshot version preserving
              historical versions.
            - "create_or_replace": Drop and recreate the table with new data.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location ID Alias table schema includes fields:

        - location_id
        - location_id_alias
        """
        validate_input_is_parquet(in_path)
        extraction_function = extraction_function or self.extraction_func
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
            primary_location_id_prefix=location_id_prefix,
            primary_location_id_field=self.primary_location_id_field,
            secondary_location_id_prefix=location_id_alias_prefix,
            secondary_location_id_field=self.secondary_location_id_field,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
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
        location_id_prefix: str = None,
        location_id_alias_prefix: str = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import location ID aliases from CSV file format.

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
            A custom function to extract and transform the data from the input
            files to the TEEHR data model. If None (default), uses the table's
            default extraction function.
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.csv' to search for all CSV files recursively.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        location_id_alias_prefix : str, optional
            The prefix to add to location ID aliases.
        write_mode : str, optional (default: "append")
            The write mode for the table. Options include:

            - "insert": Insert new data without checking for duplicates.
            - "append": Insert new data, skipping rows that already exist.
            - "upsert": Update existing data, insert new data.
            - "overwrite": Update table with new snapshot version preserving
              historical versions.
            - "create_or_replace": Drop and recreate the table with new data.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame during validation.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location ID Alias table schema includes fields:

        - location_id
        - location_id_alias
        """ # noqa
        validate_input_is_csv(in_path)
        extraction_function = extraction_function or self.extraction_func
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
            primary_location_id_prefix=location_id_prefix,
            primary_location_id_field=self.primary_location_id_field,
            secondary_location_id_prefix=location_id_alias_prefix,
            secondary_location_id_field=self.secondary_location_id_field,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
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
        location_id_alias_prefix: str = None,
        write_mode: str = "append",
        drop_duplicates: bool = True,
    ):
        """Import data from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame]
            DataFrame to load into the table.
        namespace_name : str, optional
            The namespace name to write to. If None, uses the
            active catalog's namespace.
        catalog_name : str, optional
            The catalog name to write to. If None, uses the
            active catalog's catalog name.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        location_id_prefix : str, optional
            The prefix to add to location IDs.
            Note, the methods for fetching USGS and NWM data automatically
            prefix location IDs with "usgs" or the nwm version
            ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        location_id_alias_prefix : str, optional
            The prefix to add to location ID aliases.
        write_mode : str, optional (default: "append")
            The write mode for the table. Options include:

            - "insert": Insert new data without checking for duplicates.
            - "append": Insert new data, skipping rows that already exist.
            - "upsert": Update existing data, insert new data.
            - "overwrite": Update table with new snapshot version preserving
              historical versions.
            - "create_or_replace": Drop and recreate the table with new data.
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
            primary_location_id_prefix=location_id_prefix,
            secondary_location_id_prefix=location_id_alias_prefix,
            primary_location_id_field=self.primary_location_id_field,
            secondary_location_id_field=self.secondary_location_id_field,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
        )
        self._load_sdf()
