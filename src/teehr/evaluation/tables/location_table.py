"""Location table class."""
from typing import Union
import logging
from pathlib import Path

import pandas as pd
import pyspark.sql as ps
import geopandas as gpd

from teehr.models.table_enums import TableWriteEnum
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.locations import convert_single_locations
from teehr.querying.utils import df_to_gdf

logger = logging.getLogger(__name__)


class LocationTable(BaseTable):
    """Access methods to locations table."""

    def __init__(
        self,
        ev,
        table_name: str = "locations",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on. Defaults to 'locations'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)
        self._load = ev.load

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        gdf = df_to_gdf(self.to_pandas())
        gdf.attrs['table_type'] = self.table_name
        gdf.attrs['fields'] = self.to_sdf().columns
        return gdf

    def load_spatial(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = convert_single_locations,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        pattern: str = "**/*.parquet",
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import geometry data.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Any file format that can be read by GeoPandas.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        extraction_function : callable, optional
            A function to extract and transform the data from the input files
            to the TEEHR data model.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files.
            Only used when in_path is a directory.
        location_id_prefix : str, optional
            The prefix to add to location IDs.
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
            Additional keyword arguments are passed to GeoPandas read_file().

        File is first read by GeoPandas, field names renamed and
        then validated and inserted into the dataset.

        Notes
        -----
        The TEEHR Location Crosswalk table schema includes fields:

        - id
        - name
        - geometry

        ..note::
          The methods for fetching USGS and NWM data expect
          location IDs to be prefixed with "usgs" or the nwm version
          ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
        """
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
            drop_duplicates=drop_duplicates,
            primary_location_id_field="id",
            **kwargs
        )
        self._load_sdf()

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame, gpd.GeoDataFrame],
        namespace_name: str = None,
        catalog_name: str = None,
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
    ):
        """Load data from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame, gpd.GeoDataFrame]
            DataFrame or GeoDataFrame to load into the table.
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
        self._load.dataframe(
            df=df,
            table_name=self.table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            primary_location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            primary_location_id_field="id"
        )
        self._load_sdf()