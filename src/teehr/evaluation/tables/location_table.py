"""Location table class."""
from typing import Union
import logging
from pathlib import Path

from teehr.models.table_enums import TableWriteEnum
from teehr.evaluation.tables.generic_table import Table
from teehr.loading.locations import convert_single_locations

logger = logging.getLogger(__name__)


class LocationTable(Table):
    """Access methods to locations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self._load = ev.load

    def __call__(
        self,
        table_name: str = "locations",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the locations table.

        Note
        ----
        Creates an instance of a Table class with 'locations'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )

    # def __init__(self, ev):
    #     """Initialize class."""
    #     super().__init__(ev)
    #     self.table_name = "locations"
    #     self.dir = to_path_or_s3path(ev.active_catalog.dataset_dir, self.table_name)
    #     self.format = "parquet"
    #     self.filter_model = LocationFilter
    #     self.schema_func = schemas.locations_schema
    #     self.uniqueness_fields = ["id"]
    #     self.cache_dir = Path(
    #         ev.active_catalog.cache_dir,
    #         const.LOADING_CACHE_DIR,
    #         const.LOCATIONS_DIR
    #     )

    # def field_enum(self) -> LocationFields:
    #     """Get the location fields enum."""
    #     fields = self._get_schema("pandas").columns.keys()
    #     return LocationFields(
    #         "LocationFields",
    #         {field: field for field in fields}
    #     )

    # def to_pandas(self):
    #     """Return Pandas DataFrame for Location Table."""
    #     self._check_load_table()
    #     df = self.sdf.toPandas()
    #     df.attrs['table_type'] = self.table_name
    #     df.attrs['fields'] = self.fields()
    #     return df

    # def to_geopandas(self):
    #     """Return GeoPandas DataFrame."""
    #     self._check_load_table()
    #     gdf = df_to_gdf(self.to_pandas())
    #     gdf.attrs['table_type'] = self.table_name
    #     gdf.attrs['fields'] = self.fields()
    #     return gdf

        # namespace_name: str = None,
        # catalog_name: str = None,
        # extraction_function: callable = convert_single_timeseries,
        # pattern: str = "**/*.parquet",
        # field_mapping: dict = None,
        # constant_field_values: dict = None,
        # location_id_prefix: str = None,
        # write_mode: TableWriteEnum = "append",
        # parallel: bool = False,
        # max_workers: Union[int, None] = MAX_CPUS,
        # persist_dataframe: bool = False,
        # drop_duplicates: bool = True,
        # location_id_field: str = "location_id",

    def load_spatial(
        self,
        in_path: Union[Path, str],
        namespace_name: str = None,
        catalog_name: str = None,
        extraction_function: callable = convert_single_locations,
        field_mapping: dict = None,
        pattern: str = "**/*.parquet",
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        location_id_field: str = "location_id",
        **kwargs
    ):
        """Import geometry data.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Any file format that can be read by GeoPandas.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
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
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are
            overwritten.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame.
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
            # constant_field_values=constant_field_values,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            # parallel=parallel,
            # max_workers=max_workers,
            # persist_dataframe=persist_dataframe,
            drop_duplicates=drop_duplicates,
            location_id_field=location_id_field,
            **kwargs
        )
        self._load_table()


        # # Clear the cache directory if it exists.
        # remove_dir_if_exists(self.cache_dir)

        # self._ev.extract.to_cache(
        #     in_datapath=in_path,
        #     field_mapping=field_mapping,
        #     pattern=pattern,
        #     cache_dir=self.cache_dir,
        #     table_fields=self.fields(),
        #     table_schema_func=self.schema_func(type="pandas"),
        #     write_schema_func=self.schema_func(type="arrow"),
        #     extraction_func=convert_single_locations,
        #     **kwargs
        # )

        # # Read the converted files to Spark DataFrame
        # df = self._read.from_cache(
        #     path=self.cache_dir,
        #     table_schema_func=self.schema_func(),
        # ).to_sdf()

        # # Add or replace location_id prefix if provided
        # if location_id_prefix:
        #     df = add_or_replace_sdf_column_prefix(
        #         sdf=df,
        #         column_name="id",
        #         prefix=location_id_prefix,
        #     )

        # validated_df = self._ev.validate.schema(
        #     sdf=df,
        #     table_schema=self.schema_func(),
        #     drop_duplicates=drop_duplicates,
        #     foreign_keys=self.foreign_keys,
        #     uniqueness_fields=self.uniqueness_fields
        # )

        # self._ev.write.to_warehouse(
        #     source_data=validated_df,
        #     table_name=self.table_name,
        #     write_mode=write_mode,
        #     uniqueness_fields=self.uniqueness_fields
        # )

    # def _load_dataframe(
    #     self,
    #     df: Union[pd.DataFrame, ps.DataFrame, gpd.GeoDataFrame],
    #     field_mapping: dict,
    #     constant_field_values: dict,
    #     location_id_prefix: str,
    #     write_mode: TableWriteEnum,
    #     persist_dataframe: bool,
    #     drop_duplicates: bool
    # ):
    #     """Load a timeseries from an in-memory dataframe."""
    #     default_field_mapping = {}
    #     fields = self.schema_func(type="pandas").columns.keys()
    #     for field in fields:
    #         if field not in default_field_mapping.values():
    #             default_field_mapping[field] = field
    #     if field_mapping:
    #         logger.debug(
    #             "Merging user field_mapping with default field mapping."
    #         )
    #         field_mapping = merge_field_mappings(
    #             default_field_mapping,
    #             field_mapping
    #         )
    #     else:
    #         logger.debug("Using default field mapping.")
    #         field_mapping = default_field_mapping
    #     # verify constant_field_values keys are in field_mapping values
    #     if constant_field_values:
    #         validate_constant_values_dict(
    #             constant_field_values,
    #             field_mapping.values()
    #         )

    #     # Convert the input DataFrame to Spark DataFrame
    #     if isinstance(df, gpd.GeoDataFrame):
    #         tmp_df = df.to_wkb()
    #         df = self.spark.createDataFrame(tmp_df)
    #     elif isinstance(df, pd.DataFrame):
    #         df = self.spark.createDataFrame(df)
    #     elif not isinstance(df, ps.DataFrame):
    #         raise TypeError(
    #             "Input dataframe must be a Pandas DataFrame, "
    #             "a PySpark DataFrame, or a GeoPandas DataFrame."
    #         )
    #     # Apply field mapping and constant field values
    #     df = df.withColumnsRenamed(field_mapping)

    #     if constant_field_values:
    #         for field, value in constant_field_values.items():
    #             df = df.withColumn(field, lit(value))

    #     if persist_dataframe:
    #         df = df.persist()

    #     if location_id_prefix:
    #         df = add_or_replace_sdf_column_prefix(
    #             sdf=df,
    #             column_name="location_id",
    #             prefix=location_id_prefix,
    #         )
    #     validated_df = self._ev.validate.schema(
    #         sdf=df,
    #         table_schema=self.schema_func(),
    #         drop_duplicates=drop_duplicates,
    #         foreign_keys=self.foreign_keys,
    #         uniqueness_fields=self.uniqueness_fields
    #     )
    #     self._ev.write.to_warehouse(
    #         source_data=validated_df,
    #         table_name=self.table_name,
    #         write_mode=write_mode,
    #         uniqueness_fields=self.uniqueness_fields
    #     )

    #     df.unpersist()

    # def load_dataframe(
    #     self,
    #     df: Union[pd.DataFrame, ps.DataFrame, gpd.GeoDataFrame],
    #     field_mapping: dict = None,
    #     constant_field_values: dict = None,
    #     location_id_prefix: str = None,
    #     write_mode: TableWriteEnum = "append",
    #     persist_dataframe: bool = False,
    #     drop_duplicates: bool = True,
    # ):
    #     """Import data from an in-memory dataframe.

    #     Parameters
    #     ----------
    #     df : Union[pd.DataFrame, ps.DataFrame, gpd.GeoDataFrame]
    #         DataFrame or GeoDataFrame to load into the table.
    #     field_mapping : dict, optional
    #         A dictionary mapping input fields to output fields.
    #         Format: {input_field: output_field}
    #     constant_field_values : dict, optional
    #         A dictionary mapping field names to constant values.
    #         Format: {field_name: value}.
    #     location_id_prefix : str, optional
    #         The prefix to add to location IDs.
    #         Used to ensure unique location IDs across configurations.
    #         Note, the methods for fetching USGS and NWM data automatically
    #         prefix location IDs with "usgs" or the nwm version
    #         ("nwm12, "nwm21", "nwm22", or "nwm30"), respectively.
    #     write_mode : TableWriteEnum, optional (default: "append")
    #         The write mode for the table.
    #         Options are "append", "upsert", and "overwrite".
    #         If "append", the table will be appended with new data that does
    #         already exist.
    #         If "upsert", existing data will be replaced and new data that
    #         does not exist will be appended.
    #         If "overwrite", existing partitions receiving new data are overwritten.
    #     persist_dataframe : bool, optional (default: False)
    #         Whether to repartition and persist the pyspark dataframe after
    #         reading from the cache. This can improve performance when loading
    #         a large number of files from the cache.
    #     drop_duplicates : bool, optional (default: True)
    #         Whether to drop duplicates from the dataframe.
    #     """ # noqa
    #     self._load_dataframe(
    #         df=df,
    #         field_mapping=field_mapping,
    #         constant_field_values=constant_field_values,
    #         location_id_prefix=location_id_prefix,
    #         write_mode=write_mode,
    #         persist_dataframe=persist_dataframe,
    #         drop_duplicates=drop_duplicates
    #     )
    #     self._load_table()