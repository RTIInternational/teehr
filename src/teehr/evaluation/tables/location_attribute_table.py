"""Location Attribute Table class."""
import teehr.const as const
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.location_attributes import convert_location_attributes
from teehr.loading.utils import validate_input_is_csv, validate_input_is_parquet
from teehr.models.filters import LocationAttributeFilter
from teehr.models.table_enums import LocationAttributeFields
from teehr.querying.utils import join_geometry
import teehr.models.pandera_dataframe_schemas as schemas
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.loading.utils import add_or_replace_sdf_column_prefix
from teehr.models.table_enums import TableWriteEnum
import pyspark.sql as ps
import pandas as pd
from teehr.models.pydantic_table_models import Attribute


logger = logging.getLogger(__name__)


class LocationAttributeTable(BaseTable):
    """Access methods to location attributes table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "location_attributes"
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.format = "parquet"
        self.filter_model = LocationAttributeFilter
        self.schema_func = schemas.location_attributes_schema
        self.unique_column_set = [
            "location_id",
            "attribute_name"
        ]
        self.foreign_keys = [
            {
                "column": "location_id",
                "domain_table": "locations",
                "domain_column": "id",
            },
            {
                "column": "attribute_name",
                "domain_table": "attributes",
                "domain_column": "name",
            }
        ]

    def _load(
        self,
        in_path: Union[Path, str],
        pattern: str = None,
        field_mapping: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        update_attrs_table: bool = True,
        **kwargs
    ):
        """Load location attributes helper."""
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.LOCATION_ATTRIBUTES_DIR
        )
        # Clear the cache directory if it exists.
        remove_dir_if_exists(cache_dir)

        convert_location_attributes(
            in_path,
            cache_dir,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        df = self._read_files(cache_dir)

        # Add or replace location_id prefix if provided
        if location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="location_id",
                prefix=location_id_prefix,
            )

        if update_attrs_table:
            attr_names = [
                row.attribute_name for row in
                df.select("attribute_name").distinct().collect()
            ]
            attr_list = []
            for attr_name in attr_names:
                attr_list.append(
                    Attribute(
                        name=attr_name,
                        type="continuous",
                        description=f"{attr_name} default description"
                    )
                )
            self.ev.attributes.add(attr_list)

        # Validate using the _validate() method
        validated_df = self._validate(
            df=df,
            drop_duplicates=drop_duplicates
        )

        # Write to the table df.rdd.getNumPartitions()
        self._write_spark_df(
            df=validated_df,
            num_partitions=df.rdd.getNumPartitions(),
            write_mode=write_mode
        )

        # Reload the table
        # self._load_table()

    def field_enum(self) -> LocationAttributeFields:
        """Get the location attribute fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return LocationAttributeFields(
            "LocationAttributeFields",
            {field: field for field in fields}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Attributes."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = self.name
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = join_geometry(self.df, self.ev.locations.to_sdf())
        gdf.attrs['table_type'] = self.name
        gdf.attrs['fields'] = self.fields()
        return gdf

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        update_attrs_table: bool = True,
        **kwargs
    ):
        """Import location_attributes from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
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
        update_attrs_table : bool, optional (default: True)
            Whether to add default attributes for the location attributes.
            If True, it will add default attributes for each unique attribute
            name found in the data with category="continuous" and the
            default description "<attribute_name> default description".
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Notes
        -----
        The TEEHR Location Attribute table schema includes fields:

        - location_id
        - attribute_name
        - value
        """
        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            update_attrs_table=update_attrs_table,
            **kwargs
        )
        self._load_table()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        update_attrs_table: bool = True,
        **kwargs
    ):
        """Import location_attributes from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
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
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the DataFrame.
        update_attrs_table : bool, optional (default: True)
            Whether to add default attributes for the location attributes.
            If True, it will add default attributes for each unique attribute
            name found in the data with category="continuous" and the
            default description "<attribute_name> default description".
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Notes
        -----
        The TEEHR Location Attribute table schema includes fields:

        - location_id
        - attribute_name
        - value
        """
        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            update_attrs_table=update_attrs_table,
            **kwargs
        )
        self._load_table()

    def load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        persist_dataframe: bool = False,
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
            If "overwrite", existing partitions receiving new data are overwritten.
        persist_dataframe : bool, optional (default: False)
            Whether to repartition and persist the pyspark dataframe after
            reading from the cache. This can improve performance when loading
            a large number of files from the cache.
        drop_duplicates : bool, optional (default: True)
            Whether to drop duplicates from the dataframe.
        """
        self._load_dataframe(
            df=df,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            location_id_prefix=location_id_prefix,
            write_mode=write_mode,
            persist_dataframe=persist_dataframe,
            drop_duplicates=drop_duplicates
        )

        self._load_table()