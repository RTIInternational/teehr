import teehr.const as const
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.locations import convert_locations
from teehr.models.filters import LocationFilter
from teehr.models.table_enums import LocationFields
from teehr.querying.utils import df_to_gdf
import teehr.models.pandera_dataframe_schemas as schemas
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.loading.utils import add_or_replace_sdf_column_prefix
from teehr.models.table_enums import TableWriteEnum


logger = logging.getLogger(__name__)


class LocationTable(BaseTable):
    """Access methods to locations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "locations"
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.format = "parquet"
        self.save_mode = "append"
        self.filter_model = LocationFilter
        self.schema_func = schemas.locations_schema
        self.unique_columns = [
            "id",
            "name",
            "geometry",
        ]

    def field_enum(self) -> LocationFields:
        """Get the location fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return LocationFields(
            "LocationFields",
            {field: field for field in fields}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Table."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = 'locations'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = df_to_gdf(self.to_pandas())
        gdf.attrs['table_type'] = self.name
        gdf.attrs['fields'] = self.fields()
        return gdf

    def load_spatial(
        self,
        in_path: Union[Path, str],
        field_mapping: dict = None,
        pattern: str = "**/*.parquet",
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        update_columns: list[str] = None,
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
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table. Options are "append" or "upsert".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
        update_columns : list[str], optional
            When ``write_mode`` is "upsert", the names of columns containing
            data to be updated.
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
        """
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.LOCATIONS_DIR
        )
        # Clear the cache directory if it exists.
        remove_dir_if_exists(cache_dir)

        convert_locations(
            in_path,
            cache_dir,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        df = self._read_files(cache_dir)

        # Validate using the validate method
        validated_df = self._validate(df)

        # Add location_id prefix if provided
        if location_id_prefix:
            validated_df = add_or_replace_sdf_column_prefix(
                sdf=validated_df,
                column_name="id",
                prefix=location_id_prefix,
            )

        # Write to the table
        self._write_spark_df(
            validated_df,
            write_mode=write_mode,
            update_columns=update_columns,
            num_partitions=1
        )

        # Reload the table
        self._load_table()