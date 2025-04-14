import teehr.const as const
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.location_crosswalks import convert_location_crosswalks
from teehr.loading.utils import validate_input_is_csv, validate_input_is_parquet
from teehr.models.filters import LocationCrosswalkFilter
from teehr.models.table_enums import LocationCrosswalkFields
from teehr.querying.utils import join_geometry
import teehr.models.pandera_dataframe_schemas as schemas
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.models.table_enums import TableWriteEnum
from teehr.loading.utils import add_or_replace_sdf_column_prefix


logger = logging.getLogger(__name__)


class LocationCrosswalkTable(BaseTable):
    """Access methods to location crosswalks table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "location_crosswalks"
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.format = "parquet"
        self.filter_model = LocationCrosswalkFilter
        self.schema_func = schemas.location_crosswalks_schema
        self.unique_column_set = [
            "primary_location_id",
            "secondary_location_id"
        ]

    def _load(
        self,
        in_path: Union[Path, str],
        field_mapping: dict = None,
        pattern: str = None,
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        **kwargs
    ):
        """Load location crosswalks helper."""
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.LOCATION_CROSSWALKS_DIR
        )
        # Clear the cache directory if it exists.
        remove_dir_if_exists(cache_dir)

        convert_location_crosswalks(
            in_path,
            cache_dir,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )

        # Read the converted files to Spark DataFrame
        df = self._read_files(cache_dir)

        # Add or replace primary location_id prefix if provided
        if primary_location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="primary_location_id",
                prefix=primary_location_id_prefix,
            )

        # Add or replace secondary location_id prefix if provided
        if secondary_location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="secondary_location_id",
                prefix=secondary_location_id_prefix,
            )

        # Validate using the validate method
        validated_df = self._validate(df)

        # Write to the table df.rdd.getNumPartitions()
        self._write_spark_df(
            df=validated_df,
            num_partitions=df.rdd.getNumPartitions(),
            write_mode=write_mode,
        )

        # Reload the table
        self._load_table()

    def _get_schema(self, type: str = "pyspark"):
        """Get the location crosswalk schema."""
        if type == "pandas":
            return self.schema_func(type="pandas")

        location_ids = self.ev.locations.distinct_values("id")
        return self.schema_func(location_ids=location_ids)

    def field_enum(self) -> LocationCrosswalkFields:
        """Get the location crosswalk fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return LocationCrosswalkFields(
            "LocationCrosswalkFields",
            {field: field for field in fields}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Crosswalk."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = self.name
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = join_geometry(
            self.df, self.ev.locations.to_sdf(),
            "primary_location_id"
        )
        gdf.attrs['table_type'] = self.name
        gdf.attrs['fields'] = self.fields()
        return gdf

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        **kwargs
    ):
        """Import location crosswalks from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
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
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are
            overwritten.
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
        self._load(
            in_path=in_path,
            field_mapping=field_mapping,
            pattern=pattern,
            primary_location_id_prefix=primary_location_id_prefix,
            secondary_location_id_prefix=secondary_location_id_prefix,
            write_mode=write_mode,
            **kwargs
        )
        self._load_table()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        primary_location_id_prefix: str = None,
        secondary_location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        **kwargs
    ):
        """Import location crosswalks from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
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
            Options are "append", "upsert", and "overwrite".
            If "append", the table will be appended with new data that does
            already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
            If "overwrite", existing partitions receiving new data are overwritten
        **kwargs
            Additional keyword arguments are passed to pd.read_csv()
            or pd.read_parquet().

        Notes
        -----
        The TEEHR Location Crosswalk table schema includes fields:

        - primary_location_id
        - secondary_location_id
        """
        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            field_mapping=field_mapping,
            pattern=pattern,
            primary_location_id_prefix=primary_location_id_prefix,
            secondary_location_id_prefix=secondary_location_id_prefix,
            write_mode=write_mode,
            **kwargs
        )
        self._load_table()