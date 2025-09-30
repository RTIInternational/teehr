"""Secondary timeseries table class."""
import teehr.const as const
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.loading.timeseries import convert_single_timeseries
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.loading.utils import (
    add_or_replace_sdf_column_prefix
)
import teehr.models.pandera_dataframe_schemas as schemas
from teehr.models.table_enums import TimeseriesFields
from teehr.models.table_enums import TableWriteEnum
import pyspark.sql as ps
import pandas as pd

from teehr.const import MAX_CPUS
from teehr.querying.utils import df_to_gdf


logger = logging.getLogger(__name__)


class SecondaryTimeseriesTable(TimeseriesTable):
    """Access methods to secondary timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "secondary_timeseries"
        self.dir = to_path_or_s3path(ev.active_catalog.dataset_dir, self.name)
        self.schema_func = schemas.secondary_timeseries_schema
        self.uniqueness_fields = [
            "location_id",
            "value_time",
            "reference_time",
            "variable_name",
            "unit_name",
            "member",
            "configuration_name",
        ]
        self.foreign_keys = [
            {
                "column": "variable_name",
                "domain_table": "variables",
                "domain_column": "name",
            },
            {
                "column": "unit_name",
                "domain_table": "units",
                "domain_column": "name",
            },
            {
                "column": "configuration_name",
                "domain_table": "configurations",
                "domain_column": "name",
            },
            {
                "column": "location_id",
                "domain_table": "location_crosswalks",
                "domain_column": "secondary_location_id",
            }
        ]
        self.cache_dir = Path(
            self._ev.active_catalog.cache_dir,
            const.LOADING_CACHE_DIR,
            const.SECONDARY_TIMESERIES_DIR
        )

    def field_enum(self) -> TimeseriesFields:
        """Get the timeseries fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return TimeseriesFields(
            "TimeseriesFields",
            {field: field for field in fields}
        )

    def _load(
        self,
        in_path: Union[Path, str],
        pattern="**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        persist_dataframe: bool = False,
        drop_duplicates: bool = True,
        parallel: bool = False,
        max_workers: Union[int, None] = MAX_CPUS,
        **kwargs
    ):
        """Import timeseries helper."""
        # Clear the cache directory if it exists.
        remove_dir_if_exists(self.cache_dir)

        self._extract.to_cache(
            in_datapath=in_path,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            pattern=pattern,
            cache_dir=self.cache_dir,
            table_fields=self.fields(),
            table_schema_func=self.schema_func(type="pandas"),
            write_schema_func=self.schema_func(type="arrow"),
            extraction_func=convert_single_timeseries,
            parallel=parallel,
            max_workers=max_workers,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        # df = self._read_files_from_cache_or_s3(self.cache_dir)
        df = self._read.from_cache(
            path=self.cache_dir,
            pattern=pattern,
            file_format=self.format,
            show_missing_table_warning=False,
            table_schema_func=self.schema_func()
        )

        if persist_dataframe:
            df = df.persist()

        # Add or replace location_id prefix if provided
        if location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="location_id",
                prefix=location_id_prefix,
            )
        validated_df = self._validate.schema(
            sdf=df,
            table_schema=self.schema_func(),
            drop_duplicates=drop_duplicates,
            foreign_keys=self.foreign_keys,
            uniqueness_fields=self.uniqueness_fields,
            add_missing_columns=True
        )
        self._write.to_warehouse(
            source_data=validated_df,
            target_table=self.name,
            write_mode=write_mode,
            uniqueness_fields=self.uniqueness_fields
        )

        df.unpersist()

    def _join_geometry(self):
        """Join geometry."""
        logger.debug("Joining locations geometry.")

        joined_df = self._ev.sql("""
            SELECT
                sf.*,
                lf.geometry as geometry
            FROM secondary_timeseries sf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = sf.location_id
            JOIN locations lf
                on cf.primary_location_id = lf.id
        """,
        create_temp_views=["secondary_timeseries", "location_crosswalks", "locations"])
        return df_to_gdf(joined_df.toPandas())

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        return self._join_geometry()

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
        """Import secondary timeseries from an in-memory dataframe.

        Parameters
        ----------
        df : Union[pd.DataFrame, ps.DataFrame]
            DataFrame to load into the secondary timeseries table.
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

        Notes
        -----
        The TEEHR secondary timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        - member
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