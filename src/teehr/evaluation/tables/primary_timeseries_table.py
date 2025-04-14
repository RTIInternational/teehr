"""Primary timeseries table class."""
import teehr.const as const
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.models.table_enums import TimeseriesFields
from teehr.loading.timeseries import convert_timeseries
import teehr.models.pandera_dataframe_schemas as schemas
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.models.table_enums import TableWriteEnum
from teehr.loading.utils import add_or_replace_sdf_column_prefix


logger = logging.getLogger(__name__)


class PrimaryTimeseriesTable(TimeseriesTable):
    """Access methods to primary timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "primary_timeseries"
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.schema_func = schemas.primary_timeseries_schema
        self.partition_by = [
            "configuration_name",
            "variable_name",
        ]

    def field_enum(self) -> TimeseriesFields:
        """Get the timeseries fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return TimeseriesFields(
            "TimeseriesFields",
            {field: field for field in fields}
        )

    def _get_schema(self, type: str = "pyspark"):
        """Get the primary timeseries schema."""
        if type == "pandas":
            return self.schema_func(type="pandas")

        location_ids = self.ev.locations.distinct_values("id")
        variable_names = self.ev.variables.distinct_values("name")
        configuration_names = self.ev.configurations.distinct_values("name")
        unit_names = self.ev.units.distinct_values("name")
        return self.schema_func(
            location_ids=location_ids,
            variable_names=variable_names,
            configuration_names=configuration_names,
            unit_names=unit_names
        )

    def _load(
        self,
        in_path: Union[Path, str],
        pattern="**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        location_id_prefix: str = None,
        write_mode: TableWriteEnum = "append",
        **kwargs
    ):
        """Import timeseries helper."""
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.PRIMARY_TIMESERIES_DIR
        )
        # Clear the cache directory if it exists.
        remove_dir_if_exists(cache_dir)

        convert_timeseries(
            in_path=in_path,
            out_path=cache_dir,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            timeseries_type="primary",
            pattern=pattern,
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

        # Validate using the _validate() method
        validated_df = self._validate(df)

        # Write to the table
        self._write_spark_df(validated_df, write_mode=write_mode)

        # Reload the table
        self._load_table()