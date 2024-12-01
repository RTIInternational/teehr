import sys
from pathlib import Path
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
# from teehr.loading.joined_timeseries import create_joined_timeseries_dataset
from teehr.models.filters import JoinedTimeseriesFilter
from teehr.models.table_enums import JoinedTimeseriesFields
from teehr.querying.utils import join_geometry
import teehr.models.pandera_dataframe_schemas as schemas
import pyspark.sql as ps
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path, path_to_spark
from typing import Union


import logging

logger = logging.getLogger(__name__)


class JoinedTimeseriesTable(TimeseriesTable):
    """Access methods to joined timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.joined_timeseries_dir
        self.filter_model = JoinedTimeseriesFilter
        # self.table_model = JoinedTimeseriesTable
        self.validate_filter_field_types = False
        self.strict_validation = False
        self.schema_func = schemas.joined_timeseries_schema
        # self._load_table()

    def field_enum(self) -> JoinedTimeseriesFields:
        """Get the joined timeseries fields enum."""
        self._check_load_table()
        fields_list = self.df.columns
        return JoinedTimeseriesFields(
            "JoinedTimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Joined Timeseries."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = 'joined_timeseries'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        return join_geometry(
            self.df, self.ev.locations.to_sdf(),
            "primary_location_id"
        )

    # def _get_schema(self, type: str = "pyspark"):
    #     """Get the primary timeseries schema."""
    #     if type == "pandas":
    #         return self.schema_func(type="pandas")

    #     return self.schema_func()

    # def _read_files(
    #         self,
    #         path: Union[str, Path, S3Path],
    #         pattern: str = None,
    #         **options
    # ) -> ps.DataFrame:
    #     """Read data from directory as a spark dataframe."""
    #     logger.info(f"Reading files from {path}.")
    #     if len(options) == 0:
    #         options = {
    #             "header": "true",
    #             "ignoreMissingFiles": "true"
    #         }

    #     path = to_path_or_s3path(path)

    #     path = path_to_spark(path, pattern)

    #     # May need to deal with empty files here.
    #     df = (
    #         self.ev.spark.read.format(self.format)
    #         # .schema(self._get_schema().to_structtype())
    #         .options(**options)
    #         .load(path)
    #     )

    #     return df

    def _join(self) -> ps.DataFrame:
        """Join primary and secondary timeseries."""
        joined_df = self.ev.sql("""
            SELECT
                sf.reference_time
                , sf.value_time as value_time
                , pf.location_id as primary_location_id
                , sf.location_id as secondary_location_id
                , pf.value as primary_value
                , sf.value as secondary_value
                , sf.configuration_name
                , sf.unit_name
                , sf.variable_name
            FROM secondary_timeseries sf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = sf.location_id
            JOIN primary_timeseries pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.unit_name = pf.unit_name
                and sf.variable_name = pf.variable_name
        """,
        create_temp_views=["secondary_timeseries", "location_crosswalks", "primary_timeseries"])
        return joined_df

    def _add_attr(self, joined_df: ps.DataFrame) -> ps.DataFrame:
        """Add attributes to the joined timeseries dataframe."""

        location_attributes_df = self.ev.location_attributes.to_sdf()

        joined_df.createTempView("joined")

        # Get distinct attribute names
        distinct_attributes = self.ev.location_attributes.distinct_values("attribute_name")

        # Pivot the table
        pivot_df = (
            location_attributes_df.groupBy("location_id")
            .pivot("attribute_name", distinct_attributes).agg({"value": "max"})
        )

        # Create a view
        pivot_df.createTempView("attrs")

        joined_df = self.spark.sql("""
            SELECT
                joined.*
                , attrs.*
            FROM joined
            JOIN attrs
                on joined.primary_location_id = attrs.location_id
        """)

        self.spark.catalog.dropTempView("joined")
        self.spark.catalog.dropTempView("attrs")

        return joined_df

    def _add_udfs(self, joined_df: ps.DataFrame) -> ps.DataFrame:
        """Add UDFs to the joined timeseries dataframe."""

        try:
            sys.path.append(str(Path(self.ev.scripts_dir).resolve()))
            import user_defined_fields as udf # noqa
            joined_df = udf.add_user_defined_fields(joined_df)
        except ImportError:
            logger.info(
                f"No user-defined fields found in {self.ev.scripts_dir}."
                "Not adding user-defined fields."
            )
            return joined_df

        return joined_df

    def create(self, execute_udf: bool = False):
        """Create joined timeseries table.

        Parameters
        ----------
        execute_udf : bool, optional
            Execute UDFs, by default False
        """
        joined_df = self._join()
        joined_df = self._add_attr(joined_df)

        if execute_udf:
            joined_df = self._add_udfs(joined_df)

        validated_df = self._validate(joined_df, False)

        self._write_spark_df(validated_df)
        logger.info("Joined timeseries table created.")
        self._load_table()