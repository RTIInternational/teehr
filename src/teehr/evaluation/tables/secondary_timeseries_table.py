"""Secondary timeseries table class."""
import teehr.const as const
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.loading.timeseries import convert_timeseries
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
from teehr.models.metrics.basemodels import (
    BaselineMethodEnum,
    ClimatologyResolutionEnum
)
from pyspark.sql import functions as F
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
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.schema_func = schemas.secondary_timeseries_schema
        self.unique_column_set = [
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
        max_workers: Union[int, None] = MAX_CPUS,
        persist_dataframe: bool = False,
        drop_duplicates: bool = True,
        **kwargs
    ):
        """Import timeseries helper."""
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.SECONDARY_TIMESERIES_DIR
        )
        # Clear the cache directory if it exists.
        remove_dir_if_exists(cache_dir)

        convert_timeseries(
            in_path=in_path,
            out_path=cache_dir,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            timeseries_type="secondary",
            pattern=pattern,
            max_workers=max_workers,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        df = self._read_files(cache_dir)

        if persist_dataframe:
            df = df.persist()

        # Add or replace location_id prefix if provided
        if location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="location_id",
                prefix=location_id_prefix,
            )
        # Validate using the _validate() method
        validated_df = self._validate(
            df=df,
            drop_duplicates=drop_duplicates
        )

        # Write to the table
        self._write_spark_df(
            validated_df,
            write_mode=write_mode
        )
        # Reload the table
        # self._load_table()

        df.unpersist()

    def _join_geometry(self):
        """Join geometry."""
        logger.debug("Joining locations geometry.")

        joined_df = self.ev.sql("""
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

    def _get_target_forecast(
        self,
        target_configuration_name: str,
        member_id: str = None
    ) -> ps.DataFrame:
        """Get a target forecast."""
        member_id = self.df.filter(
            f"configuration_name = '{target_configuration_name}'"
        ).select(F.first("member", ignorenulls=True)).first()[0]
        if member_id is not None:
            sec_sdf = self.df.filter(
                (f"configuration_name = '{target_configuration_name}'") and
                (f"member = '{member_id}'")
            )
        else:
            sec_sdf = self.df.filter(
                f"configuration_name = '{target_configuration_name}'"
            )
        if sec_sdf.isEmpty():
            raise ValueError(
                "No secondary data found for configuration:"
                f" {target_configuration_name}"
            )
        return sec_sdf

    def create_reference_forecast(
        self,
        target_configuration_name: str,
        output_configuration_name: str,
        reference_configuration_name: str = None,
        method: BaselineMethodEnum = "climatology",
        temporal_resolution: ClimatologyResolutionEnum = "day_of_year",
        aggregate_reference_timeseries: bool = False,
        time_window: str = "6 hours",
    ):
        """Create a new forecast timeseries based on a reference configuration.

        Parameters
        ----------
        reference_configuration_name : str
            Configuration name of the reference timeseries.
        target_configuration_name : str
            Configuration name of the target forecast timeseries.
        output_configuration_name : str
            Configuration name of the output forecast timeseries.
        method : BaselineMethodEnum, optional
            Method for the reference calculation,
            by default "climatology".
        temporal_resolution : ClimatologyResolutionEnum, optional
            Temporal resolution for the climatology calculation,
            by default "day_of_year".
        aggregate_reference_timeseries : bool, optional
            Whether to downsample the reference timeseries,
            by default False.
        start_hour : int, optional
            If downsample_reference_timeseries is True, the start hour
            for the rolling average calculation, by default -7
            (7 hours before the value_time).
        end_hour : int, optional
            If downsample_reference_timeseries is True, the end hour
            for the rolling average calculation, by default 0
            (the value_time itself).
        """
        self._check_load_table()
        # 1. Get the reference timeseries and target forecast.
        # If all reference_time values are null in the target secondary
        # configuration (ie, it's a historical sim), we can't continue.
        if self.df.filter(
                    f"configuration_name = '{target_configuration_name}'"
                ).select(F.first("reference_time", ignorenulls=True)).first()[0] is None:
            raise ValueError(
                "No reference_time values found in the target configuration. "
                "Please specify a valid target forecast configuration."
            )
        target_sdf = self._get_target_forecast(
            target_configuration_name=target_configuration_name
        )

        ref_type = self.ev.configurations.filter(
            f"name = '{reference_configuration_name}'"
        ).to_sdf().first()["type"]
        if ref_type == "primary":
            reference_sdf = self.ev.primary_timeseries.filter(
                f"configuration_name = '{reference_configuration_name}'"
            ).to_sdf()
            partition_by = self.ev.primary_timeseries.unique_column_set
            partition_by.remove("value_time")
        elif ref_type == "secondary":
            reference_sdf = self.df.filter(
                f"configuration_name = '{reference_configuration_name}'"
            )
            partition_by = self.ev.secondary_timeseries.unique_column_set
            partition_by.remove("value_time")
        if reference_sdf.isEmpty():
            raise ValueError(
                "No data found for the reference configuration: "
                f"{reference_configuration_name}, please calculate or load"
                " it first."
            )

        # 2. Aggregate the reference timeseries to the
        # using a rolling average if aggregate_reference_timeseries is True.
        if aggregate_reference_timeseries:
            logger.debug("Calculating rolling average for reference timeseries.")
            reference_sdf = self._calculate_rolling_average(
                sdf=reference_sdf,
                partition_by=partition_by,
                time_window=time_window
            )

        # 3. Map reference timeseries to the target forecast according
        # to the method specified.
        if method == "climatology" and temporal_resolution is None:
            raise ValueError(
                "A temporal_resolution must "
                "be provided for climatology. "
                "Please specify a valid value."
            )
        elif method == "climatology":
            time_period = self._get_time_period_rlc(
                temporal_resolution=temporal_resolution
            )
            target_sdf = time_period.apply_to(target_sdf)

            # TODO: This is redundant with climatology method?
            reference_sdf = time_period.apply_to(reference_sdf)

            # Join the reference sdf  to the template secondary forecast
            xwalk_sdf = self.ev.location_crosswalks.to_sdf()
            xwalk_sdf.createOrReplaceTempView("location_crosswalks")
            reference_sdf.createOrReplaceTempView("reference_timeseries")
            target_sdf.createOrReplaceTempView("template_timeseries")

            logger.debug("Joining reference climatology timeseries to template forecast.")
            query = f"""
                SELECT
                    tf.reference_time
                    , tf.value_time as value_time
                    , tf.location_id as location_id
                    , rf.value as value
                    , tf.unit_name
                    , tf.variable_name
                    , tf.member
                    , '{output_configuration_name}' as configuration_name
                FROM template_timeseries tf
                JOIN location_crosswalks cf
                    on cf.secondary_location_id = tf.location_id
                LEFT JOIN reference_timeseries rf
                    on cf.primary_location_id = rf.location_id
                    and tf.{time_period.output_field_name} = rf.{time_period.output_field_name}
                    and tf.unit_name = rf.unit_name
                    and tf.variable_name = rf.variable_name
                    and tf.value_time = rf.value_time
            """  # noqa
            ref_fcst_sdf = self.ev.spark.sql(query)

            logger.debug("Filling NaNs with forward fill and backward fill.")
            final_sdf = self._ffill_and_bfill_nans(ref_fcst_sdf)

            self.spark.catalog.dropTempView("location_crosswalks")
            self.spark.catalog.dropTempView("reference_timeseries")
            self.spark.catalog.dropTempView("template_timeseries")

        elif method == "persistence":
            # TODO: Implement persistence method.
            raise NotImplementedError(
                "Persistence method is not implemented yet."
            )

        # TODO: Remove when we update NaN handling?
        final_sdf = final_sdf.dropna(subset=["value"])

        self.load_dataframe(
            df=final_sdf,
            constant_field_values={
                "member": None,
            }
        )

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