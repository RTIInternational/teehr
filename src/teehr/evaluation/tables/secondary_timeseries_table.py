"""Secondary timeseries table class."""
import teehr.const as const
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.loading.timeseries import convert_timeseries
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.loading.utils import add_or_replace_sdf_column_prefix
from teehr.querying.utils import group_df
from teehr.models.calculated_fields.row_level import RowLevelCalculatedFields as rlc
import teehr.models.pandera_dataframe_schemas as schemas
from teehr.models.table_enums import TimeseriesFields
from teehr.models.table_enums import TableWriteEnum
from teehr.models.metrics.basemodels import (
    BaselineMethodEnum,
    ClimatologyResolutionEnum,
    ClimatologyStatisticEnum
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.sql as ps
from teehr.models.pydantic_table_models import (
    Configuration
)
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
        self._load_table()

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

    def _calculate_climatology(
        self,
        primary_configuration_name: str,
        temporal_resolution: ClimatologyResolutionEnum,
        summary_statistic: ClimatologyStatisticEnum = "mean",
    ) -> ps.DataFrame:
        """Calculate climatology."""
        if temporal_resolution == ClimatologyResolutionEnum.day_of_year:
            time_period = rlc.DayOfYear()
        elif temporal_resolution == ClimatologyResolutionEnum.hour_of_year:
            time_period = rlc.HourOfYear()
        elif temporal_resolution == ClimatologyResolutionEnum.month:
            time_period = rlc.Month()
        elif temporal_resolution == ClimatologyResolutionEnum.year:
            time_period = rlc.Year()
        elif temporal_resolution == ClimatologyResolutionEnum.water_year:
            time_period = rlc.WaterYear()
        elif temporal_resolution == ClimatologyResolutionEnum.season:
            time_period = rlc.Seasons()

        if summary_statistic == "mean":
            summary_func = F.mean
        elif summary_statistic == "median":
            summary_func = F.expr("percentile_approx(value, 0.5)")
        elif summary_statistic == "max":
            summary_func = F.max
        elif summary_statistic == "min":
            summary_func = F.min

        # Select primary config to use for reference calculation.
        primary_sdf = (
            self.
            ev.
            primary_timeseries.
            filter(
                f"configuration_name = '{primary_configuration_name}'"
            ).
            to_sdf()
        )
        # Add the time period as a calculated field.
        primary_sdf = time_period.apply_to(primary_sdf)
        # Aggregate values based on the time period and summary statistic.
        groupby_field_list = self.ev.primary_timeseries.unique_column_set.copy()
        groupby_field_list.append(time_period.output_field_name)
        aggregated_field_name = f"{summary_statistic}_primary_value"
        summary_sdf = (
            group_df(primary_sdf, groupby_field_list).
            agg(summary_func("value").alias(aggregated_field_name))
        )
        return summary_sdf

    def _add_secondary_timeseries(
        self,
        ref_sdf: ps.DataFrame,
        output_configuration_name: str,
        output_configuration_description: str,
    ):
        """Add secondary timeseries to the evaluation.

        Nptes
        -----
        - Adds the configuration name to the configuration table if
          it doesn't exist.
        - Appends the data to the secondary timeseries table.
        """
        # Update configuration_name and add to the configurations table.
        ref_sdf = ref_sdf.withColumn(
            "configuration_name",
            F.lit(output_configuration_name)
        ).withColumn(
            "member",
            F.lit(None)
        )
        if (
            self.ev.configurations.filter(
                {
                    "column": "name",
                    "operator": "=",
                    "value": output_configuration_name
                }
            ).to_sdf().count() == 0
        ):
            self.ev.configurations.add(
                Configuration(
                    name=output_configuration_name,
                    type="secondary",
                    description=output_configuration_description,
                )
            )

        validated_df = self._validate(ref_sdf)
        # Write to the secondary timeseries table, overwriting any existing
        # data with the same configuration_name.
        self._write_spark_df(
            validated_df,
            write_mode="overwrite",
            partition_by=self.partition_by,
        )

    def calculate_climatology(
        self,
        primary_configuration_name: str,
        output_configuration_name: str,
        output_configuration_description: str,
        temporal_resolution: ClimatologyResolutionEnum = "day_of_year",
        summary_statistic: ClimatologyStatisticEnum = "mean",
    ):
        """Calculate climatology and add as a new timeseries.

        Parameters
        ----------
        primary_configuration_name : str
            Name of the primary configuration to use for the calculation.
        output_configuration_name : str
            Name of the output configuration.
        output_configuration_description : str
            Description of the output configuration.
        temporal_resolution : ClimatologyResolutionEnum, optional
            Temporal resolution for the climatology calculation,
            by default "day_of_year".
        summary_statistic : ClimatologyStatisticEnum, optional
            Summary statistic for the climatology calculation,
            by default "mean".
        """
        clim_sdf = self._calculate_climatology(
            primary_configuration_name=primary_configuration_name,
            temporal_resolution=temporal_resolution,
            summary_statistic=summary_statistic,
            output_configuration_name=output_configuration_name
        )
        self._add_secondary_timeseries(
            ref_sdf=clim_sdf,
            output_configuration_name=output_configuration_name,
            output_configuration_description=output_configuration_description,
        )
        pass

    def create_reference_forecast(
        self,
        primary_configuration_name: str,
        target_configuration_name: str,
        output_configuration_name: str,
        output_configuration_description: str,
        method: BaselineMethodEnum = "climatology",
        temporal_resolution: ClimatologyResolutionEnum = "day_of_year",
        summary_statistic: ClimatologyStatisticEnum = "mean",
        climatology_configuration_name: str = None,
    ):
        """Calculate climatology metrics and add as a new configuration.

        Parameters
        ----------
        primary_configuration_name : str
            Name of the primary configuration to use for the calculation.
        target_configuration_name : str
            Name of the target configuration to use for the calculation.
        output_configuration_name : str
            Name of the output configuration.
        output_configuration_description : str
            Description of the output configuration.
        method : BaselineMethodEnum, optional
            Method for the reference calculation,
            by default "climatology".
        temporal_resolution : ClimatologyResolutionEnum, optional
            Temporal resolution for the climatology calculation,
            by default "day_of_year".
        summary_statistic : ClimatologyStatisticEnum, optional
            Summary statistic for the climatology calculation,
            by default "mean".
        climatology_configuration_name : str, optional
            Name of the climatology configuration to use for the calculation.

        Notes
        -----
        - This does not save the baseline (ie, climatology).
          Call the method directly if you want to save.
        """
        self._check_load_table()
        # If all reference_time values are null in the target secondary
        # configuration (ie, it's a historical sim), we can't continue.
        if self.df.filter(
                    f"configuration_name = '{target_configuration_name}'"
                ).select(F.first("reference_time", ignorenulls=True)).first()[0] is None:
            raise ValueError(
                "No reference_time values found in the target configuration. "
                "Please specify a valid target forecast configuration."
            )
        if method == "climatology":
            if climatology_configuration_name is not None:
                # Use the climatology configuration if provided.
                reference_sdf = self.df.filter(
                    f"configuration_name = '{climatology_configuration_name}'"
                )
            else:
                # Calculate the climatology if not provided.
                reference_sdf = self._calculate_climatology(
                    primary_configuration_name=primary_configuration_name,
                    temporal_resolution=temporal_resolution,
                    summary_statistic=summary_statistic,
                )
        elif method == "persistence":
            # TODO: Implement persistence method.
            raise NotImplementedError(
                "Persistence method is not implemented yet."
            )
        # Join the reference with the secondary timeseries,
        # using a single member if secondary is an ensemble.
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

        time_period = rlc.HourOfYear()
        temp_sdf = time_period.apply_to(sec_sdf)

        # Get the rolling average of previous 6 hours
        # TODO: Should be an argument here.
        w = (Window.partitionBy("location_id").orderBy(F.col("hour_of_year")).rangeBetween(-7, 0))
        reference_sdf = reference_sdf.withColumn('rolling_mean', F.avg("mean_primary_value").over(w))

        # # This works but does not account for location_id.
        # ref_fcst_sdf = temp_sdf.join(
        #     reference_sdf,
        #     on=[temporal_resolution]
        # ).select(
        #     temp_sdf["value_time"],
        #     temp_sdf["reference_time"],
        #     temp_sdf["unit_name"],
        #     temp_sdf["variable_name"],
        #     temp_sdf["location_id"],
        #     temp_sdf["member"],
        #     reference_sdf["rolling_mean"].alias("value"),
        #     reference_sdf["configuration_name"],
        # )

        # pass

        # Join the reference sdf  to the template secondary forecast
        xwalk_sdf = self.ev.location_crosswalks.to_sdf()
        xwalk_sdf.createOrReplaceTempView("location_crosswalks")
        reference_sdf.createOrReplaceTempView("reference_timeseries")
        temp_sdf.createOrReplaceTempView("template_timeseries")

        query = """
            SELECT
                tf.reference_time
                , tf.value_time as value_time
                , tf.location_id as location_id
                , rf.rolling_mean as value
                , rf.configuration_name
                , tf.unit_name
                , tf.variable_name
                , tf.member
            FROM template_timeseries tf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = tf.location_id
            JOIN reference_timeseries rf
                on cf.primary_location_id = rf.location_id
                and tf.hour_of_year = rf.hour_of_year
                and tf.unit_name = rf.unit_name
                and tf.variable_name = rf.variable_name
        """
        ref_fcst_sdf = self.ev.spark.sql(query)

        self.spark.catalog.dropTempView("location_crosswalks")
        self.spark.catalog.dropTempView("reference_timeseries")
        self.spark.catalog.dropTempView("template_timeseries")

        pass

        self._add_secondary_timeseries(
            ref_sdf=ref_fcst_sdf,
            output_configuration_name=output_configuration_name,
            output_configuration_description=output_configuration_description,
        )



