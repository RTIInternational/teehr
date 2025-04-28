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
import pyspark.sql as ps
from teehr.models.pydantic_table_models import (
    Configuration
)


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
            "member"
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

        location_ids = self.ev.location_crosswalks.distinct_values("secondary_location_id")  # noqa
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

    @staticmethod
    def _calculate_climatology(
        primary_sdf: ps.DataFrame,
        temporal_resolution: ClimatologyResolutionEnum,
        summary_statistic: ClimatologyStatisticEnum = "mean",
        output_configuration_name: str = "climatology",
    ):
        """Calculate climatology."""
        if temporal_resolution == ClimatologyResolutionEnum.day_of_year:
            time_period = rlc.DayOfYear()
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

        primary_sdf = time_period.apply_to(primary_sdf)

        groupby_field_list = [
            "location_id",
            "variable_name",
            "unit_name",
            "configuration_name",
        ]
        groupby_field_list.append(time_period.output_field_name)
        aggregated_field_name = f"{summary_statistic}_primary_value"
        summary_sdf = (
            group_df(primary_sdf, groupby_field_list).
            agg(summary_func("value").alias(aggregated_field_name))
        )
        temp_sdf = primary_sdf.join(
            summary_sdf,
            on=groupby_field_list,
            how="left"
        )
        groupby_field_list.remove(time_period.output_field_name)

        return (
            temp_sdf.
            drop("value").
            drop(time_period.output_field_name).
            withColumnRenamed(aggregated_field_name, "value").
            withColumn("configuration_name", F.lit(output_configuration_name))
        )

    def _add_secondary_timeseries(
        self,
        ref_sdf: ps.DataFrame,
        xwalk_sdf: ps.DataFrame,
        output_configuration_name: str,
        output_configuration_description: str,
        output_location_id_prefix: str = "test",
    ):
        """Add secondary timeseries to the evaluation.

        Nptes
        -----
        - Adds the specified prefix to the location ids.
        - Adds the configuration name to the configuration table if
          it doesn't exist.
        - Appends the data to the secondary timeseries table.
        """
        # Update location_id prefixes.
        ref_sdf = add_or_replace_sdf_column_prefix(
            sdf=ref_sdf,
            column_name="location_id",
            prefix=output_location_id_prefix,
        )
        xwalk_sdf = add_or_replace_sdf_column_prefix(
            sdf=xwalk_sdf,
            column_name="secondary_location_id",
            prefix=output_location_id_prefix,
        )
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
        # Append to location_crosswalks and secondary_timeseries tables.
        self.ev.location_crosswalks._write_spark_df(
            xwalk_sdf,
            write_mode="append"
        )
        self._write_spark_df(
            ref_sdf,
            write_mode="append",
            partition_by=self.partition_by,
        )

    def calculate_climatology(
        self,
        primary_configuration_name: str,
        output_configuration_name: str,
        output_configuration_description: str,
        output_location_id_prefix: str = "test",
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
        output_location_id_prefix : str, optional
            Prefix for the location IDs in the output configuration,
            by default "test".
        temporal_resolution : ClimatologyResolutionEnum, optional
            Temporal resolution for the climatology calculation,
            by default "day_of_year".
        summary_statistic : ClimatologyStatisticEnum, optional
            Summary statistic for the climatology calculation,
            by default "mean".
        """
        clim_sdf = self._calculate_climatology(
            primary_sdf=self.df.filter(
                f"configuration_name = '{primary_configuration_name}'"
            ),
            temporal_resolution=temporal_resolution,
            summary_statistic=summary_statistic,
            output_configuration_name=output_configuration_name
        )
        self._add_secondary_timeseries(
            ref_sdf=clim_sdf,
            xwalk_sdf=self.ev.location_crosswalks.to_sdf(),
            output_configuration_name=output_configuration_name,
            output_configuration_description=output_configuration_description,
            output_location_id_prefix=output_location_id_prefix
        )
        pass

    def create_reference_forecast(
        self,
        primary_configuration_name: str,
        target_configuration_name: str,
        output_configuration_name: str,
        output_configuration_description: str,
        output_location_id_prefix: str = "test",
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
        output_location_id_prefix : str, optional
            Prefix for the location IDs in the output configuration,
            by default "test".
        method : BaselineMethodEnum, optional
            Method for the reference calculation,
            by default "climatology".
        temporal_resolution : ClimatologyResolutionEnum, optional
            Temporal resolution for the climatology calculation,
            by default "day_of_year".
        summary_statistic : ClimatologyStatisticEnum, optional
            Summary statistic for the climatology calculation,
            by default "mean".

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
                ).filter(F.col("reference_time").isNotNull()).count() == 0:
            raise ValueError(
                "No reference_time values found in the target configuration. "
                "Please specify a valid target forecast configuration."
            )

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
        if method == "climatology":
            if climatology_configuration_name is not None:
                # Use the climatology configuration if provided.
                reference_sdf = self.df.filter(
                    f"configuration_name = '{climatology_configuration_name}'"
                )
            else:
                # Calculate the climatology if not provided.
                reference_sdf = self._calculate_climatology(
                    primary_sdf=primary_sdf,
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

        xwalk_sdf = self.ev.location_crosswalks.to_sdf()
        reference_sdf.createOrReplaceTempView("primary_timeseries")
        sec_sdf.createOrReplaceTempView("secondary_timeseries")
        xwalk_sdf.createOrReplaceTempView("location_crosswalks")
        query = """
            SELECT
                sf.reference_time
                , sf.value_time as value_time
                , sf.location_id as location_id
                , pf.value as value
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
        """
        ref_sdf = self.ev.spark.sql(query)

        self._add_secondary_timeseries(
            ref_sdf=ref_sdf,
            xwalk_sdf=xwalk_sdf,
            output_configuration_name=output_configuration_name,
            output_configuration_description=output_configuration_description,
            output_location_id_prefix=output_location_id_prefix
        )
