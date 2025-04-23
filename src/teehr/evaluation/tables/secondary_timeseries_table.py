"""Secondary timeseries table class."""
import teehr.const as const
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.loading.timeseries import convert_timeseries
import teehr.models.pandera_dataframe_schemas as schemas
from teehr.models.table_enums import TimeseriesFields
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from teehr.models.table_enums import TableWriteEnum
from teehr.loading.utils import add_or_replace_sdf_column_prefix
from teehr.querying.utils import group_df
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
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
        time_period: CalculatedFieldBaseModel,
        summary_statistic: list = "mean",
        output_configuration_name: str = "climatology",
    ):
        """Calculate climatology."""
        primary_sdf = time_period.apply_to(primary_sdf)
        groupby_field_list = [
            "location_id",
            "variable_name",
            "unit_name",
            "configuration_name",
        ]
        if summary_statistic == "mean":
            summary_func = F.mean
        elif summary_statistic == "median":
            summary_func = F.expr("percentile_approx(value, 0.5)")
        elif summary_statistic == "max":
            summary_func = F.max
        elif summary_statistic == "min":
            summary_func = F.min
        else:
            raise ValueError(
                f"Invalid summary statistic: {summary_statistic}. "
                "Valid values are: mean, median, max, min."
            )
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

    def create_reference_forecast(
        self,
        time_period: CalculatedFieldBaseModel,
        primary_configuration_name: str,
        target_configuration_name: str,
        output_configuration_name: str,
        output_configuration_description: str,
        summary_statistic: list = "mean",
        location_id_prefix: str = "test",
    ):
        """Calculate climatology metrics and add as a new configuration.

        Parameters
        ----------

        """
        self._check_load_table()

        # Select primary config to use for climatology.
        primary_sdf = (
            self.
            ev.
            primary_timeseries.
            filter(
                f"configuration_name = '{primary_configuration_name}'"
            ).
            to_sdf()
        )
        clim_sdf = self._calculate_climatology(
            primary_sdf=primary_sdf,
            time_period=time_period,
            summary_statistic=summary_statistic,
        )

        # Join the climatology with the secondary timeseries,
        # using a single member if secondary is an ensemble.
        member_id = self.df.filter(
            f"configuration_name = '{target_configuration_name}'"
        ).select(F.first("member", ignorenulls=True)).first()[0]
        if member_id is None:
            sec_sdf = self.df.filter(
                (f"configuration_name = '{target_configuration_name}'") and
                (f"member = '{member_id}'")
            )
        else:
            sec_sdf = self.df.filter(
                f"configuration_name = '{target_configuration_name}'"
            )

        xwalk_sdf = self.ev.location_crosswalks.to_sdf()
        clim_sdf.createOrReplaceTempView("primary_timeseries")
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

        # Update location_id prefixes.
        ref_sdf = add_or_replace_sdf_column_prefix(
            sdf=ref_sdf,
            column_name="location_id",
            prefix=location_id_prefix,
        )
        xwalk_sdf = add_or_replace_sdf_column_prefix(
            sdf=xwalk_sdf,
            column_name="secondary_location_id",
            prefix=location_id_prefix,
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