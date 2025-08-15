"""Classes for generating synthetic timeseries."""
from typing import Union, List

from pyspark.sql import functions as F
import pyspark.sql as ps

from teehr.evaluation.evaluation import Evaluation
from teehr.models.filters import FilterBaseModel
from teehr.models.generate.base import (
    GeneratorABC,
    SummaryTimeseriesBaseModel,
    NormalsResolutionEnum,
    NormalsStatisticEnum,
    TimeseriesModel
)
from teehr.querying.utils import group_df
from teehr.generate.utils import (
    get_time_period_rlc,
    calculate_rolling_average,
    ffill_and_bfill_nans
)


class Persistence(SummaryTimeseriesBaseModel, GeneratorABC):
    """Model for generating a synthetic persistence forecast timeseries.

    This model generates a synthetic persistence forecast timeseries based on
    an input timeseries DataFrame. It assigns the values from the input
    timeseries to the forecast timeseries based on t-0 time, without any
    modifications or aggregations.
    """
    # TODO: Implement
    df: ps.DataFrame = None
    pass


class ReferenceForecast(SummaryTimeseriesBaseModel, GeneratorABC):
    """Model for generating a synthetic reference forecast timeseries.

    Notes
    -----
    This model generates a synthetic reference forecast timeseries based on
    an input timeseries DataFrame. It assigns the values from the input
    timeseries to the forecast timeseries based on value time, optionally
    aggregrating values within a specified time window.
    """
    df: ps.DataFrame = None
    reference_tsm: TimeseriesModel = None
    template_tsm: TimeseriesModel = None
    output_tsm: TimeseriesModel = None

    aggregate_reference_timeseries: bool = False
    aggregation_time_window: str = "6 hours"
    temporal_resolution: NormalsResolutionEnum = NormalsResolutionEnum.day_of_year

    def generate(
        self,
        ev: Evaluation,
        reference_sdf: ps.DataFrame,
        template_sdf: ps.DataFrame,
        partition_by: List[str]
    ) -> ps.DataFrame:
        """Generate synthetic reference forecast timeseries."""
        # Aggregate the reference timeseries to the
        # using a rolling average if aggregate_reference_timeseries is True.
        if self.aggregate_reference_timeseries is True:
            reference_sdf = calculate_rolling_average(
                sdf=reference_sdf,
                partition_by=partition_by,
                time_window=self.aggregation_time_window
            )
        time_period = get_time_period_rlc(
            temporal_resolution=self.temporal_resolution
        )
        template_sdf = time_period.apply_to(template_sdf)
        # TODO: This is redundant with climatology method?
        reference_sdf = time_period.apply_to(reference_sdf)
        # Join the reference sdf  to the template secondary forecast
        xwalk_sdf = ev.location_crosswalks.to_sdf()
        xwalk_sdf.createOrReplaceTempView("location_crosswalks")
        reference_sdf.createOrReplaceTempView("reference_timeseries")
        template_sdf.createOrReplaceTempView("template_timeseries")
        # logger.debug(
        #     "Joining reference climatology timeseries to template forecast."
        # )
        # TODO: What about output tsm variable_name? Is it the same as input?
        query = f"""
            SELECT
                tf.reference_time
                , tf.value_time as value_time
                , tf.location_id as location_id
                , rf.value as value
                , tf.unit_name
                , tf.variable_name
                , tf.member
                , '{self.output_tsm.configuration_name}' as configuration_name
            FROM template_timeseries tf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = tf.location_id
            LEFT JOIN reference_timeseries rf
                on cf.primary_location_id = rf.location_id
                and tf.{time_period.output_field_name} = rf.{time_period.output_field_name}
                and tf.unit_name = rf.unit_name
                and tf.value_time = rf.value_time
        """  # noqa
        results_sdf = ev.spark.sql(query)
        ev.spark.catalog.dropTempView("location_crosswalks")
        ev.spark.catalog.dropTempView("reference_timeseries")
        ev.spark.catalog.dropTempView("template_timeseries")

        results_df = results_sdf.toPandas()  # TEMP

        return results_sdf


class Normals(SummaryTimeseriesBaseModel, GeneratorABC):
    """Model for generating synthetic normals timeseries."""

    temporal_resolution: NormalsResolutionEnum = NormalsResolutionEnum.day_of_year
    summary_statistic: NormalsStatisticEnum = NormalsStatisticEnum.mean
    input_tsm: TimeseriesModel = None
    df: ps.DataFrame = None

    def _get_output_variable_name(self, sdf) -> str:
        input_variable_name = sdf.select(
            F.first("variable_name")
        ).collect()[0][0]
        variable = input_variable_name.split("_")[0]
        return (
            f"{variable}_{self.temporal_resolution.value}_"
            f"{self.summary_statistic.value}"
        )

    def generate(
        self,
        input_timeseries_sdf: ps.DataFrame
    ):
        """Generate synthetic normals timeseries."""
        time_period = get_time_period_rlc(self.temporal_resolution)

        if self.summary_statistic == NormalsStatisticEnum.mean:
            summary_func = F.mean
        elif self.summary_statistic == NormalsStatisticEnum.median:
            summary_func = F.expr("percentile_approx(value, 0.5)")
        elif self.summary_statistic == NormalsStatisticEnum.max:
            summary_func = F.max
        elif self.summary_statistic == NormalsStatisticEnum.min:
            summary_func = F.min

        groupby_field_list = [
            "location_id",
            "variable_name",
            "unit_name",
            "configuration_name"
        ]
        # Add the time period as a calculated field.
        input_timeseries_sdf = time_period.apply_to(input_timeseries_sdf)
        # Aggregate values based on the time period and summary statistic.
        groupby_field_list.append(time_period.output_field_name)
        summary_sdf = (
            group_df(input_timeseries_sdf, groupby_field_list).
            agg(summary_func("value").alias("value"))
        )
        # TODO: Use a template here instead of joining back?
        # Decompose the input sdf by:
        # - start/end dates
        # - timestep
        # Create a template sdf based on those variables

        normals_sdf = input_timeseries_sdf.drop("value").join(
            summary_sdf,
            on=groupby_field_list,
            how="left"
        ).drop(time_period.output_field_name)

        output_variable_name = self._get_output_variable_name(normals_sdf)

        # Over-write the variable_name
        normals_sdf = normals_sdf.withColumn(
            "variable_name",
            F.lit(output_variable_name)
        )

        return normals_sdf


class SummaryTimeseriesGenerators:
    """Synthetic timeseries generators."""

    Normals = Normals


class BenchmarkForecastGenerators:
    """Synthetic generated benchmark forecasts."""

    ReferenceForecast = ReferenceForecast
    Persistence = Persistence