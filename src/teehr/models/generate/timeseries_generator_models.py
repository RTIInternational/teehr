"""Classes for generating synthetic timeseries."""
from typing import List
import logging

from pyspark.sql import functions as F
import pyspark.sql as ps

from teehr.evaluation.evaluation import Evaluation
from teehr.models.generate.base import (
    GeneratorABC,
    SignatureGeneratorBaseModel,
    NormalsResolutionEnum,
    NormalsStatisticEnum,
    BenchmarkGeneratorBaseModel
)
from teehr.querying.utils import group_df
from teehr.generate.utils import (
    get_time_period_rlc,
    calculate_rolling_average,
    ffill_and_bfill_nans
)

logger = logging.getLogger(__name__)


class Persistence(BenchmarkGeneratorBaseModel, GeneratorABC):
    """Model for generating a synthetic persistence forecast timeseries.

    This model generates a synthetic persistence forecast timeseries based on
    an input timeseries DataFrame. It assigns the values from the input
    timeseries to the forecast timeseries based on t-0 time, without any
    modifications or aggregations.
    """

    # TODO: Implement
    df: ps.DataFrame = None
    pass


class ReferenceForecast(BenchmarkGeneratorBaseModel, GeneratorABC):
    """Model for generating a synthetic reference forecast timeseries.

    Parameters
    ----------
    aggregate_reference_timeseries : bool
        Whether to aggregate the reference timeseries.
        Defaults to False.
    aggregation_time_window : str
        The time window for aggregation. Defaults to "6 hours".
    df : ps.DataFrame
        The DataFrame containing the timeseries data.

    Notes
    -----
    This model generates a synthetic reference forecast timeseries based on
    an input timeseries DataFrame. It assigns the values from the input
    timeseries to the forecast timeseries based on value time, optionally
    aggregrating values within a specified time window.

    This requires specific timeseries to work with.
    """

    aggregate_reference_timeseries: bool = False
    aggregation_time_window: str = "6 hours"
    df: ps.DataFrame = None

    def generate(
        self,
        ev: Evaluation,
        reference_sdf: ps.DataFrame,
        template_sdf: ps.DataFrame,
        partition_by: List[str],
        output_configuration_name: str
    ) -> ps.DataFrame:
        """Generate synthetic reference forecast timeseries."""
        # Aggregate the reference timeseries to the
        # using a rolling average if aggregate_reference_timeseries is True.
        # TODO: Should this define a new variable_name?
        if self.aggregate_reference_timeseries is True:
            reference_sdf = calculate_rolling_average(
                spark=ev.spark,
                sdf=reference_sdf,
                partition_by=partition_by,
                time_window=self.aggregation_time_window
            )
        # Join the reference sdf  to the template secondary forecast
        xwalk_sdf = ev.location_crosswalks.to_sdf()
        xwalk_sdf.createOrReplaceTempView("location_crosswalks")
        reference_sdf.createOrReplaceTempView("reference_timeseries")
        template_sdf.createOrReplaceTempView("template_timeseries")
        logger.debug(
            "Joining reference timeseries values to the template forecast."
        )
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
                and tf.unit_name = rf.unit_name
                and tf.value_time = rf.value_time
        """  # noqa
        results_sdf = ev.spark.sql(query)
        ev.spark.catalog.dropTempView("location_crosswalks")
        ev.spark.catalog.dropTempView("reference_timeseries")
        ev.spark.catalog.dropTempView("template_timeseries")

        logger.debug("Filling NaNs with forward fill and backward fill.")
        results_sdf = ffill_and_bfill_nans(results_sdf)

        #  # TODO: Is this needed?
        # results_sdf = results_sdf.dropna(subset=["value"])

        return results_sdf


class Normals(SignatureGeneratorBaseModel, GeneratorABC):
    """Model for generating synthetic normals timeseries.

    Parameters
    ----------
    temporal_resolution : NormalsResolutionEnum
        The temporal resolution for the normals timeseries.
    summary_statistic : NormalsStatisticEnum
        The summary statistic to use for the normals timeseries.
    df : ps.DataFrame
        The DataFrame containing the timeseries data.
    """

    temporal_resolution: NormalsResolutionEnum = NormalsResolutionEnum.day_of_year
    summary_statistic: NormalsStatisticEnum = NormalsStatisticEnum.mean
    df: ps.DataFrame = None

    @staticmethod
    def _update_variable_names(
        sdf,
        time_period,
        statistic
    ) -> ps.DataFrame:
        """Update variable_name field values based on time period and stat."""
        return sdf.withColumn(
            "variable_name",
            F.concat_ws(
                "_",
                F.split(sdf.variable_name, pattern="_")[0],
                F.lit(time_period.output_field_name),
                F.lit(statistic.value)
            )
        )

    def generate(
        self,
        input_dataframe: ps.DataFrame,
        output_dataframe: ps.DataFrame,
        fillna: bool,
        dropna: bool
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
        input_dataframe = time_period.apply_to(input_dataframe)
        # Aggregate values based on the time period and summary statistic.
        groupby_field_list.append(time_period.output_field_name)
        summary_sdf = (
            group_df(input_dataframe, groupby_field_list).
            agg(summary_func("value").alias("value"))
        )

        output_dataframe = time_period.apply_to(output_dataframe)
        normals_sdf = output_dataframe.join(
            summary_sdf,
            on=groupby_field_list,
            how="left"
        ).drop(time_period.output_field_name)

        normals_sdf = self._update_variable_names(
            normals_sdf,
            time_period=time_period,
            statistic=self.summary_statistic
        )

        if fillna is True:
            normals_sdf = ffill_and_bfill_nans(
                normals_sdf
            )

        if dropna is True:
            normals_sdf = normals_sdf.dropna(subset=["value"])

        return normals_sdf


class SignatureTimeseriesGenerators:
    """Synthetic timeseries generators."""

    Normals = Normals
    # Detrend = Detrend


class BenchmarkForecastGenerators:
    """Synthetic generated benchmark forecasts."""

    ReferenceForecast = ReferenceForecast
    Persistence = Persistence
