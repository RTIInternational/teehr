"""Classes for generating synthetic timeseries."""
from typing import Union, List

from pyspark.sql import functions as F
import pyspark.sql as ps

from teehr.models.filters import FilterBaseModel
from teehr.models.generate.base import (
    GeneratorABC,
    TimeseriesGeneratorBaseModel,
    NormalsResolutionEnum,
    NormalsStatisticEnum
)
from teehr.querying.utils import group_df
from teehr.generate.utils import (
    get_time_period_rlc,
    # calculate_rolling_average,
    # ffill_and_bfill_nans
)


class Normals(TimeseriesGeneratorBaseModel, GeneratorABC):
    """Model for generating synthetic normals timeseries."""

    temporal_resolution: NormalsResolutionEnum = NormalsResolutionEnum.day_of_year
    summary_statistic: NormalsStatisticEnum = NormalsStatisticEnum.mean

    def generate(
        self,
        input_timeseries_sdf: ps.DataFrame,
        output_variable_name: str
    ) -> ps.DataFrame:
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
        normals_sdf = input_timeseries_sdf.drop("value").join(
            summary_sdf,
            on=groupby_field_list,
            how="left"
        ).drop(time_period.output_field_name)

        input_variable_name = normals_sdf.select(F.first("variable_name")).collect()[0][0]
        variable = input_variable_name.split("_")[0]
        output_variable_name = (
                f"{variable}_{self.temporal_resolution.value}_"
                f"{self.summary_statistic.value}"
            )

        # Over-write the variable_name
        normals_sdf = normals_sdf.withColumn(
            "variable_name",
            F.lit(output_variable_name)
        )

        return normals_sdf


class TimeseriesGenerators:
    """Synthetic timeseries generators."""

    Normals = Normals
