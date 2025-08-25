"""Module for generating metrics."""
from typing import Union, List

import pandas as pd
import geopandas as gpd
import pyspark.sql as ps
import pyspark.sql.functions as F
from teehr.models.filters import (
    JoinedTimeseriesFilter
)
from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
from teehr.models.table_enums import (
    JoinedTimeseriesFields
)
from teehr.querying.filter_format import validate_and_apply_filters
from teehr.querying.metric_format import apply_aggregation_metrics
from teehr.querying.utils import (
    order_df,
    group_df,
    join_geometry,
    parse_fields_to_list
)

import logging

logger = logging.getLogger(__name__)


class Metrics:
    """Component class for calculating metrics."""

    def __init__(self, ev) -> None:
        """Initialize the Metrics class."""
        self.spark = ev.spark
        self.dataset_dir = ev.dataset_dir
        self.locations = ev.locations
        self.joined_timeseries = ev.joined_timeseries
        self.df = self.joined_timeseries.to_sdf()

    def query(
        self,
        filters: Union[
            str, dict, JoinedTimeseriesFilter,
            List[Union[str, dict, JoinedTimeseriesFilter]]
        ] = None,
        order_by: Union[
            str, JoinedTimeseriesFields,
            List[Union[str, JoinedTimeseriesFields]]
        ] = None,
        group_by: Union[
            str, JoinedTimeseriesFields,
            List[Union[str, JoinedTimeseriesFields]]
        ] = None,
        include_metrics: Union[
            List[MetricsBasemodel],
            str
        ] = None
    ):
        """Perform a query on the dataset joined timeseries table.

        Parameters
        ----------
        filters : Union[str, dict, JoinedTimeseriesFilter, List[Union[str, dict, JoinedTimeseriesFilter]]], optional
            The filters to apply to the query, by default None
        order_by : Union[str, JoinedTimeseriesFields, List[Union[str, JoinedTimeseriesFields]]], optional
            The fields to order the query by, by default None
        group_by : Union[str, JoinedTimeseriesFields, List[Union[str, JoinedTimeseriesFields]]], optional
            The fields to group the query by, by default None
        include_metrics : Union[List[MetricsBasemodel], str], optional
            The metrics to include in the query, by default None

        Examples
        --------

        >>> import teehr
        >>> ev = teehr.Evaluation()

        Define some metrics, optionally including an available bootstrapping
        method. (:func:`Metric Models <teehr.models.metrics.metric_models>`).

        >>> import teehr.Metrics as m
        >>> import teehr.Bootstrappers as b

        Define a Circular Block bootstrapper.
        (:func:`Bootstrap Models <teehr.models.metrics.bootstrap_models>`).

        >>> boot = b.CircularBlock(
        >>>     seed=40,
        >>>     block_size=100,
        >>>     quantiles=None,
        >>>     reps=500
        >>> )

        Include the bootstrap model in the metric definition(s), along with other
        optional arguments.

        >>> kge = m.KlingGuptaEfficiency(bootstrap=boot)
        >>> primary_avg = m.Average(
        >>>     transform="log",
        >>>     output_field_name="primary_avg",
        >>>     input_field_names=["primary_value"]
        >>> )
        >>> mvtd = m.MaxValueTimeDelta(input_field_names=["secondary_value"])
        >>> pmvt = m.MaxValueTime(input_field_names=["secondary_value"])

        >>> include_metrics = [pmvt, mvtd, primary_avg, kge]

        Get the currently available fields to use in the query.

        >>> flds = eval.joined_timeseries.field_enum()

        Define some filters.

        >>> filters = [
        >>>     JoinedTimeseriesFilter(
        >>>         column=flds.primary_location_id,
        >>>         operator=ops.eq,
        >>>         value="gage-A"
        >>>     )
        >>> ]

        Perform the query, returning the results as a GeoPandas DataFrame.

        >>> metrics_df = eval.metrics.query(
        >>>     include_metrics=include_metrics,
        >>>     group_by=[flds.primary_location_id],
        >>>     order_by=[flds.primary_location_id],
        >>>     filters=filters,
        >>> ).to_geopandas()
        """ # noqa
        logger.info("Calculating performance metrics.")
        if filters is not None:
            logger.debug("Applying filters to the metrics query.")
            self.df = validate_and_apply_filters(
                sdf=self.df,
                filter_model=self.joined_timeseries.filter_model,
                filters=filters,
                fields_enum=self.joined_timeseries.field_enum(),
                validate=False
            )

        logger.debug(f"Grouping the metrics query {group_by}.")
        self.df = group_df(self.df, group_by)

        self.df = apply_aggregation_metrics(
            self.df,
            include_metrics,
        )

        self.df = self._post_process_metric_results(
            include_metrics,
            group_by
        )

        if order_by is not None:
            logger.debug(f"Ordering the metrics by: {order_by}.")
            self.df = order_df(self.df, order_by)

        return self

    def to_pandas(self) -> pd.DataFrame:
        """Convert the DataFrame to a Pandas DataFrame."""
        df = self.df.toPandas()
        df.attrs['table_type'] = 'metrics'
        return df

    def to_geopandas(self) -> gpd.GeoDataFrame:
        """Convert the DataFrame to a GeoPandas DataFrame."""
        if "primary_location_id" not in self.df.columns:
            err_msg = "The primary_location_id field must be included in " \
                      "the group_by to include geometry."
            logger.error(err_msg)
            raise ValueError(err_msg)
        return join_geometry(
            self.df,
            self.locations.to_sdf(),
            "primary_location_id"
        )

    def to_sdf(self) -> ps.DataFrame:
        """Return the Spark DataFrame."""
        return self.df

    def add_calculated_fields(self, cfs: Union[CalculatedFieldBaseModel, List[CalculatedFieldBaseModel]]):
        """Add calculated fields to the joined timeseries DataFrame before running metrics.

        Parameters
        ----------
        cfs : Union[CalculatedFieldBaseModel, List[CalculatedFieldBaseModel]]
            The CFs to apply to the DataFrame.

        Returns
        -------
        self
            The Metrics object with the CFs applied to the DataFrame.

        Examples
        --------
        >>> import teehr
        >>> from teehr import RowLevelCalculatedFields as rcf
        >>> ev.join_timeseries.add_calculated_fields([
        >>>     rcf.Month()
        >>> ]).write()
        """
        # self._check_load_table()

        if not isinstance(cfs, List):
            cfs = [cfs]

        for cf in cfs:
            self.df = cf.apply_to(self.df)

        return self

    def _post_process_metric_results(
        self,
        include_metrics: List[MetricsBasemodel],
        group_by: Union[
            str, JoinedTimeseriesFields,
            List[Union[str, JoinedTimeseriesFields]]
        ]
    ) -> ps.DataFrame:
        """Post-process the results of the metrics query.

        Notes
        -----
        This method includes functionality to update the dataframe returned
        by the query method depending on metric model attributes.

        If the metric model specifies a reference configuration, it will
        calculate the skill score of metric values for each configuration
        relative to the reference configuration. The skill score is calculated
        as `1 - (metric_value / reference_metric_value)`.

        Additionally, if the metric model specifies unpacking of results,
        metric results returned as a dictionary will be unpacked into separate
        columns in the DataFrame.
        """
        for model in include_metrics:
            if model.reference_configuration is not None:
                self.df = self._calculate_metric_skill_score(
                    model.output_field_name,
                    model.reference_configuration,
                    group_by
                )

            if model.unpack_results:
                self.df = model.unpack_function(
                    self.df,
                    model.output_field_name
                )

        return self.df

    def _calculate_metric_skill_score(
        self,
        metric_field: str,
        reference_configuration: str,
        group_by: Union[
            str, JoinedTimeseriesFields,
            List[Union[str, JoinedTimeseriesFields]]
        ]
    ) -> ps.DataFrame:
        """Calculate skill score based on a reference configuration.

        Calculate the skill score of metric values for each configuration
        relative to the reference configuration. The skill score is calculated
        as `1 - (metric_value / reference_metric_value)`.
        """
        logger.debug("Calculating skill score.")
        group_by_strings = parse_fields_to_list(group_by)
        # TODO: Raise error if configuration_name is not in group_by?
        group_by_strings.remove("configuration_name")

        pivot_sdf = (
            self.df
            .groupBy(group_by_strings).
            pivot("configuration_name").
            agg(F.first(metric_field))
        )
        # Get all configuration names except the reference configuration
        configurations = self.df.select("configuration_name").distinct().collect()
        configurations = [row.configuration_name for row in configurations]
        configurations.remove(reference_configuration)

        skill_score_col = f"{metric_field}_skill_score"
        sdf = self.df.withColumn(skill_score_col, F.lit(None))

        for config in configurations:
            # Pivot and calculate the skill score.
            temp_col = f"{config}_{metric_field}_skill"
            pivot_sdf = pivot_sdf.withColumn(
                temp_col,
                1 - F.col(config) / F.col(reference_configuration)
            ).withColumn(
                "configuration_name",
                F.lit(config)
            )
            # Join skill score values from the pivot table.
            join_cols = group_by_strings + ["configuration_name"]
            sdf = sdf.join(
                pivot_sdf,
                on=join_cols,
                how="left"
            ).select(
                *join_cols,
                F.col(f"{metric_field}"),
                F.col(temp_col),
                F.col(skill_score_col)
            )
            # Now update the column based on the configuration name.
            sdf = sdf.withColumn(
                skill_score_col,
                F.when(
                    sdf["configuration_name"] == f"{config}",
                    sdf[temp_col]
                ).otherwise(sdf[skill_score_col])
            ).select(
                *join_cols,
                F.col(f"{metric_field}"),
                F.col(skill_score_col)
            )

        return sdf
