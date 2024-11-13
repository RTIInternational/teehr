"""Module for generating metrics."""
from typing import Union, List

import pandas as pd
import geopandas as gpd
import pyspark.sql as ps
from teehr.models.filters import (
    JoinedTimeseriesFilter
)
from teehr.models.metrics.metric_models import MetricsBasemodel
from teehr.models.table_enums import (
    JoinedTimeseriesFields
)
from teehr.querying.filter_format import validate_and_apply_filters
from teehr.querying.metric_format import apply_aggregation_metrics
from teehr.querying.utils import (
    order_df,
    group_df,
    join_geometry
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
            include_metrics
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
