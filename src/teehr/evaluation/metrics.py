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

    def __init__(self, eval) -> None:
        """Initialize the Metrics class."""
        self.spark = eval.spark
        self.dataset_dir = eval.dataset_dir
        self.locations = eval.locations
        self.joined_timeseries = eval.joined_timeseries
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
        """Get the metrics in the dataset."""
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
        return self.df.toPandas()

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
