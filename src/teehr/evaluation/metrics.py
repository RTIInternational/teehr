"""Module for generating metrics."""
from typing import Union, List

import pandas as pd
import geopandas as gpd
import pyspark.sql as ps
# import pyspark.sql.functions as F
from teehr.models.filters import (
    JoinedTimeseriesFilter
)
from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
from teehr.models.table_enums import (
    JoinedTimeseriesFields
)
from teehr.querying.metric_format import apply_aggregation_metrics
from teehr.querying.utils import (
    order_df,
    group_df,
    join_geometry,
    post_process_metric_results
)

import logging

logger = logging.getLogger(__name__)


class Metrics:
    """Component class for calculating metrics."""

    def __init__(self, ev) -> None:
        """Initialize the Metrics class."""
        self._ev = ev
        self.spark = ev.spark
        self.locations = ev.locations
        self._write = ev.write

    def __call__(
        self,
        table_name: str = "joined_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ) -> "Metrics":
        """Initialize the Metrics class.

        Parameters
        ----------
        table_name : str, optional
            The name of the table to use for metrics calculations,
            by default "joined_timeseries"
        namespace_name : Union[str, None], optional
            The namespace of the table, by default None in which case the
            namespace_name of the active catalog is used.
        catalog_name : Union[str, None], optional
            The catalog of the table, by default None in which case the
            catalog_name of the active catalog is used.
        """
        self.table_name = table_name
        self.table = self._ev.table(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
        )
        self.sdf = self.table.to_sdf()

        return self

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
        """Perform a query on the specified table.

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

        By default, the metrics query operates on the ``joined_timeseries`` table.
        To specify a different table, initialize the Metrics class with the
        desired table name.

        >>> fdc = teehr.Signatures.FlowDurationCurveSlope()
        >>> fdc.input_field_names = ["value"]

        >>> metrics_df = eval.metrics(
        >>>     table_name="primary_timeseries"
        >>> ).query(
        >>>     include_metrics=[fdc],
        >>>     group_by=[flds.primary_location_id],
        >>>     order_by=[flds.primary_location_id],
        >>> ).to_pandas()
        """ # noqa
        logger.info("Calculating performance metrics.")
        if filters is not None:
            logger.debug("Applying filters to the metrics query.")
            validated_filters = self._ev.validate.table_filters(
                table_name=self.table_name,
                filters=filters,
                validate=False
            )
            for filter in validated_filters:
                self.sdf = self.sdf.filter(filter)

        logger.debug(f"Grouping the metrics query {group_by}.")
        self.sdf = group_df(self.sdf, group_by)

        self.sdf = apply_aggregation_metrics(
            self.sdf,
            include_metrics,
        )

        self.sdf = post_process_metric_results(
            sdf=self.sdf,
            include_metrics=include_metrics,
            group_by=group_by
        )

        if order_by is not None:
            logger.debug(f"Ordering the metrics by: {order_by}.")
            self.sdf = order_df(self.sdf, order_by)

        return self

    def to_pandas(self) -> pd.DataFrame:
        """Convert the DataFrame to a Pandas DataFrame."""
        df = self.sdf.toPandas()
        df.attrs['table_type'] = 'metrics'
        return df

    def to_geopandas(self) -> gpd.GeoDataFrame:
        """Convert the DataFrame to a GeoPandas DataFrame."""
        if "primary_location_id" not in self.sdf.columns:
            err_msg = "The primary_location_id field must be included in " \
                      "the group_by to include geometry."
            logger.error(err_msg)
            raise ValueError(err_msg)
        return join_geometry(
            self.sdf,
            self.locations.to_sdf(),
            "primary_location_id"
        )

    def to_sdf(self) -> ps.DataFrame:
        """Return the Spark DataFrame."""
        return self.sdf

    def add_calculated_fields(self, cfs: Union[CalculatedFieldBaseModel, List[CalculatedFieldBaseModel]]):
        """Add in-memory calculated fields to the table before running metrics.

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
        >>> ev.metrics(table_name="joined_timeseries").add_calculated_fields([
        >>>     rcf.Month()
        >>> ]).query(
        >>>     include_metrics=[fdc],
        >>>     group_by=[flds.primary_location_id],
        >>>     order_by=[flds.primary_location_id],
        >>> ).to_pandas()
        """
        if not isinstance(cfs, List):
            cfs = [cfs]

        for cf in cfs:
            self.sdf = cf.apply_to(self.sdf)

        return self

    def write(
        self,
        table_name: str = "metrics",
        write_mode: str = "create_or_replace"
    ):
        """Write the metrics DataFrame to a warehouse table.

        Parameters
        ----------
        table_name : str, optional
            The name of the table to write to, by default "metrics"
        write_mode : str, optional
            The write mode to use, by default "create_or_replace"
            Options are: "create", "append", "overwrite", "create_or_replace"

        Example
        -------
        >>> import teehr

        >>> ev = teehr.Evaluation()

        >>> metrics_df = ev.metrics.query(
        >>>     include_metrics=[...],
        >>>     group_by=["primary_location_id"]
        >>> ).write_to_warehouse(
        >>>     table_name="metrics",
        >>>     write_mode="create_or_replace"
        >>> )
        """
        logger.info(
            f"Writing metrics results to the warehouse table: {table_name}."
        )
        self._write.to_warehouse(
            source_data=self.sdf,
            table_name=table_name,
            write_mode=write_mode
        )
        return self