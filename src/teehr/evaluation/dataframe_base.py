"""Base class for DataFrame access patterns (Tables and Views)."""
from abc import ABC
from typing import List, Union
import logging

from teehr.models.str_enum import StrEnum
from teehr.querying.utils import (
    df_to_gdf,
    join_attributes,
    join_geometry,
    order_df,
    group_df,
    post_process_metric_results
)
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
from teehr.models.filters import TableFilter
from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.querying.metric_format import apply_aggregation_metrics
import pyspark.sql as ps

logger = logging.getLogger(__name__)


class TeehrDataFrameBase(ABC):
    """Abstract base class for DataFrame-based data access.

    This class provides the common interface and implementation for both:
    - Tables: Read from persisted iceberg tables
    - Views: Computed on-the-fly from other data sources

    Subclasses must implement the `sdf` property to provide access to
    the underlying Spark DataFrame.
    """

    def __init__(self, ev):
        """Initialize the TeehrDataFrameBase.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related operations.
        """
        self._ev = ev
        self._write = ev._write
        self._sdf: ps.DataFrame = None
        self._has_geometry = None

    def to_sdf(self) -> ps.DataFrame:
        """Return the PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called (e.g., show(), collect(), toPandas()).

        Returns
        -------
        ps.DataFrame
            The Spark DataFrame.
        """
        return self._sdf

    def to_pandas(self):
        """Return Pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            The data as a Pandas DataFrame.
        """
        df = self.to_sdf().toPandas()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame.

        Returns
        -------
        gpd.GeoDataFrame
            The data as a GeoPandas DataFrame.
        """
        if self._has_geometry:
            logger.debug("DataFrame already has geometry. Converting to GeoPandas.")
            gdf = df_to_gdf(self.to_pandas())
            return gdf
        gdf = df_to_gdf(self.add_geometry().to_pandas())
        return gdf

    def add_geometry(self):
        """Add geometry to the DataFrame by joining with the locations table."""
        sdf = self.to_sdf()
        gdf = join_geometry(sdf, self._ev.locations.to_sdf())
        self._sdf = gdf
        self._has_geometry = True
        return self

    def add_attributes(
        self,
        attr_list: List[str] = None,
        location_id_col: str = None,
    ):
        """Add location attributes to the DataFrame.

        Joins pivoted location attributes to the DataFrame. The join column
        is auto-detected from common location ID field names ('location_id',
        'primary_location_id') unless specified.

        This is especially useful when called *after* a ``aggregate()`` with
        GROUP BY and aggregation metrics, so that attributes do not need
        to be included in the ``group_by`` clause in order to pass through
        to the result.

        Parameters
        ----------
        attr_list : List[str], optional
            Specific attributes to add. If None, all attributes are added.
        location_id_col : str, optional
            The column name in the DataFrame to join on. If None, checks
            for 'location_id' then 'primary_location_id'.

        Returns
        -------
        self
            Returns self for method chaining.

        Examples
        --------
        Add all attributes:

        >>> df = accessor.add_attributes().to_pandas()

        Add specific attributes:

        >>> df = accessor.add_attributes(
        ...     attr_list=["drainage_area", "ecoregion"]
        ... ).to_pandas()

        Specify join column explicitly:

        >>> df = accessor.add_attributes(
        ...     location_id_col="primary_location_id"
        ... ).to_pandas()

        Add attributes after metric aggregation — avoids including them
        in ``group_by``:

        >>> from teehr.metrics import KGE
        >>> df = (
        ...     ev.joined_timeseries_view()
        ...     .aggregate(
        ...         group_by=["primary_location_id"],
        ...         metrics=[KGE()]
        ...     )
        ...     .add_attributes(attr_list=["drainage_area", "ecoregion"])
        ...     .to_pandas()
        ... )
        """
        attrs_sdf = self._ev.location_attributes_view(attr_list=attr_list).to_sdf()

        if attrs_sdf.isEmpty():
            logger.warning(
                "No location attributes found. Skipping adding attributes."
            )
            return self

        sdf = self.to_sdf()
        self._sdf = join_attributes(sdf, attrs_sdf, location_id_col)
        return self

    def _apply_filters(
        self,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ],
        validate: bool = False
    ):
        """Apply filters to the DataFrame.

        Parameters
        ----------
        filters : Union[str, dict, TableFilter, List[...]]
            The filters to apply.
        validate : bool, optional
            Whether to validate filter field types. Default is False.
        """
        if not isinstance(filters, list):
            filters = [filters]

        # Use to_sdf() to ensure computation (for Views)
        sdf = self.to_sdf()
        validated_filters = self._ev._validate.sdf_filters(
            sdf=sdf,
            filters=filters,
            validate=validate
        )
        for f in validated_filters:
            sdf = sdf.filter(f)
        self._sdf = sdf

    def filter(
        self,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None
    ):
        """Apply filters to the DataFrame.

        Parameters
        ----------
        filters : Union[str, dict, TableFilter, List[...]]
            The filters to apply. Can be SQL strings, dictionaries,
            or TableFilter objects.

        Returns
        -------
        self
            Returns self for method chaining.

        Examples
        --------
        Filters as dictionary:

        >>> df = accessor.filter(
        >>>     filters=[
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": ">",
        >>>             "value": "2022-01-01",
        >>>         },
        >>>     ]
        >>> ).to_pandas()

        Filters as string:

        >>> df = accessor.filter(
        >>>     filters=["value_time > '2022-01-01'"]
        >>> ).to_pandas()
        """
        if filters is None:
            logger.info(
                "No filters provided to filter method. "
                "Returning unfiltered data."
            )
            return self

        logger.info(f"Setting filter {filters}.")
        self._apply_filters(filters)
        return self

    def order_by(
        self,
        fields: Union[str, StrEnum, List[Union[str, StrEnum]]]
    ):
        """Apply ordering to the DataFrame.

        Parameters
        ----------
        fields : Union[str, StrEnum, List[...]]
            The fields to order by.

        Returns
        -------
        self
            Returns self for method chaining.

        Examples
        --------
        >>> df = accessor.order_by("value_time").to_pandas()
        """
        logger.info(f"Setting order_by {fields}.")
        self._sdf = order_df(self.to_sdf(), fields)
        return self

    def aggregate(
        self,
        group_by: Union[str, List[str]],
        metrics: List[MetricsBasemodel]
    ):
        """Aggregate data with grouping and metrics.

        Parameters
        ----------
        group_by : Union[str, List[str]]
            Fields to group by for metric calculation.
        metrics : List[MetricsBasemodel]
            Metrics to calculate.

        Returns
        -------
        self
            Returns self for method chaining.

        Examples
        --------
        >>> df = accessor.aggregate(
        >>>     metrics=[KGE()],
        >>>     group_by=["primary_location_id"]
        >>> ).to_pandas()

        Chain with filter and order_by:

        >>> from teehr import DeterministicMetrics as dm
        >>>
        >>> df = (
        >>>     accessor
        >>>     .filter("primary_location_id LIKE 'usgs%'")
        >>>     .aggregate(
        >>>         group_by=["primary_location_id", "configuration_name"],
        >>>         metrics=[dm.KlingGuptaEfficiency(), dm.RelativeBias()]
        >>>     )
        >>>     .order_by(["primary_location_id", "configuration_name"])
        >>>     .to_pandas()
        >>> )

        """
        logger.info("Performing the aggregation.")

        logger.debug(f"Grouping by '{group_by}' and applying metrics.")
        gp = group_df(self.to_sdf(), group_by)

        sdf = apply_aggregation_metrics(
            gp=gp,
            include_metrics=metrics,
        )
        self._sdf = post_process_metric_results(
            metrics_sdf=sdf,
            include_metrics=metrics,
            group_by=group_by
        )

        return self

    def add_calculated_fields(
        self,
        cfs: Union[CalculatedFieldBaseModel, List[CalculatedFieldBaseModel]]
    ):
        """Add calculated fields to the DataFrame.

        Parameters
        ----------
        cfs : Union[CalculatedFieldBaseModel, List[...]]
            The calculated fields to add.

        Returns
        -------
        self
            Returns self for method chaining.

        Examples
        --------
        >>> import teehr
        >>> from teehr import RowLevelCalculatedFields as rcf
        >>>
        >>> df = accessor.add_calculated_fields([
        >>>     rcf.Month()
        >>> ]).to_pandas()
        """
        if not isinstance(cfs, list):
            cfs = [cfs]

        sdf = self.to_sdf()
        for cf in cfs:
            sdf = cf.apply_to(sdf)
        self._sdf = sdf

        return self

    def write(
        self,
        table_name: str,
        write_mode: str = "create_or_replace"
    ):
        """Write the DataFrame to an iceberg table.

        .. deprecated::
            Use :meth:`write_to` instead. This method will be removed in a
            future release.

        Parameters
        ----------
        table_name : str
            The name of the table to write to.
        write_mode : str, optional
            The write mode. Options:

            - ``"insert"``: Insert all rows directly without duplicate checking.
            - ``"append"``: Insert new rows; skip rows matching uniqueness fields.
            - ``"upsert"``: Insert new rows; update existing rows matching uniqueness fields.
            - ``"overwrite"``: Replace all data, preserving table history.
            - ``"create_or_replace"``: Drop and recreate table. Loses history.

            Default is "create_or_replace".

        Returns
        -------
        self
            Returns self for method chaining.
        """
        import warnings
        warnings.warn(
            "write() is deprecated, use write_to() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return self.write_to(table_name=table_name, write_mode=write_mode)

    def write_to(
        self,
        table_name: str,
        write_mode: str = "create_or_replace"
    ):
        """Write the DataFrame to an iceberg table.

        Parameters
        ----------
        table_name : str
            The name of the table to write to.
        write_mode : str, optional
            The write mode. Options:

            - ``"insert"``: Insert all rows directly without duplicate checking.
            - ``"append"``: Insert new rows; skip rows matching uniqueness fields.
            - ``"upsert"``: Insert new rows; update existing rows matching uniqueness fields.
            - ``"overwrite"``: Replace all data, preserving table history.
            - ``"create_or_replace"``: Drop and recreate table. Loses history.

            Default is "create_or_replace".

        Returns
        -------
        self
            Returns self for method chaining.

        Examples
        --------
        >>> accessor.aggregate(
        ...     metrics=[KGE()],
        ...     group_by=["primary_location_id"]
        ... ).write_to("location_metrics")
        """
        logger.info(f"Writing to table: {table_name}.")

        # Throw error if table is a core table to prevent accidental overwrites
        is_core_table = self._ev.table(table_name).is_core_table
        if is_core_table:
            raise ValueError(
                f"Cannot write to core table: {table_name} with this method. "
                f"Use the load_dataframe() method on the table instead."
            )
        self._write.to_warehouse(
            source_data=self.to_sdf(),
            table_name=table_name,
            write_mode=write_mode
        )
        return self
