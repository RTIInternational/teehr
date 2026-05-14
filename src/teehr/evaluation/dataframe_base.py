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
    post_process_metric_results
)
from teehr.calculated_fields.models.base import CalculatedFieldBaseModel
from teehr.models.filters import TableFilter
from teehr.metrics.models.base import MetricsBasemodel
from teehr.metrics.engine import aggregate_metrics_with_engine
from teehr.calculated_fields.engine import (
    apply_calculated_fields_with_engine,
)
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
        return self._with_sdf(gdf, has_geometry=True)

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
        TeehrDataFrameBase
            A new accessor instance with attributes joined.

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
            return self._with_sdf(self.to_sdf())

        sdf = self.to_sdf()
        joined_sdf = join_attributes(sdf, attrs_sdf, location_id_col)
        return self._with_sdf(joined_sdf)

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
        return sdf

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
        TeehrDataFrameBase
            A new accessor instance with filters applied.

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
            return self._with_sdf(self.to_sdf())

        logger.info(f"Setting filter {filters}.")
        filtered_sdf = self._apply_filters(filters)
        return self._with_sdf(filtered_sdf)

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
        TeehrDataFrameBase
            A new accessor instance with ordering applied.

        Examples
        --------
        >>> df = accessor.order_by("value_time").to_pandas()
        """
        logger.info(f"Setting order_by {fields}.")
        ordered_sdf = order_df(self.to_sdf(), fields)
        return self._with_sdf(ordered_sdf)

    def aggregate(
        self,
        group_by: Union[str, List[str]],
        metrics: List[MetricsBasemodel],
        engine: str = "auto",
    ):
        """Aggregate data with grouping and metrics.

        Parameters
        ----------
        group_by : Union[str, List[str]]
            Fields to group by for metric calculation.
        metrics : List[MetricsBasemodel]
            Metrics to calculate.
        engine : str, optional
            Aggregation engine to use. Options are ``"auto"``,
            ``"python"``, or ``"spark"``. Default is ``"auto"``.

        Returns
        -------
        TeehrDataFrameBase
            A new accessor instance with aggregation results.

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

        logger.debug(
            f"Grouping by '{group_by}' and applying metrics with "
            f"engine='{engine}'."
        )
        sdf = aggregate_metrics_with_engine(
            sdf=self.to_sdf(),
            group_by=group_by,
            metrics=metrics,
            engine=engine,
        )
        metrics_sdf = post_process_metric_results(
            metrics_sdf=sdf,
            include_metrics=metrics,
            group_by=group_by
        )
        return self._with_sdf(metrics_sdf)

    def add_calculated_fields(
        self,
        cfs: Union[CalculatedFieldBaseModel, List[CalculatedFieldBaseModel]],
        engine: str = "auto",
    ):
        """Add calculated fields to the DataFrame.

        Parameters
        ----------
        cfs : Union[CalculatedFieldBaseModel, List[...]]
            The calculated fields to add.
        engine : str, optional
            Execution engine for calculated fields. Options are ``"auto"``,
            ``"python"``, or ``"spark"``. Default is ``"auto"``.

        Returns
        -------
        TeehrDataFrameBase
            A new accessor instance with calculated fields added.

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

        sdf = apply_calculated_fields_with_engine(
            sdf=self.to_sdf(),
            cfs=cfs,
            engine=engine,
        )
        return self._with_sdf(sdf)

    def write(
        self,
        table_name: str,
        write_mode: str = "create_or_replace",
        uniqueness_fields: list[str] | None = None,
        nullable_fields: list[str] | None = None,
        partition_by: list[str] | None = None,
        write_ordered_by: list[str] | None = None,
        use_partition_filters: bool = True,
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
        uniqueness_fields : list[str], optional
            Explicit uniqueness fields to use for custom-table append or
            upsert writes. If omitted, uses the target table metadata.
        nullable_fields : list[str], optional
            Explicit nullable uniqueness fields to compare with null-safe
            equality during append or upsert writes. If omitted, uses the
            target table schema when available.
        partition_by : list[str], optional
            Partition expressions to use when creating a custom table with
            ``write_mode="create_or_replace"``.
        write_ordered_by : list[str], optional
            Field names to use for Iceberg table write order via
            ``ALTER TABLE ... WRITE ORDERED BY``. Each field is written as
            ``ASC NULLS LAST``.
        use_partition_filters : bool, optional
            Whether to add partition-based predicates for MERGE partition
            pruning. Default is True.

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
        return self.write_to(
            table_name=table_name,
            write_mode=write_mode,
            uniqueness_fields=uniqueness_fields,
            nullable_fields=nullable_fields,
            partition_by=partition_by,
            write_ordered_by=write_ordered_by,
            use_partition_filters=use_partition_filters,
        )

    def write_to(
        self,
        table_name: str,
        write_mode: str = "create_or_replace",
        uniqueness_fields: list[str] | None = None,
        nullable_fields: list[str] | None = None,
        partition_by: list[str] | None = None,
        write_ordered_by: list[str] | None = None,
        use_partition_filters: bool = True,
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
        uniqueness_fields : list[str], optional
            Explicit uniqueness fields to use for custom-table append or
            upsert writes. If omitted, uses the target table metadata.
        nullable_fields : list[str], optional
            Explicit nullable uniqueness fields to compare with null-safe
            equality during append or upsert writes. If omitted, uses the
            target table schema when available.
        partition_by : list[str], optional
            Partition expressions to use when creating a custom table with
            ``write_mode="create_or_replace"``.
        write_ordered_by : list[str], optional
            Field names to use for Iceberg table write order via
            ``ALTER TABLE ... WRITE ORDERED BY``. Each field is written as
            ``ASC NULLS LAST``.
        use_partition_filters : bool, optional
            Whether to add partition-based predicates for MERGE partition
            pruning. Default is True.

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
            write_mode=write_mode,
            uniqueness_fields=uniqueness_fields,
            nullable_fields=nullable_fields,
            partition_by=partition_by,
            write_ordered_by=write_ordered_by,
            use_partition_filters=use_partition_filters,
        )
        return self

    def _with_sdf(
        self,
        sdf: ps.DataFrame,
        has_geometry: bool | None = None,
    ):
        """Return a new accessor instance wrapping the provided DataFrame."""
        new_table = self.__class__(self._ev)
        new_table._sdf = sdf
        # View subclasses lazily recompute in to_sdf() unless marked computed.
        # When cloning with an already-transformed DataFrame, preserve it.
        if hasattr(new_table, "_computed"):
            new_table._computed = True
        new_table._has_geometry = self._has_geometry if has_geometry is None else has_geometry
        return new_table

    def __getattr__(self, name):
        """Proxy attribute access to the underlying Spark DataFrame.

        Allows calling PySpark DataFrame methods directly on the TEEHR
        object. If the proxied method returns a new Spark DataFrame, it is
        wrapped in a new instance of this class to preserve method chaining.

        Parameters
        ----------
        name : str
            The attribute or method name to look up on the Spark DataFrame.

        Returns
        -------
        Any
            The attribute value or a wrapper function. If the underlying
            Spark method returns a DataFrame, a new instance of this class
            wrapping that DataFrame is returned.

        Raises
        ------
        AttributeError
            If Spark proxying is disabled, or if the underlying Spark
            DataFrame cannot be resolved.
        """
        try:
            ev = object.__getattribute__(self, '_ev')
            proxy_enabled = getattr(ev, 'enable_spark_proxy', False)
        except AttributeError:
            proxy_enabled = False

        if not proxy_enabled:
            raise AttributeError(
                f"'{name}' is not a TEEHR method. "
                f"Use .to_sdf() for direct Spark DataFrame access, "
                f"or set enable_spark_proxy=True on your Evaluation to enable "
                f"transparent proxying of PySpark DataFrame methods."
            )

        try:
            sdf = self.to_sdf()
        except Exception as exc:
            raise AttributeError(
                f"Unable to resolve Spark DataFrame for proxy attribute '{name}'."
            ) from exc

        attr = getattr(sdf, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, ps.DataFrame):
                    return self._with_sdf(result)
                return result
            return wrapper
        return attr
