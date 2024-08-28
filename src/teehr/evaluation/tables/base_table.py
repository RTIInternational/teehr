"""Access methods to units table."""
from typing import Union, List
from teehr.querying.filter_format import validate_and_apply_filters
from teehr.querying.utils import order_df
from teehr.models.enums import StrEnum

from teehr.models.dataset.table_models import (
    TableBaseModel,
)
from teehr.models.dataset.filters import (
    FilterBaseModel,

)
from pyspark.sql import SparkSession, DataFrame

import logging

logger = logging.getLogger(__name__)


class BaseTable():
    """Base query class."""

    def __init__(self, eval):
        """Initialize class."""
        self.dir = None
        self.spark: SparkSession = None
        self.table_model: TableBaseModel = None
        self.filter_model: FilterBaseModel = None
        self.df: DataFrame = None

    def query(
        self,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        order_by: Union[str, List[str], StrEnum, List[StrEnum]] = None
    ):
        """Run a query against the table with filters and order_by.

        In general a user will either use the query methods or the filter and
        order_by methods.  The query method is a convenience method that will
        apply filters and order_by in a single call.

        Parameters
        ----------
        filters : Union[
                str, dict, FilterBaseModel,
                List[Union[str, dict, FilterBaseModel]]
            ]
            The filters to apply to the query.  The filters can be a string,
            dictionary, FilterBaseModel or a list of any of these.  The filters

        order_by : Union[str, List[str], StrEnum, List[StrEnum]]
            The fields to order the query by.  The fields can be a string,
            StrEnum or a list of any of these.  The fields will be ordered in
            the order they are provided.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Filters as dictionary
        >>> ts_df = eval.primary_timeseries.query(
        >>>     filters=[
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": ">",
        >>>             "value": "2022-01-01",
        >>>         },
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": "<",
        >>>             "value": "2022-01-02",
        >>>         },
        >>>         {
        >>>             "column": "location_id",
        >>>             "operator": "=",
        >>>             "value": "gage-C",
        >>>         },
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_df()

        Filters as string
        >>> ts_df = eval.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_df()

        Filters as FilterBaseModel
        >>> from teehr.models.dataset.filters import TimeseriesFilter
        >>> from teehr.models.dataset.filters import FilterOperators
        >>>
        >>> fields = eval.primary_timeseries.field_enum()
        >>> ts_df = eval.primary_timeseries.query(
        >>>     filters=[
        >>>         TimeseriesFilter(
        >>>             column=fields.value_time,
        >>>             operator=FilterOperators.gt,
        >>>             value="2022-01-01",
        >>>         ),
        >>>         TimeseriesFilter(
        >>>             column=fields.value_time,
        >>>             operator=FilterOperators.lt,
        >>>             value="2022-01-02",
        >>>         ),
        >>>         TimeseriesFilter(
        >>>             column=fields.location_id,
        >>>             operator=FilterOperators.eq,
        >>>             value="gage-C",
        >>>         ),
        >>> ]).to_df()

        """
        if filters is not None:
            self.df = validate_and_apply_filters(
                sdf=self.df,
                filters=filters,
                filter_model=self.filter_model,
                fields_enum=self.field_enum(),
                table_model=self.table_model
            )
        if order_by is not None:
            self.df = order_df(self.df, order_by)
        return self

    def filter(
        self,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ]
    ):
        """Apply a filter.

        Parameters
        ----------
        filters : Union[
                str, dict, FilterBaseModel,
                List[Union[str, dict, FilterBaseModel]]
            ]
            The filters to apply to the query.  The filters can be a string,
            dictionary, FilterBaseModel or a list of any of these.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Filters as dictionary
        >>> ts_df = eval.primary_timeseries.filter(
        >>>     filters=[
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": ">",
        >>>             "value": "2022-01-01",
        >>>         },
        >>>         {
        >>>             "column": "value_time",
        >>>             "operator": "<",
        >>>             "value": "2022-01-02",
        >>>         },
        >>>         {
        >>>             "column": "location_id",
        >>>             "operator": "=",
        >>>             "value": "gage-C",
        >>>         },
        >>>     ]
        >>> ).to_df()

        Filters as string
        >>> ts_df = eval.primary_timeseries.filter(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_df()

        Filters as FilterBaseModel
        >>> from teehr.models.dataset.filters import TimeseriesFilter
        >>> from teehr.models.dataset.filters import FilterOperators
        >>>
        >>> fields = eval.primary_timeseries.field_enum()
        >>> ts_df = eval.primary_timeseries.filter(
        >>>     filters=[
        >>>         TimeseriesFilter(
        >>>             column=fields.value_time,
        >>>             operator=FilterOperators.gt,
        >>>             value="2022-01-01",
        >>>         ),
        >>>         TimeseriesFilter(
        >>>             column=fields.value_time,
        >>>             operator=FilterOperators.lt,
        >>>             value="2022-01-02",
        >>>         ),
        >>>         TimeseriesFilter(
        >>>             column=fields.location_id,
        >>>             operator=FilterOperators.eq,
        >>>             value="gage-C",
        >>>         ),
        >>> ]).to_df()

        """
        logger.info(f"Setting filter {filter}.")
        self.df = validate_and_apply_filters(
            sdf=self.df,
            filters=filters,
            filter_model=self.filter_model,
            fields_enum=self.field_enum(),
            table_model=self.table_model
        )
        return self

    def order_by(
        self,
        fields: Union[str, StrEnum, List[Union[str, StrEnum]]]
    ):
        """Apply an order_by.

        Parameters
        ----------
        fields : Union[str, StrEnum, List[Union[str, StrEnum]]]
            The fields to order the query by.  The fields can be a string,
            StrEnum or a list of any of these.  The fields will be ordered in
            the order they are provided.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------
        Order by string
        >>> ts_df = eval.primary_timeseries.order_by("value_time").to_df()

        Order by StrEnum
        >>> from teehr.querying.field_enums import TimeseriesFields
        >>> ts_df = eval.primary_timeseries.order_by(TimeseriesFields.value_time).to_df()

        """

        self.df = order_df(self.df, fields)
        return self

    def fields(self):
        """Return table columns as a list."""
        return self.df.columns

    def field_enum(self) -> StrEnum:
        """Get the fields enum."""
        raise NotImplementedError("field_enum method must be implemented.")

    def to_df(self):
        """Return Pandas DataFrame."""
        return self.df.toPandas()

    def to_sdf(self):
        """Return PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called.  For example, calling `show()`, `collect()` or toPandas().
        This can be useful for further processing or analysis, for example,

        >>> ts_sdf = eval.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_sdf()
        >>> ts_df = ts_sdf.select("value_time", "location_id", "value").orderBy("value").toPandas()
        >>> ts_df.head()
        """
        return self.df
