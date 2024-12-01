"""Access methods to units table."""

from teehr.models.str_enum import StrEnum
from teehr.querying.filter_format import validate_and_apply_filters
import pyspark.sql as ps
from typing import List, Union
# from pyspark.sql.functions import col
from pathlib import Path
from teehr.querying.utils import order_df
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path, path_to_spark
# from teehr.models.pydantic_table_models import TableBaseModel
from teehr.models.filters import FilterBaseModel
from pyspark.errors import AnalysisException

import logging

logger = logging.getLogger(__name__)


class BaseTable():
    """Base query class."""

    def __init__(self, ev):
        """Initialize class."""
        self.ev = ev
        self.dir = None
        self.schema_func = None
        self.format = None
        self.save_mode = "error"
        self.partition_by = None
        self.spark = ev.spark
        self.df: ps.DataFrame = None
        # self.table_model: TableBaseModel = None
        self.filter_model: FilterBaseModel = None
        self.strict_validation = True
        self.validate_filter_field_types = True

    @staticmethod
    def _raise_missing_table_error():
        """Raise an error if the table does not exist."""
        err_msg = (
            "The requested table does not exist in the dataset."
            " Please load it first."
        )
        logger.error(err_msg)
        raise ValueError(err_msg)

    def _read_files(
            self,
            path: Union[str, Path, S3Path],
            pattern: str = None,
            **options
    ) -> ps.DataFrame:
        """Read data from directory as a spark dataframe."""
        logger.info(f"Reading files from {path}.")
        if len(options) == 0:
            options = {
                "header": "true",
                "ignoreMissingFiles": "true"
            }

        path = to_path_or_s3path(path)

        path = path_to_spark(path, pattern)

        # May need to deal with empty files here.
        df = (
            self.ev.spark.read.format(self.format)
            # .schema(self._get_schema().to_structtype())
            .options(**options)
            .load(path)
        )

        return df

    def _load_table(self, **kwargs):
        """Read data from directory as a spark dataframe."""
        logger.info(f"Loading files from {self.dir}.")
        self.df = self._read_files(self.dir, **kwargs)

    def _check_load_table(self):
        """Check if the table is loaded."""
        if self.df is None:
            self._load_table()
        if self.df is None:
            self._raise_missing_table_error()

    def _write_spark_df(self, df: ps.DataFrame, **kwargs):
        """Write spark dataframe to directory."""
        logger.info(f"Writing files to {self.dir}.")
        if len(kwargs) == 0:
            kwargs = {
                "header": "true",
            }

        partition_by = self.partition_by
        if partition_by is None:
            partition_by = []

        df.write.partitionBy(partition_by).format(self.format).mode(self.save_mode).options(**kwargs).save(str(self.dir))

        self._load_table()

    def _get_schema(self, type: str = "pyspark"):
        """Get the primary timeseries schema."""
        if type == "pandas":
            return self.schema_func(type="pandas")

        return self.schema_func()

    def _validate(self, df: ps.DataFrame, strict: bool = True) -> ps.DataFrame:
        """Validate a DataFrame against the schema."""
        schema = self._get_schema()

        logger.info(f"Validating DataFrame with {schema.columns}.")

        if strict:
            schema_cols = schema.columns.keys()
            df = df.select(*schema_cols)

        validated_df = schema.validate(df)

        if len(validated_df.pandera.errors) > 0:
            logger.error(f"Validation failed: {validated_df.pandera.errors}")
            raise ValueError(f"Validation failed: {validated_df.pandera.errors}")

        return validated_df

    def validate(self):
        """Validate the table against the schema."""
        self._check_load_table()
        self._validate(self.df)

    def query(
        self,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        order_by: Union[str, StrEnum, List[Union[str, StrEnum]]] = None
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

        Filters as dictionary:

        >>> ts_df = ev.primary_timeseries.query(
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
        >>> ).to_pandas()

        Filters as string:

        >>> ts_df = ev.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_pandas()

        Filters as FilterBaseModel:

        >>> from teehr.models.filters import TimeseriesFilter
        >>> from teehr.models.filters import FilterOperators
        >>>
        >>> fields = ev.primary_timeseries.field_enum()
        >>> ts_df = ev.primary_timeseries.query(
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
        >>> ]).to_pandas()

        """
        logger.info("Performing the query.")
        self._check_load_table()
        if filters is not None:
            self.df = validate_and_apply_filters(
                sdf=self.df,
                filters=filters,
                filter_model=self.filter_model,
                fields_enum=self.field_enum(),
                dataframe_schema=self._get_schema("pandas"),
                validate=self.validate_filter_field_types
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

        Filters as dictionary:

        >>> ts_df = ev.primary_timeseries.filter(
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
        >>> ).to_pandas()

        Filters as string:

        >>> ts_df = ev.primary_timeseries.filter(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_pandas()

        Filters as FilterBaseModel:

        >>> from teehr.models.filters import TimeseriesFilter
        >>> from teehr.models.filters import FilterOperators
        >>>
        >>> fields = ev.primary_timeseries.field_enum()
        >>> ts_df = ev.primary_timeseries.filter(
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
        >>> ]).to_pandas()

        """
        logger.info(f"Setting filter {filter}.")
        self._check_load_table()
        self.df = validate_and_apply_filters(
            sdf=self.df,
            filters=filters,
            filter_model=self.filter_model,
            fields_enum=self.field_enum(),
            dataframe_schema=self._get_schema("pandas"),
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

        Order by string:

        >>> ts_df = ev.primary_timeseries.order_by("value_time").to_df()

        Order by StrEnum:

        >>> from teehr.querying.field_enums import TimeseriesFields
        >>> ts_df = ev.primary_timeseries.order_by(
        >>>     TimeseriesFields.value_time
        >>> ).to_pandas()

        """
        logger.info(f"Setting order_by {fields}.")
        self._check_load_table()
        self.df = order_df(self.df, fields)
        return self

    def fields(self) -> List[str]:
        """Return table columns as a list."""
        self._check_load_table()
        return self.df.columns

    def distinct_values(self, column: str) -> List[str]:
        """Return distinct values for a column."""
        self._check_load_table()
        return self.df.select(column).distinct().rdd.flatMap(lambda x: x).collect()

    def field_enum(self) -> StrEnum:
        """Get the fields enum."""
        raise NotImplementedError("field_enum method must be implemented.")

    def to_pandas(self):
        """Return Pandas DataFrame."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = None
        df.attrs['fields'] = None
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        raise NotImplementedError("to_geopandas method must be implemented.")

    def to_sdf(self):
        """Return PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called.  For example, calling `show()`, `collect()` or toPandas().
        This can be useful for further processing or analysis, for example,

        >>> ts_sdf = ev.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ]
        >>> ).to_sdf()
        >>> ts_df = (
        >>>     ts_sdf.select("value_time", "location_id", "value")
        >>>    .orderBy("value").toPandas()
        >>> )
        >>> ts_df.head()
        """
        self._check_load_table()
        return self.df


