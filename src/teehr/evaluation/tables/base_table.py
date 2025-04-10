"""Base class for tables."""

from teehr.models.str_enum import StrEnum
from teehr.querying.filter_format import validate_and_apply_filters
import pyspark.sql as ps
from typing import List, Union
from pathlib import Path
from teehr.querying.utils import order_df
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path, path_to_spark
from teehr.models.filters import FilterBaseModel
from teehr.models.table_enums import TableWriteEnum
import logging
from pyspark.sql.functions import lit, col, row_number, asc
from pyspark.sql.window import Window
import shutil


logger = logging.getLogger(__name__)


class BaseTable():
    """Base table class."""

    def __init__(self, ev):
        """Initialize class."""
        self.ev = ev
        self.name = None
        self.dir = None
        self.schema_func = None
        self.format = None
        self.partition_by = None
        self.spark = ev.spark
        self.df: ps.DataFrame = None
        self.filter_model: FilterBaseModel = None
        self.strict_validation = True
        self.validate_filter_field_types = True

    @staticmethod
    def _raise_missing_table_error(table_name: str):
        """Raise an error if the table does not exist."""
        err_msg = (
            f"The '{table_name}' table does not exist in the dataset."
            " Please load it first."
        )
        logger.error(err_msg)
        raise ValueError(err_msg)

    def _drop_duplicates(
        self,
        sdf: ps.DataFrame,
    ) -> ps.DataFrame:
        """Drop duplicates from the DataFrame."""
        logger.info(f"Dropping duplicates from {self.name}.")
        window_spec = Window.partitionBy(*self.unique_column_set)
        ordering_expr = [asc(col) for col in self.unique_column_set]
        window_spec = window_spec.orderBy(*ordering_expr)
        sdf_with_row_num = sdf.withColumn("row_num", row_number().over(window_spec))
        deduplicated_sdf = sdf_with_row_num.filter("row_num == 1").drop("row_num")
        return deduplicated_sdf

    def _read_files(
        self,
        path: Union[str, Path, S3Path],
        pattern: str = None,
        use_table_schema: bool = False,
        show_missing_table_warning: bool = True,
        **options
    ) -> ps.DataFrame:
        """Read data from table directory as a spark dataframe.

        Parameters
        ----------
        path : Union[str, Path, S3Path]
            The path to the directory containing the files.
        pattern : str, optional
            The pattern to match files.
        use_table_schema : bool, optional
            If True, use the table schema to read the files.
            Missing files will be ignored with 'ignoreMissingFiles'
            set to True (default).
        show_missing_table_warning : bool, optional
            If True, show the warning an empty table was returned.
            The default is True.
        **options
            Additional options to pass to the spark read method.

        Returns
        -------
        df : ps.DataFrame
            The spark dataframe.
        """
        logger.info(f"Reading files from {path}.")
        if len(options) == 0:
            options = {
                "header": "true",
                "ignoreMissingFiles": "true"
            }

        path = to_path_or_s3path(path)

        path = path_to_spark(path, pattern)

        if use_table_schema is True:
            schema = self.schema_func().to_structtype()
            df = self.ev.spark.read.format(self.format).options(**options).load(path, schema=schema)
            if len(df.head(1)) == 0 and show_missing_table_warning:
                logger.warning(f"An empty dataframe was returned for '{self.name}'.")
        else:
            df = self.ev.spark.read.format(self.format).options(**options).load(path)

        return df

    def _load_table(self, **kwargs):
        """Load the table from the directory to self.df.

        Parameters
        ----------
        **kwargs
            Additional options to pass to the spark read method.
        """
        logger.info(f"Loading files from {self.dir}.")
        self.df = self._read_files(self.dir, **kwargs)

    def _check_load_table(self):
        """Check if the table is loaded.

        If the table is not loaded, try to load it.  If the table is still
        not loaded, raise an error.
        """
        if self.df is None:
            self._load_table()
        if self.df is None:
            self._raise_missing_table_error(table_name=self.name)

    def _upsert_without_duplicates(
        self,
        df,
        num_partitions: int = None,
        **kwargs
    ):
        """Update existing data and append new data without duplicates."""
        logger.info(
            f"Upserting to {self.name} without duplicates."
        )
        partition_by = self.partition_by
        if partition_by is None:
            partition_by = []

        existing_sdf = self._read_files(
            self.dir,
            use_table_schema=True,
            show_missing_table_warning=False,
            **kwargs
        )

        # Limit the dataframe to the partitions that are being updated.
        for partition in partition_by:
            partition_values = df.select(partition).distinct(). \
                rdd.flatMap(lambda x: x).collect()
            if partition_values[0] is not None:  # all null partition values
                existing_sdf = existing_sdf.filter(
                    col(partition).isin(partition_values)
                )

        # Remove rows from existing_sdf that are to be updated.
        # Concat and re-write.
        if not existing_sdf.isEmpty():
            existing_sdf = existing_sdf.join(
                df,
                how="left_anti",
                on=self.unique_column_set,
            )
            df = existing_sdf.unionByName(df)
            # Get columns in correct order
            df = df.select([*self.schema_func().columns])

        # Re-validate since the table was changed
        validated_df = self._validate(df)

        # Drop potential duplicates
        validated_df = self._drop_duplicates(validated_df)

        if num_partitions is not None:
            validated_df = validated_df.repartition(num_partitions)
        (
            validated_df.
            write.
            partitionBy(partition_by).
            format(self.format).
            mode("overwrite").
            options(**kwargs).
            save(str(self.dir))
        )

    def _append_without_duplicates(
        self,
        df,
        num_partitions: int = None,
        **kwargs
    ):
        """Append new data without duplicates."""
        logger.info(
            f"Appending to {self.name} without duplicates."
        )
        partition_by = self.partition_by
        if partition_by is None:
            partition_by = []

        existing_sdf = self._read_files(
            self.dir,
            use_table_schema=True,
            show_missing_table_warning=False,
            **kwargs
        )

        # existing_sdf = existing_sdf.persist()

        # Anti-join: Joins rows from left df that do not have a match
        # in right df.  This is used to drop duplicates. df gets written
        # in append mode.
        if not existing_sdf.isEmpty():
            df = df.join(
                existing_sdf,
                how="left_anti",
                on=self.unique_column_set,
            )

        # Only continue if there is new data to write.
        if not df.isEmpty():
            # Re-validate since the table was changed
            validated_df = self._validate(df)

            if num_partitions is not None:
                validated_df = validated_df.repartition(num_partitions)

            # Drop potential duplicates
            validated_df = self._drop_duplicates(validated_df)

            (
                validated_df.
                write.
                partitionBy(partition_by).
                format(self.format).
                mode("append").
                options(**kwargs).
                save(str(self.dir))
            )
        else:
            logger.info(
                f"No new data to append to {self.name}. "
                "Nothing will be written."
            )

    def _dynamic_overwrite(
        self,
        df: ps.DataFrame,
        **kwargs
    ):
        """Overwrite partitions contained in the dataframe."""
        logger.info(
            f"Overwriting table partitions in {self.name}."
        )
        partition_by = self.partition_by
        if partition_by is None:
            partition_by = []
        (
            df.
            write.
            partitionBy(partition_by).
            format(self.format).
            mode("overwrite").
            options(**kwargs).
            save(str(self.dir))
        )

    def _check_for_null_unique_column_values(self, df: ps.DataFrame):
        """Remove a field from self.unique_column_set if all values are null."""
        for field_name in self.unique_column_set:
            if field_name in df.columns:
                if len(df.filter(df[field_name].isNotNull()).collect()) == 0:
                    logger.debug(
                        f"All {field_name} values are null. "
                        f"{field_name} will be removed as a partition column."
                    )
                    self.unique_column_set.remove(field_name)

    def _write_spark_df(
        self,
        df: ps.DataFrame,
        write_mode: TableWriteEnum = "append",
        num_partitions: int = None,
        **kwargs
    ):
        """Write spark dataframe to directory.

        Parameters
        ----------
        df : ps.DataFrame
            The spark dataframe to write.
        **kwargs
            Additional options to pass to the spark write method.
        """
        if self.ev.is_s3:
            logger.error("Writing to S3 is not supported.")
            raise ValueError("Writing to S3 is not supported.")

        logger.info(f"Writing files to {self.dir}.")

        if len(kwargs) == 0:
            kwargs = {
                "header": "true",
            }

        if df is not None:

            self._check_for_null_unique_column_values(df)

            if write_mode == "overwrite":
                self._dynamic_overwrite(df, **kwargs)
                self._load_table()
            elif write_mode == "append":
                self._append_without_duplicates(
                    df=df,
                    num_partitions=num_partitions,
                    **kwargs
                )
            elif write_mode == "upsert":
                self._upsert_without_duplicates(
                    df=df,
                    num_partitions=num_partitions,
                    **kwargs
                )
            else:
                raise ValueError(
                    f"Invalid write mode: {write_mode}. "
                    "Valid values are 'append', 'overwrite' and 'upsert'."
                )
            self._load_table()

    def _get_schema(self, type: str = "pyspark"):
        """Get the primary timeseries schema.

        Parameters
        ----------
        type : str, optional
            The type of schema to return.  Valid values are "pyspark" and
            "pandas".  The default is "pyspark".
        """
        if type == "pandas":
            return self.schema_func(type="pandas")

        return self.schema_func()

    def _validate(
        self,
        df: ps.DataFrame,
        strict: bool = True,
        add_missing_columns: bool = False
    ) -> ps.DataFrame:
        """Validate a DataFrame against the table schema.

        Parameters
        ----------
        df : ps.DataFrame
            The DataFrame to validate.
        strict : bool, optional
            If True, any extra columns will be dropped before validation.
            If False, will be validated as-is.
            The default is True.

        Returns
        -------
        validated_df : ps.DataFrame
            The validated DataFrame.
        """
        schema = self._get_schema()

        logger.info(f"Validating DataFrame with {schema.columns}.")

        schema_cols = schema.columns.keys()

        # Add missing columns
        if add_missing_columns:
            for col_name in schema_cols:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))

        if strict:
            df = df.select(*schema_cols)

        validated_df = schema.validate(df)

        if len(validated_df.pandera.errors) > 0:
            logger.error(f"Validation failed: {validated_df.pandera.errors}")
            raise ValueError(f"Validation failed: {validated_df.pandera.errors}")

        return validated_df

    def validate(self):
        """Validate the dataset table against the schema."""
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
            The filters to apply to the query.  The filters can be an SQL string,
            dictionary, FilterBaseModel or a list of any of these. The filters
            will be applied in the order they are provided.

        order_by : Union[str, List[str], StrEnum, List[StrEnum]]
            The fields to order the query by.  The fields can be a string,
            StrEnum or a list of any of these.  The fields will be ordered in
            the order they are provided.

        Returns
        -------
        self : BaseTable or subclass of BaseTable

        Examples
        --------

        Filters as dictionaries:

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

        Filters as SQL strings:

        >>> ts_df = ev.primary_timeseries.query(
        >>>     filters=[
        >>>         "value_time > '2022-01-01'",
        >>>         "value_time < '2022-01-02'",
        >>>         "location_id = 'gage-C'"
        >>>     ],
        >>>     order_by=["location_id", "value_time"]
        >>> ).to_pandas()

        Filters as FilterBaseModels:

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
            The filters to apply to the query.  The filters can be an SQL string,
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
        df.attrs['table_type'] = self.name
        df.attrs['fields'] = self.fields()
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

