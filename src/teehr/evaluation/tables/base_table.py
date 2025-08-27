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
from teehr.loading.utils import (
    merge_field_mappings,
    validate_constant_values_dict,
    add_or_replace_sdf_column_prefix
)
import logging
from pyspark.sql.functions import lit, col
from functools import reduce
import pandas as pd


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
        self.foreign_keys = []

    @staticmethod
    def _raise_missing_table_error(table_name: str):
        """Raise an error if the table does not exist."""
        err_msg = (
            f"The '{table_name}' table does not exist in the dataset."
            " Please load it first."
        )
        logger.error(err_msg)
        raise ValueError(err_msg)

    def _enforce_foreign_keys(self, sdf: ps.DataFrame):
        """Enforce foreign keys relationships on the timeseries tables."""
        if len(self.foreign_keys) > 0:
            logger.info(
                f"Enforcing foreign key constraints for {self.name}."
            )
        for fk in self.foreign_keys:
            sdf.createOrReplaceTempView("temp_table")
            sql = f"""
                SELECT t.* from temp_table t
                LEFT ANTI JOIN {fk['domain_table']} d
                ON t.{fk['column']} = d.{fk['domain_column']}
            """
            result_sdf = self.ev.sql(
                query=sql, create_temp_views=[fk["domain_table"]]
            )
            self.spark.catalog.dropTempView("temp_table")
            self.spark.catalog.dropTempView(fk["domain_table"])
            if not result_sdf.isEmpty():
                raise ValueError(
                    f"Foreign key constraint violation: "
                    f"A {fk['column']} entry in {self.name} is not found in "
                    f"the {fk['domain_column']} column in {fk['domain_table']}"
                )

    def _read_files(
        self,
        path: Union[str, Path, S3Path],
        pattern: str = None,
        show_missing_table_warning: bool = False,
        **options
    ) -> ps.DataFrame:
        """Read data from table directory as a spark dataframe.

        Parameters
        ----------
        path : Union[str, Path, S3Path]
            The path to the directory containing the files.
        pattern : str, optional
            The pattern to match files.
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
        # First, read the file with the schema and check if it's empty.
        # If it's not empty and it's the joined timeseries table,
        # read it again without the schema to ensure all fields are included.
        # Otherwise, continue.
        schema = self.schema_func().to_structtype()
        df = self.ev.spark.read.format(self.format).options(**options).load(path, schema=schema)
        if df.isEmpty():
            if show_missing_table_warning:
                logger.warning(f"An empty dataframe was returned for '{self.name}'.")

        return df

    def _load_table(self, **kwargs):
        """Load the table from the directory to self.df.

        Parameters
        ----------
        **kwargs
            Additional options to pass to the spark read method.
        """
        logger.info(f"Loading files from {self.dir}.")
        self.df = self._read_files(
            self.dir,
            **kwargs
        )

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
            # Create the join condition clause using eqNullSafe to treat
            # nulls as equal.
            join_condition = reduce(
                lambda x, y: x & y, [df[k].eqNullSafe(existing_sdf[k]) for k in self.unique_column_set]  # noqa: E501
            )
            existing_sdf = existing_sdf.join(
                df,
                how="left_anti",
                on=join_condition,
            )
            df = existing_sdf.unionByName(df)
            # Get columns in correct order
            df = df.select([*self.schema_func().columns])

        if num_partitions is not None:
            df = df.repartition(num_partitions)
        (
            df.
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
        # Anti-join: Joins rows from left df that do not have a match
        # in right df.  This is used to drop duplicates. df gets written
        # in append mode.
        if not existing_sdf.isEmpty():
            # Create the join condition clause using eqNullSafe to treat
            # nulls as equal.
            join_condition = reduce(
                lambda x, y: x & y, [df[k].eqNullSafe(existing_sdf[k]) for k in self.unique_column_set]  # noqa: E501
            )
            df = df.join(
                existing_sdf,
                how="left_anti",
                on=join_condition,
            )

        if num_partitions is not None:
            df = df.repartition(num_partitions)

        # Only continue if there is new data to write.
        if not df.isEmpty():
            (
                df.
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

        if write_mode == "overwrite":
            self._dynamic_overwrite(
                df,
                **kwargs
            )
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
        # self._load_table(show_missing_table_warning=False)

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
        add_missing_columns: bool = False,
        drop_duplicates: bool = True,
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

        if drop_duplicates:
            df = df.dropDuplicates(subset=self.unique_column_set)

        validated_df = schema.validate(df)

        if len(validated_df.pandera.errors) > 0:
            logger.error(f"Validation failed: {validated_df.pandera.errors}")
            raise ValueError(f"Validation failed: {validated_df.pandera.errors}")

        self._enforce_foreign_keys(validated_df)

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
        Note: The filter method is universal for all table types. When
        repurposing this example, ensure filter arguments (e.g., column names,
        values) are valid for the specific table type.

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
            validate=self.validate_filter_field_types
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

    def distinct_values(self,
                        column: str,
                        location_prefixes: bool = False
                        ) -> List[str]:
        """Return distinct values for a column.

        Parameters
        ----------
        column : str
            The column to get distinct values for.
        location_prefixes : bool
            Whether to return location prefixes. If True, only the unique
            prefixes of the locations will be returned. Only compatible with
            primary_timeseries, secondary_timeseries, joined_timeseries,
            locations, location_attributes, and location_crosswalk tables and
            their respective location columns.
            Default: False

        Returns
        -------
        List[str]
            The distinct values for the column.
        """
        self._check_load_table()
        if column not in self.df.columns:
            raise ValueError(
                f"Invalid column: '{column}' for table: '{self.name}'"
            )
        if location_prefixes:
            # ensure valid table
            valid_tables = ['primary_timeseries',
                            'secondary_timeseries',
                            'joined_timeseries',
                            'locations',
                            'location_attributes',
                            'location_crosswalks']
            if self.name not in valid_tables:
                raise ValueError(
                    f"""
                    Invalid table: '{self.name}' with argument
                    location_prefixes==True. Valid tables are: {valid_tables}
                    """
                    )
            # ensure valid columns for selected table
            valid_columns = {'primary_timeseries': ['location_id'],
                             'secondary_timeseries': ['location_id'],
                             'joined_timeseries': ['primary_location_id',
                                                   'secondary_location_id'],
                             'locations': ['id'],
                             'location_attributes': ['location_id'],
                             'location_crosswalks': ['primary_location_id',
                                                     'secondary_location_id']
                             }
            if column not in valid_columns[self.name]:
                raise ValueError(
                    f"""
                    Invalid column: '{column}' for table: '{self.name}' with
                    argument location_prefixes==True. Valid columns are:
                    {valid_columns[self.name]}
                    """
                )
            # get unique location prefixes
            unique_locations = self.df.select(column).distinct().rdd.flatMap(
                lambda x: x
                ).collect()
            prefixes = [location.split('-')[0] for location
                        in unique_locations
                        ]
            return list(set(prefixes))

        else:
            return self.df.select(column).distinct().rdd.flatMap(
                lambda x: x
                ).collect()

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

    def _load_dataframe(
        self,
        df: Union[pd.DataFrame, ps.DataFrame],
        field_mapping: dict,
        constant_field_values: dict,
        location_id_prefix: str,
        write_mode: TableWriteEnum,
        persist_dataframe: bool,
        drop_duplicates: bool
    ):
        """Load a timeseries from an in-memory dataframe."""
        if (isinstance(df, ps.DataFrame) and df.isEmpty()) or (
            isinstance(df, pd.DataFrame) and df.empty
        ):
            logger.debug(
                "The input dataframe is empty. "
                "No data will be loaded into the table."
            )
            return
        default_field_mapping = {}
        fields = self.schema_func(type="pandas").columns.keys()
        for field in fields:
            if field not in default_field_mapping.values():
                default_field_mapping[field] = field
        if field_mapping:
            logger.debug("Merging user field_mapping with default field mapping.")
            field_mapping = merge_field_mappings(
                default_field_mapping,
                field_mapping
            )
        else:
            logger.debug("Using default field mapping.")
            field_mapping = default_field_mapping
        # verify constant_field_values keys are in field_mapping values
        if constant_field_values:
            validate_constant_values_dict(
                constant_field_values,
                field_mapping.values()
            )

        # Convert the input DataFrame to Spark DataFrame
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)
        elif not isinstance(df, ps.DataFrame):
            raise TypeError(
                "Input dataframe must be a Pandas DataFrame or a PySpark DataFrame."
            )
        # Apply field mapping and constant field values
        df = df.withColumnsRenamed(field_mapping)

        if constant_field_values:
            for field, value in constant_field_values.items():
                df = df.withColumn(field, lit(value))

        if persist_dataframe:
            df = df.persist()

        if location_id_prefix:
            df = add_or_replace_sdf_column_prefix(
                sdf=df,
                column_name="location_id",
                prefix=location_id_prefix,
            )
        validated_df = self._validate(
            df=df,
            drop_duplicates=drop_duplicates,
            add_missing_columns=True
        )
        self._write_spark_df(
            validated_df,
            write_mode=write_mode
        )

        df.unpersist()
