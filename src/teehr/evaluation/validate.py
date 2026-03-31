"""Class for validating data."""
import logging
from typing import List, Dict, Union

import pyspark.sql as ps
import pandas as pd
import geopandas as gpd
from pandera.pyspark import DataFrameSchema as SparkDataFrameSchema
from pandera.pandas import DataFrameSchema as PandasDataFrameSchema
from pyspark.sql.functions import lit

from teehr.models.filters import TableFilter
from teehr.querying.filter_format import (
    format_filter,
    validate_filter
)

logger = logging.getLogger(__name__)


class Validate:
    """Class for validating data."""

    def __init__(self, ev=None) -> None:
        """Initialize the Validate class with an Evaluation instance.

        Parameters
        ----------
        ev : Evaluation
            An instance of the Evaluation class containing Spark session
            and catalog details. The default is None, which allows access to
            the class's static methods only.
        """
        if ev is not None:
            self._ev = ev

    @staticmethod
    def _pandera_validation(
        df: ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame,
        table_schema: SparkDataFrameSchema | PandasDataFrameSchema,
    ) -> ps.DataFrame | pd.DataFrame:
        """Validate the DataFrame against the provided schema.

        This only checks data types, fields, and nullability using
        the pandera schema, it does not enforce foreign key relationships.

        Parameters
        ----------
        df : ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The Spark, Pandas, or GeoPandas DataFrame to validate.
        table_schema : SparkDataFrameSchema | PandasDataFrameSchema
            The schema to validate against.

        Returns
        -------
        ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The validated Spark, Pandas, or GeoPandas DataFrame.

        Examples
        --------
        Validate a PySpark DataFrame against the primary timeseries schema:

        >>> from teehr.models.pandera_dataframe_schemas import primary_timeseries_schema
        >>> validated_sdf = ev._validate._pandera_validation(
        ...     df=raw_sdf,
        ...     table_schema=primary_timeseries_schema()
        ... )

        For Pandas DataFrames:

        >>> validated_pdf = ev._validate._pandera_validation(
        ...     df=raw_pdf,
        ...     table_schema=primary_timeseries_schema(type="pandas")
        ... )
        """
        logger.info("Validating DataFrame against schema.")
        if isinstance(table_schema, SparkDataFrameSchema):
            if not isinstance(df, ps.DataFrame):
                raise ValueError(
                    "df must be a Spark DataFrame if"
                    " table_schema is a Spark DataFrameSchema."
                )
        elif isinstance(table_schema, PandasDataFrameSchema):
            if not isinstance(df, pd.DataFrame | gpd.GeoDataFrame):
                raise ValueError(
                    "df must be a Pandas or GeoPandas DataFrame if"
                    " table_schema is a Pandas DataFrameSchema."
                )
        else:
            raise ValueError(
                "table_schema must be a Spark or Pandas DataFrameSchema."
            )

        validated_df = table_schema.validate(df)

        # PySpark pandera does not raise exceptions on validation failure,
        # it stores errors in .pandera.errors. Check and raise if errors exist.
        if isinstance(table_schema, SparkDataFrameSchema):
            errors = validated_df.pandera.errors
            if errors:
                error_msgs = []
                for error_type, error_dict in errors.items():
                    for check_type, error_list in error_dict.items():
                        for err in error_list:
                            error_msgs.append(
                                f"{err.get('column', 'unknown')}: "
                                f"{err.get('error', err.get('check', 'validation failed'))}"
                            )
                raise ValueError(
                    "Schema validation failed:\n" + "\n".join(error_msgs)
                )

        return validated_df

    def sdf_filters(
        self,
        sdf: ps.DataFrame,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ],
        validate: bool = True
    ) -> List[str]:
        """Validate and format filter(s) against an existing DataFrame.

        This method validates filters against the schema and columns of an
        in-memory DataFrame, allowing filtering on calculated fields that
        don't exist in the warehouse table.

        Parameters
        ----------
        sdf : ps.DataFrame
            The Spark DataFrame to validate filters against.
        filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ]
            The filters to validate.
        validate : bool, optional
            Whether to validate the filter field types against the schema.
            The default is True.

        Returns
        -------
        List[str]
            List of validated filter strings ready to apply with sdf.filter().
        """
        if not isinstance(filters, List):
            logger.debug("Filter is not a list. Making a list.")
            filters = [filters]

        table_fields = sdf.columns
        table_schema = sdf.schema
        validated_filters = []
        for filter in filters:
            logger.debug(f"Validating and applying {filter}")
            if not isinstance(filter, str):
                # Convert dict or re-validate TableFilter with context
                if isinstance(filter, TableFilter):
                    filter = TableFilter.model_validate(
                        filter.model_dump(),
                        context={"field_names": table_fields}
                    )
                else:
                    # Assume it's a dict
                    filter = TableFilter.model_validate(
                        filter,
                        context={"field_names": table_fields}
                    )
                logger.debug(f"Filter: {filter.model_dump_json()}")
                if validate is True:
                    filter = validate_filter(filter, table_schema)
                filter = format_filter(filter)

            validated_filters.append(filter)

        return validated_filters

    def table_filters(
        self,
        table_name: str,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ],
        validate: bool = True
    ) -> List[str]:
        """Validate table filter(s) by reading the table schema.

        Parameters
        ----------
        table_name : str
            The name of the table to validate filters for.
        filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ]
            The filters to validate.
        validate : bool, optional
            Whether to validate the filter field types against the table schema.
            The default is True.

        Returns
        -------
        List[str]
            List of validated filter strings ready to apply with sdf.filter().
        """
        tbl = self._ev.table(table_name=table_name)
        sdf = tbl.to_sdf()
        return self.sdf_filters(sdf, filters, validate)

    def _enforce_foreign_keys(
        self,
        sdf: ps.DataFrame,
        foreign_keys: List[Dict[str, str]]
    ):
        """Enforce foreign key relationships.

        Parameters
        ----------
        sdf : ps.DataFrame
            The Spark DataFrame to enforce foreign key relationships on.
        foreign_keys : List[Dict[str, str]]
            A list of dictionaries specifying the foreign key relationships to enforce.
            Each dictionary should have the following keys:
                - column: The name of the column in sdf that is the foreign key.
                - domain_table: The name of the domain table to check against.
                - domain_column: The name of the column in the domain table that is the primary key.

        Raises
        ------
        ValueError
            If any foreign key constraint is violated, a ValueError is raised with details about the violation.
        """
        if not isinstance(sdf, ps.DataFrame):
            raise ValueError("sdf must be a Spark DataFrame.")

        if not isinstance(foreign_keys, List):
            raise ValueError("foreign_keys must be a list of dictionaries.")

        if foreign_keys is None:
            raise ValueError("foreign_keys cannot be None.")

        if len(foreign_keys) > 0:
            logger.info(
                "Enforcing foreign key constraints."
            )
        sdf.createOrReplaceTempView("temp_table")
        for fk in foreign_keys:
            sql = f"""
                SELECT t.* from temp_table t
                LEFT ANTI JOIN {fk['domain_table']} d
                ON t.{fk['column']} = d.{fk['domain_column']}
            """
            result_sdf = self._ev.sql(sql)
            self._ev.spark.catalog.dropTempView(fk["domain_table"])
            if not result_sdf.isEmpty():
                self._ev.spark.catalog.dropTempView("temp_table")
                msg = (
                    f"Foreign key constraint violation: "
                    f"A {fk['column']} entry is not found in "
                    f"the {fk['domain_column']} column in {fk['domain_table']}"
                    f"\nFirst offending record: {result_sdf.first()}"
                )
                logger.info(msg)
                raise ValueError(msg)
        self._ev.spark.catalog.dropTempView("temp_table")

    def _add_missing_fields(
        self,
        df: pd.DataFrame | ps.DataFrame | gpd.GeoDataFrame,
        table_schema: SparkDataFrameSchema | PandasDataFrameSchema,
    ) -> pd.DataFrame | ps.DataFrame | gpd.GeoDataFrame:
        """Add missing nullable fields from the schema to the DataFrame.

        Only adds columns that are nullable in the schema. Raises an error
        if a required (non-nullable) column is missing. For GeoDataFrames,
        map/dict columns are skipped since they can't be written to parquet
        when all null (GeoDataFrame.to_parquet doesn't support arrow schema).

        Parameters
        ----------
        df : pd.DataFrame | ps.DataFrame | gpd.GeoDataFrame
            The Pandas, Spark, or GeoPandas DataFrame to add missing fields to.
        table_schema : SparkDataFrameSchema | PandasDataFrameSchema
            The schema containing column definitions with nullability info.

        Returns
        -------
        pd.DataFrame | ps.DataFrame | gpd.GeoDataFrame
            The DataFrame with missing nullable fields added.

        Raises
        ------
        ValueError
            If a required (non-nullable) column is missing from the DataFrame.
        """
        schema_cols = table_schema.columns
        missing_required = []

        for col_name, col_schema in schema_cols.items():
            if col_name not in df.columns:
                is_nullable = getattr(col_schema, 'nullable', True)
                if is_nullable:
                    if isinstance(df, ps.DataFrame):
                        df = df.withColumn(col_name, lit(None))
                    else:
                        df[col_name] = None
                else:
                    missing_required.append(col_name)

        if missing_required:
            raise ValueError(
                f"Required (non-nullable) column(s) {missing_required} "
                "are missing from the DataFrame."
            )

        return df

    def _remove_extra_fields(
        self,
        df: ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame,
        columns: List[str]
    ) -> ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame:
        """Enforce strict schema by selecting only the columns in the schema.

        Parameters
        ----------
        df : ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The Pandas, Spark, or GeoPandas DataFrame to enforce strict schema on.
        columns : List[str]
            The list of columns to select from the DataFrame.

        Returns
        -------
        ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The DataFrame with only the columns in the schema.
        """
        missing_cols = [col for col in columns if col not in df.columns]
        if len(missing_cols) > 0:
            raise ValueError(
                f"The field(s) {missing_cols} are missing from the DataFrame, "
                "and are required by the schema."
            )

        if isinstance(df, ps.DataFrame):
            return df.select(*columns)

        return df[columns]

    def _drop_duplicates(
        self,
        df: ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame,
        uniqueness_fields: List[str]
    ) -> ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame:
        """Drop duplicate rows based on the uniqueness fields.

        Parameters
        ----------
        df : ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The Pandas, Spark, or GeoPandas DataFrame to drop duplicates from.
        uniqueness_fields : List[str]
            The fields that uniquely identify a record.

        Returns
        -------
        ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The DataFrame with duplicate rows dropped.
        """
        if uniqueness_fields is None:
            raise ValueError(
                "uniqueness_fields must be provided"
                " if drop_duplicates is True."
            )
        if isinstance(df, ps.DataFrame):
            return df.drop_duplicates(subset=uniqueness_fields)

        return df.drop_duplicates(subset=uniqueness_fields)

    def dataframe(
        self,
        df: ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame,
        table_schema: SparkDataFrameSchema | PandasDataFrameSchema,
        strict: bool = True,
        add_missing_columns: bool = True,
        drop_duplicates: bool = True,
        uniqueness_fields: List[str] = None,
        foreign_keys: List[Dict[str, str]] = None,
    ) -> ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame:
        """Validate the DataFrame against the table schema.

        This checks data types, fields, and nullability using
        the pandera schema, while also enforcing foreign key relationships,
        optionally dropping duplicates, and optionally adding or removing
        columns to match the table schema.

        Parameters
        ----------
        df : ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The Spark, Pandas, or GeoPandas DataFrame to enforce the schema on.
        table_schema : SparkDataFrameSchema | PandasDataFrameSchema
            The schema to enforce.
        foreign_keys : List[Dict[str, str]], optional
            The foreign key relationships to enforce.
        strict : bool, optional
            Whether to strictly enforce the schema by including only the
            columns in the schema. The default is True.
        add_missing_columns : bool, optional
            Whether to add missing columns from the schema with null values.
            The default is True.
        drop_duplicates : bool, optional
            Whether to drop duplicate rows based on the uniqueness_fields.
            The default is True.
        uniqueness_fields : List[str], optional
            The fields that uniquely identify a record. Required if
            drop_duplicates is True. The default is None.

        Returns
        -------
        ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The Spark, Pandas, or GeoPandas DataFrame with the enforced schema.
        """
        logger.info("Enforcing warehouse schema.")

        schema_cols = table_schema.columns.keys()

        # Add missing nullable columns (raises if required columns are missing)
        if add_missing_columns:
            df = self._add_missing_fields(df, table_schema)

        if strict:
            df = self._remove_extra_fields(df, schema_cols)

        if drop_duplicates:
            df = self._drop_duplicates(df, uniqueness_fields)

        df = self._pandera_validation(df, table_schema)

        if foreign_keys is not None:
            self._enforce_foreign_keys(
                sdf=df,
                foreign_keys=foreign_keys
            )

        return df
