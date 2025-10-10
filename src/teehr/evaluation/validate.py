"""Class for validating data."""
import logging
from typing import List, Dict, Union

import pyspark.sql as ps
from pandera.pyspark import DataFrameSchema as SparkDataFrameSchema
from pandera.pandas import DataFrameSchema as PandasDataFrameSchema
from pyspark.sql.functions import lit
import pandas as pd

from teehr.models.filters import FilterBaseModel
from teehr.querying.filter_format import (
    format_filter,
    validate_filter
)

logger = logging.getLogger(__name__)


class Validator:
    """Class for validating data."""

    def __init__(self, ev=None) -> None:
        """Initialize the Validator class."""
        if ev is not None:
            self._ev = ev

    def _enforce_foreign_keys(
        self,
        sdf: ps.DataFrame,
        foreign_keys: List[Dict[str, str]]
    ):
        """Enforce foreign keys relationships on the timeseries tables."""
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
            result_sdf = self._ev.sql(
                query=sql, create_temp_views=[fk["domain_table"]]
            )
            self._ev.spark.catalog.dropTempView(fk["domain_table"])
            if not result_sdf.isEmpty():
                self._ev.spark.catalog.dropTempView("temp_table")
                raise ValueError(
                    f"Foreign key constraint violation: "
                    f"A {fk['column']} entry is not found in "
                    f"the {fk['domain_column']} column in {fk['domain_table']}"
                )
        self._ev.spark.catalog.dropTempView("temp_table")

    @staticmethod
    def data(
        df: ps.DataFrame | pd.DataFrame,
        table_schema: SparkDataFrameSchema | PandasDataFrameSchema,
    ) -> ps.DataFrame | pd.DataFrame:
        """Validate the DataFrame against the provided schema.

        This only checks data types, fields, and nullability using
        the pandera schema.

        Parameters
        ----------
        sdf : ps.DataFrame
            The Spark DataFrame to validate.
        schema : SparkDataFrameSchema
            The schema to validate against.

        Returns
        -------
        ps.DataFrame
            The validated Spark DataFrame.
        """
        logger.info("Validating DataFrame against schema.")
        if isinstance(table_schema, SparkDataFrameSchema):
            if not isinstance(df, ps.DataFrame):
                raise ValueError(
                    "df must be a Spark DataFrame if"
                    " schema is a Spark DataFrameSchema."
                )
        elif isinstance(table_schema, PandasDataFrameSchema):
            if not isinstance(df, pd.DataFrame):
                raise ValueError(
                    "df must be a Pandas DataFrame."
                    " if schema is a Pandas DataFrameSchema."
                )
        else:
            raise ValueError(
                "schema must be a Spark or Pandas DataFrameSchema."
            )
        return table_schema.validate(df)

    def table_filters(
        self,
        table_name: str,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ],
        validate: bool = True
    ) -> Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
    ]:
        """Get the list of filters applied to the DataFrame."""
        if isinstance(filters, str):
            logger.debug(f"Filter {filters} is already string, returning as is")
            # return filters

        if not isinstance(filters, List):
            logger.debug("Filter is not a list.  Making a list.")
            filters = [filters]

        tbl = self._ev.table(table_name=table_name)
        filter_model = tbl.filter_model
        # To handle joined_timeseries fields. Hmmm should all properties
        # be handled this way? They could still be class properties.
        fields_enum = self._ev.table(table_name=table_name).field_enum()
        validated_filters = []
        for filter in filters:
            logger.debug(f"Validating and applying {filter}")

            if not isinstance(filter, str):
                filter = filter_model.model_validate(
                    filter,
                    context={"fields_enum": fields_enum}
                )
                logger.debug(f"Filter: {filter.model_dump_json()}")
                if validate is True:
                    filter = validate_filter(filter, tbl.schema_func("pandas"))
                filter = format_filter(filter)

            validated_filters.append(filter)

        return validated_filters

    def schema(
        self,
        sdf: ps.DataFrame,
        table_schema: SparkDataFrameSchema,
        foreign_keys: List[Dict[str, str]],
        strict: bool = True,
        add_missing_columns: bool = False,
        drop_duplicates: bool = True,
        uniqueness_fields: List[str] = None,
    ) -> ps.DataFrame:
        """Validate the DataFrame against the warehouse schema.

        This checks data types, fields, and nullability using
        the pandera schema, while also enforcing foreign key relationships,
        optionally dropping duplicates, and optionally adding or removing
        columns to match the warehouse schema.

        Parameters
        ----------
        sdf : ps.DataFrame
            The Spark DataFrame to enforce the schema on.

        Returns
        -------
        ps.DataFrame
            The Spark DataFrame with the enforced schema.
        """
        logger.info("Enforcing warehouse schema.")

        schema_cols = table_schema.columns.keys()

        # Add missing columns
        if add_missing_columns:
            for col_name in schema_cols:
                if col_name not in sdf.columns:
                    sdf = sdf.withColumn(col_name, lit(None))

        if strict:
            sdf = sdf.select(*schema_cols)

        if drop_duplicates:
            if uniqueness_fields is None:
                raise ValueError(
                    "uniqueness_fields must be provided"
                    " if drop_duplicates is True."
                )
            sdf = sdf.dropDuplicates(subset=uniqueness_fields)

        validated_df = table_schema.validate(sdf)

        if len(validated_df.pandera.errors) > 0:
            logger.error(f"Validation failed: {validated_df.pandera.errors}")
            raise ValueError(
                f"Validation failed: {validated_df.pandera.errors}"
            )

        self._enforce_foreign_keys(
            sdf=validated_df,
            foreign_keys=foreign_keys
        )

        return validated_df

    # NOTE: Should these just update self.sdf and return self for chaining?
