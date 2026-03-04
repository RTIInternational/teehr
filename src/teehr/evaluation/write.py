"""Writer class for TEEHR evaluations."""
from typing import List
from pathlib import Path

import pyspark.sql as ps
import pandas as pd
from pyarrow import Schema as ArrowSchema
import geopandas as gpd


DATATYPE_WRITE_TRANSFORMS = {"forecast_lead_time": "BIGINT"}


# TODO: Should the Writer class contain DELETE FROM? That's how it's
# organized in the docs: https://iceberg.apache.org/docs/1.9.1/spark-writes/#delete-from

class Write:
    """Class to handle writing evaluation results to storage."""

    def __init__(self, ev=None):
        """Initialize the Writer with an Evaluation instance.

        Parameters
        ----------
        ev : Evaluation
            An instance of the Evaluation class containing Spark session
            and catalog details. The default is None, which allows access to
            the classes static methods only.
        """
        if ev is not None:
            self._ev = ev

    def _apply_datatype_transform(self, view_name: str) -> str:
        """Cast fields in the DataFrame to the pre-defined types."""
        all_columns = self._ev.spark.table(view_name).columns
        select_clauses = []
        except_clauses = []
        for col in all_columns:
            if col in DATATYPE_WRITE_TRANSFORMS:
                except_clauses.append(f"EXCEPT({col})")
                select_clauses.append(f"CAST({col} AS {DATATYPE_WRITE_TRANSFORMS[col]}) AS {col}")
        select_sql = ", ".join(select_clauses)
        except_sql = ", ".join(except_clauses)

        self._ev.spark.sql(f"""
            SELECT * {except_sql},
            {select_sql}
            FROM {view_name}
        """).createOrReplaceTempView(view_name)

    def _create_or_replace(
        self,
        source_view: str,
        table_name: str,
        catalog_name: str,
        namespace_name: str,
    ):
        """Drop and recreate the table with source data.

        Creates the table if it doesn't exist. Loses table history/snapshots.
        Can change schema if source data has different columns.
        """
        sql_query = f"""
            CREATE OR REPLACE TABLE {catalog_name}.{namespace_name}.{table_name}
            AS SELECT * FROM {source_view}
        """  # noqa: E501
        self._ev.spark.sql(sql_query)

    def _upsert(
        self,
        source_view: str,
        table_name: str,
        uniqueness_fields: List[str],
        catalog_name: str,
        namespace_name: str
    ):
        """Upsert the DataFrame to the specified target in the catalog."""
        # TODO: Does this do what we want it to do? Should there be a
        # SELECT first?
        # Or should it be WHEN MATCHED THEN UPDATE *?
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        on_sql = " AND ".join(
            [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        )
        source_fields = self._ev.spark.table(source_view).columns
        update_fields = list(
            set(source_fields)
            .symmetric_difference(set(uniqueness_fields))
        )
        update_set_sql = ", ".join(
            [f"t.{fld} = s.{fld}" for fld in update_fields]
        )
        sql_query = f"""
            MERGE INTO {catalog_name}.{namespace_name}.{table_name} t
            USING {source_view} s
            ON {on_sql}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self._ev.spark.sql(sql_query)

    def _append(
        self,
        source_view: str,
        table_name: str,
        uniqueness_fields: List[str],
        catalog_name: str,
        namespace_name: str
    ):
        """Append the DataFrame to the specified target in the catalog."""
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        on_sql = " AND ".join(
            [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        )
        sql_query = f"""
            MERGE INTO {catalog_name}.{namespace_name}.{table_name} t
            USING {source_view} s
            ON {on_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self._ev.spark.sql(sql_query)

    def _overwrite(
        self,
        source_view: str,
        table_name: str,
        catalog_name: str,
        namespace_name: str
    ):
        """Replace all data in the table while preserving table structure.

        Table must already exist. Preserves table history - creates new
        snapshot so you can time-travel back to previous data.
        """
        sql_query = f"""
            INSERT OVERWRITE TABLE {catalog_name}.{namespace_name}.{table_name}
            SELECT * FROM {source_view}
        """  # noqa: E501
        self._ev.spark.sql(sql_query)

    def to_warehouse(
        self,
        source_data: pd.DataFrame | ps.DataFrame | str,
        table_name: str,
        write_mode: str = "append",
        uniqueness_fields: List[str] | None = None,
        catalog_name: str = None,
        namespace_name: str = None
    ):
        """Write the DataFrame to the specified target in the catalog.

        Parameters
        ----------
        source_data : pd.DataFrame | ps.DataFrame | str
            The Spark or Pandas DataFrame or temporary view name to write.
        table_name : str
            The target table name in the catalog.
        write_mode : str, optional
            The mode to use when writing the DataFrame. Options:

            - ``"append"``: Insert new rows; skip rows matching
              uniqueness_fields.
            - ``"upsert"``: Insert new rows; update existing rows matching
              uniqueness_fields.
            - ``"overwrite"``: Replace all data in table. Preserves table
              structure and history (can time-travel back).
            - ``"create_or_replace"``: Drop and recreate table. Loses history.
              Can change schema.

            Default is ``"append"``.
        uniqueness_fields : List[str], optional
            List of fields that uniquely identify a record, by default None,
            which means the uniqueness_fields are taken from the table class.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        """
        if (
            self._ev.read_only_remote is True and
            self._ev.active_catalog.catalog_name ==
            self._ev.remote_catalog.catalog_name
        ):
            raise ValueError(
                "Cannot write to the TEEHR-Cloud warehouse in read-only remote mode."
            )

        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name

        if uniqueness_fields is None:
            tbl = self._ev.table(table_name=table_name)
            uniqueness_fields = tbl.uniqueness_fields

        source_view_name = "source_view"

        if isinstance(source_data, pd.DataFrame):
            source_data = self._ev.spark.createDataFrame(source_data)

        if isinstance(source_data, ps.DataFrame):
            source_data.createOrReplaceTempView(source_view_name)

        if isinstance(source_data, str):
            source_view_name = source_data

        tbl_columns = self._ev.spark.table(source_view_name).columns
        if any(item in DATATYPE_WRITE_TRANSFORMS for item in tbl_columns):
            self._apply_datatype_transform(source_view_name)

        if write_mode == "append":
            if uniqueness_fields is None:
                raise ValueError(
                    "uniqueness_fields must be provided for append write mode."
                )
            self._append(
                source_view=source_view_name,
                table_name=table_name,
                uniqueness_fields=uniqueness_fields,
                catalog_name=catalog_name,
                namespace_name=namespace_name
            )
        elif write_mode == "upsert":
            if uniqueness_fields is None:
                raise ValueError(
                    "uniqueness_fields must be provided for upsert write mode."
                )
            self._upsert(
                source_view=source_view_name,
                table_name=table_name,
                uniqueness_fields=uniqueness_fields,
                catalog_name=catalog_name,
                namespace_name=namespace_name
            )
        elif write_mode == "create_or_replace":
            self._create_or_replace(
                source_view=source_view_name,
                table_name=table_name,
                catalog_name=catalog_name,
                namespace_name=namespace_name
            )
        elif write_mode == "overwrite":
            self._overwrite(
                source_view=source_view_name,
                table_name=table_name,
                catalog_name=catalog_name,
                namespace_name=namespace_name
            )
        else:
            raise ValueError(
                "write_mode must be one of 'append', 'upsert', "
                "'overwrite', or 'create_or_replace'."
            )

        self._ev.spark.sql(f"DROP VIEW IF EXISTS {source_view_name}")

    @staticmethod
    def to_cache(
        source_data: ps.DataFrame | pd.DataFrame,
        cache_filepath: str | Path,
        write_schema: ArrowSchema,
        write_mode: str = "overwrite"
    ):
        """Write the DataFrame to a parquet file for caching.

        Parameters
        ----------
        source_data : ps.DataFrame | pd.DataFrame
            The Spark or Pandas DataFrame to cache.
        cache_filepath : str
            The path to use for the cached table.
        write_schema : ArrowSchema
            The pyarrow schema to use when writing the parquet file.
        write_mode : str, optional
            The mode to use when a PySpark DataFrame is written to the cache
            using PySpark's DataFrame.write.mode. Default is "overwrite".
        """
        # Allow additional kwargs for to_parquet?
        if isinstance(source_data, gpd.GeoDataFrame):
            # Not sure why the arrow schema doesn't work with geopandas.
            source_data.to_parquet(cache_filepath)
        elif isinstance(source_data, pd.DataFrame):
            source_data.to_parquet(
                cache_filepath,
                engine="pyarrow",
                schema=write_schema
            )
        elif isinstance(source_data, ps.DataFrame):
            source_data.write.mode(write_mode).parquet(cache_filepath)
        else:
            raise ValueError(
                "source_data must be a Spark or Pandas DataFrame."
            )
