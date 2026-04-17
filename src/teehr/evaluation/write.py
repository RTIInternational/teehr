"""Writer class for TEEHR evaluations."""
import logging
import time
from typing import List, Union
from pathlib import Path

import pyspark.sql as ps
import pandas as pd
from pyarrow import Schema as ArrowSchema
import geopandas as gpd
import pyarrow as pw
import pyarrow.parquet as pq
import json

from teehr.const import REMOTE_CATALOG_NAME
from teehr.models.filters import TableFilter

logger = logging.getLogger(__name__)

DATATYPE_WRITE_TRANSFORMS = {"forecast_lead_time": "BIGINT"}
AUDIT_COLUMNS = ["created_at", "updated_at"]


class Write:
    """Class to handle writing to the warehouse."""

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

    def _apply_datatype_transform(self, view_name: str):
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

        self._ev.sql(f"""
            SELECT * {except_sql},
            {select_sql}
            FROM {view_name}
        """).createOrReplaceTempView(view_name)

    def _add_timestamps_to_view(
        self,
        source_view: str,
        add_created_at: bool = True,
        add_updated_at: bool = True
    ) -> str:
        """Create or update timestamp columns in the view.

        Parameters
        ----------
        source_view : str
            The name of the source view.
        add_created_at : bool, optional
            Whether to add created_at column, by default True.
        add_updated_at : bool, optional
            Whether to add updated_at column, by default True.

        Returns
        -------
        str
            The name of the view with timestamps added.
        """
        source_columns = self._ev.spark.table(source_view).columns
        timestamp_view = f"{source_view}_with_timestamps"

        select_parts = ["*"]
        exclude_parts = []

        if add_created_at:
            if "created_at" in source_columns:
                exclude_parts.append("created_at")
            select_parts.append("current_timestamp() AS created_at")

        if add_updated_at:
            if "updated_at" in source_columns:
                exclude_parts.append("updated_at")
            select_parts.append("current_timestamp() AS updated_at")

        select_sql = ", ".join(select_parts)
        if exclude_parts:
            exclude_sql = "EXCEPT(" + ", ".join(exclude_parts) + ")"
            select_sql = select_sql.replace("*", f"* {exclude_sql}")

        self._ev.sql(f"""
            SELECT {select_sql}
            FROM {source_view}
        """).createOrReplaceTempView(timestamp_view)

        return timestamp_view

    def _create_or_replace(
        self,
        source_view: str,
        table_name: str,
        catalog_name: str,
        namespace_name: str,
        partition_by: List[str] | None = None,
    ):
        """Drop and recreate the table with source data.

        Creates the table if it doesn't exist. Loses table history/snapshots.
        Can change schema if source data has different columns.
        Sets both created_at and updated_at to current timestamp.
        """
        timestamp_view = self._add_timestamps_to_view(source_view)
        if partition_by:
            partition_sql = ", ".join(partition_by)
            sql_query = f"""
                CREATE OR REPLACE TABLE {catalog_name}.{namespace_name}.{table_name}
                USING iceberg
                PARTITIONED BY ({partition_sql})
                AS SELECT * FROM {timestamp_view}
            """  # noqa: E501
        else:
            sql_query = f"""
                CREATE OR REPLACE TABLE {catalog_name}.{namespace_name}.{table_name}
                AS SELECT * FROM {timestamp_view}
            """  # noqa: E501
        self._ev.sql(sql_query)
        self._ev.sql(f"DROP VIEW IF EXISTS {timestamp_view}")

    def _build_on_clause(
        self,
        uniqueness_fields: List[str],
        nullable_fields: List[str]
    ) -> str:
        """Build the ON clause for MERGE statements.

        Uses regular = for non-nullable fields (better optimization)
        and <=> for nullable fields (null-safe equality).
        """
        parts = []
        for fld in uniqueness_fields:
            if fld in nullable_fields:
                # Use null-safe equality for nullable fields
                parts.append(f"t.{fld} <=> s.{fld}")
            else:
                # Use regular equality for non-nullable fields
                parts.append(f"t.{fld} = s.{fld}")
        return " AND ".join(parts)

    def _build_time_range_filter(
        self,
        source_view: str,
        uniqueness_fields: List[str],
        value_time_partition_filter: bool = True
    ) -> str:
        """Build time-range filter for partition pruning if value_time is present.

        This filter allows Iceberg to prune partitions during MERGE operations.
        Pre-computes min/max values as literals since subqueries aren't allowed
        in MERGE conditions.

        Parameters
        ----------
        source_view : str
            Name of the source view to get min/max values from.
        uniqueness_fields : List[str]
            List of uniqueness fields to check for value_time.
        value_time_partition_filter : bool, optional
            Whether to include the time-range filter. Default True.
        """
        if not value_time_partition_filter:
            return ""

        if "value_time" not in uniqueness_fields:
            return ""

        # Pre-compute min/max value_time as literals (subqueries not allowed in MERGE)
        bounds = self._ev.spark.sql(f"""
            SELECT MIN(value_time) as min_time, MAX(value_time) as max_time
            FROM {source_view}
        """).collect()[0]

        min_time = bounds["min_time"]
        max_time = bounds["max_time"]

        if min_time is None or max_time is None:
            return ""

        return f"""
            AND t.value_time >= TIMESTAMP '{min_time}'
            AND t.value_time <= TIMESTAMP '{max_time}'"""

    def _upsert(
        self,
        source_view: str,
        table_name: str,
        uniqueness_fields: List[str],
        catalog_name: str,
        namespace_name: str,
        nullable_fields: List[str] = None,
        value_time_partition_filter: bool = True
    ):
        """Upsert the DataFrame to the specified target in the catalog.

        For new rows: sets both created_at and updated_at to current timestamp.
        For existing rows: updates updated_at only, preserves created_at.
        """
        if nullable_fields is None:
            nullable_fields = []

        # Build ON clause with optimized operators
        on_sql = self._build_on_clause(uniqueness_fields, nullable_fields)

        # Add time-range filter for partition pruning
        time_filter = self._build_time_range_filter(
            source_view, uniqueness_fields, value_time_partition_filter
        )

        # Add timestamps to source view for new inserts
        timestamp_view = self._add_timestamps_to_view(source_view)

        source_fields = self._ev.spark.table(source_view).columns
        # Exclude audit columns and uniqueness fields from update fields
        non_audit_source_fields = [
            f for f in source_fields if f not in AUDIT_COLUMNS
        ]
        update_fields = list(
            set(non_audit_source_fields) - set(uniqueness_fields)
        )
        # Build update SET clause: update regular fields and updated_at only
        update_set_parts = [f"t.{fld} = s.{fld}" for fld in update_fields]
        update_set_parts.append("t.updated_at = current_timestamp()")
        update_set_sql = ", ".join(update_set_parts)

        sql_query = f"""
            MERGE INTO {catalog_name}.{namespace_name}.{table_name} t
            USING {timestamp_view} s
            ON {on_sql}{time_filter}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self._ev.sql(sql_query)
        self._ev.sql(f"DROP VIEW IF EXISTS {timestamp_view}")

    def _append(
        self,
        source_view: str,
        table_name: str,
        uniqueness_fields: List[str],
        catalog_name: str,
        namespace_name: str,
        nullable_fields: List[str] = None,
        value_time_partition_filter: bool = True
    ):
        """Append the DataFrame to the specified target in the catalog.

        Only inserts new rows (skips existing). Sets both created_at and
        updated_at to current timestamp for new rows.
        """
        if nullable_fields is None:
            nullable_fields = []

        # Build ON clause with optimized operators
        on_sql = self._build_on_clause(uniqueness_fields, nullable_fields)

        # Add time-range filter for partition pruning
        time_filter = self._build_time_range_filter(
            source_view, uniqueness_fields, value_time_partition_filter
        )

        # Add timestamps to source view for new inserts
        timestamp_view = self._add_timestamps_to_view(source_view)

        sql_query = f"""
            MERGE INTO {catalog_name}.{namespace_name}.{table_name} t
            USING {timestamp_view} s
            ON {on_sql}{time_filter}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self._ev.sql(sql_query)
        self._ev.sql(f"DROP VIEW IF EXISTS {timestamp_view}")

    def _insert(
        self,
        source_view: str,
        table_name: str,
        catalog_name: str,
        namespace_name: str
    ):
        """Insert the DataFrame into the target table without duplicate checking.

        This is faster than ``_append`` because it uses a simple
        ``INSERT INTO`` statement instead of ``MERGE INTO``.
        Duplicate rows will be inserted if they exist in the source data.
        Sets both created_at and updated_at to current timestamp.
        """
        timestamp_view = self._add_timestamps_to_view(source_view)
        sql_query = f"""
            INSERT INTO {catalog_name}.{namespace_name}.{table_name}
            SELECT * FROM {timestamp_view}
        """  # noqa: E501
        self._ev.sql(sql_query)
        self._ev.sql(f"DROP VIEW IF EXISTS {timestamp_view}")

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
        Sets both created_at and updated_at to current timestamp.
        """
        timestamp_view = self._add_timestamps_to_view(source_view)
        sql_query = f"""
            INSERT OVERWRITE TABLE {catalog_name}.{namespace_name}.{table_name}
            SELECT * FROM {timestamp_view}
        """  # noqa: E501
        self._ev.sql(sql_query)
        self._ev.sql(f"DROP VIEW IF EXISTS {timestamp_view}")

    def to_warehouse(
        self,
        source_data: pd.DataFrame | ps.DataFrame | str,
        table_name: str,
        write_mode: str = "append",
        uniqueness_fields: List[str] | None = None,
        nullable_fields: List[str] | None = None,
        partition_by: List[str] | None = None,
        catalog_name: str = None,
        namespace_name: str = None,
        value_time_partition_filter: bool = True
    ):
        """Write the DataFrame to the specified target in the catalog.

        Skips validation!

        Parameters
        ----------
        source_data : pd.DataFrame | ps.DataFrame | str
            The Spark or Pandas DataFrame or temporary view name to write.
        table_name : str
            The target table name in the catalog.
        write_mode : str, optional
            The mode to use when writing the DataFrame. Options:

            - ``"insert"``: Insert all rows directly without duplicate
              checking. Faster than ``"append"`` but may create duplicates.
            - ``"append"``: Insert new rows; skip rows matching
              uniqueness_fields (uses ``MERGE INTO``).
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
            Only used for ``"append"`` and ``"upsert"`` write modes.
        nullable_fields : List[str], optional
            List of fields that should use null-safe equality in MERGE ON
            clauses. If None, nullable fields are inferred from the target
            table schema when available.
        partition_by : List[str], optional
            Partition expressions to use when creating a custom table with
            ``write_mode="create_or_replace"``. Only supported for non-core
            tables.
        catalog_name : str, optional
            The catalog name to write to, by default None, which means the
            catalog_name of the active catalog is used.
        namespace_name : str, optional
            The namespace name to write to, by default None, which means the
            namespace_name of the active catalog is used.
        value_time_partition_filter : bool, optional
            Whether to add time-range filter for partition pruning in MERGE
            operations. When True, adds WHERE clause to limit target table
            scan to partitions matching source data's value_time range.
            Default is ``True``.
        """
        start_time = time.time()
        logger.info(f"Start writing to warehouse table '{table_name}'.")

        if (
            self._ev.read_only_remote is True and
            self._ev.active_catalog.catalog_name == REMOTE_CATALOG_NAME
        ):
            raise ValueError(
                "Cannot write to the TEEHR-Cloud warehouse in read-only remote mode."
            )

        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name

        # Get table metadata for uniqueness and nullable fields
        tbl = self._ev.table(table_name=table_name)
        default_uniqueness_fields = tbl.uniqueness_fields
        default_nullable_fields = []
        if tbl.schema_func is not None:
            schema = tbl.schema_func(type="pyspark")
            default_nullable_fields = [
                col_name for col_name, col in schema.columns.items()
                if col.nullable is True
            ]

        if tbl.is_core_table:
            if partition_by is not None:
                raise ValueError(
                    "partition_by cannot be specified for core tables. "
                    "Core table partitioning is defined by the table schema."
                )

            if (
                uniqueness_fields is not None and
                set(uniqueness_fields) != set(default_uniqueness_fields)
            ):
                raise ValueError(
                    "uniqueness_fields cannot be overridden for core tables."
                )

            if (
                nullable_fields is not None and
                set(nullable_fields) != set(default_nullable_fields)
            ):
                raise ValueError(
                    "nullable_fields cannot be overridden for core tables."
                )

        if uniqueness_fields is None:
            uniqueness_fields = default_uniqueness_fields

        # Get nullable fields from the Pandera schema
        if nullable_fields is None:
            nullable_fields = default_nullable_fields

        source_view_name = "source_view"
        created_temp_view = False

        if isinstance(source_data, pd.DataFrame):
            source_data = self._ev.spark.createDataFrame(source_data)

        if isinstance(source_data, ps.DataFrame):
            source_data.createOrReplaceTempView(source_view_name)
            created_temp_view = True

        if isinstance(source_data, str):
            source_view_name = source_data

        tbl_columns = self._ev.spark.table(source_view_name).columns
        if any(item in DATATYPE_WRITE_TRANSFORMS for item in tbl_columns):
            self._apply_datatype_transform(source_view_name)

        if write_mode == "insert":
            self._insert(
                source_view=source_view_name,
                table_name=table_name,
                catalog_name=catalog_name,
                namespace_name=namespace_name
            )
        elif write_mode == "append":
            if uniqueness_fields is None:
                raise ValueError(
                    "uniqueness_fields must be provided for append write mode."
                )
            self._append(
                source_view=source_view_name,
                table_name=table_name,
                uniqueness_fields=uniqueness_fields,
                catalog_name=catalog_name,
                namespace_name=namespace_name,
                nullable_fields=nullable_fields,
                value_time_partition_filter=value_time_partition_filter
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
                namespace_name=namespace_name,
                nullable_fields=nullable_fields,
                value_time_partition_filter=value_time_partition_filter
            )
        elif write_mode == "create_or_replace":
            self._create_or_replace(
                source_view=source_view_name,
                table_name=table_name,
                catalog_name=catalog_name,
                namespace_name=namespace_name,
                partition_by=partition_by,
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
                "write_mode must be one of 'insert', 'append', 'upsert', "
                "'overwrite', or 'create_or_replace'."
            )

        if created_temp_view:
            self._ev.sql(f"DROP VIEW IF EXISTS {source_view_name}")

        elapsed_time = time.time() - start_time
        logger.info(f"Finished writing to warehouse table '{table_name}' in {elapsed_time:.3f} seconds.")

    def delete_from(
        self,
        table_name: str,
        filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
        catalog_name: str = None,
        namespace_name: str = None,
        dry_run: bool = False,
    ) -> Union[int, ps.DataFrame]:
        """Delete rows from a table based on filter conditions.

        Parameters
        ----------
        table_name : str
            The name of the table to delete rows from.
        filters : Union[str, dict, TableFilter, List[...]], optional
            Filter conditions specifying which rows to delete.
            Supports SQL strings, dictionaries, or
            :class:`~teehr.models.filters.TableFilter` objects.
            If ``None``, all rows in the table will be deleted.
        catalog_name : str, optional
            The catalog name, by default None, which uses the active catalog.
        namespace_name : str, optional
            The namespace name, by default None, which uses the active
            namespace.
        dry_run : bool, optional
            If ``True``, returns a Spark DataFrame of rows that would be
            deleted without performing the actual deletion. This allows the
            user to inspect or count the rows before committing the delete.
            Default is ``False``.

        Returns
        -------
        int or ps.DataFrame
            If ``dry_run=False``, returns the number of rows deleted (int).
            If ``dry_run=True``, returns a Spark DataFrame of rows that
            would be deleted.

        Examples
        --------
        Preview rows that would be deleted (dry run):

        >>> sdf = ev._write.delete_from(
        >>>     table_name="primary_timeseries",
        >>>     filters=["location_id = 'usgs-01234567'"],
        >>>     dry_run=True,
        >>> )
        >>> sdf.show()
        >>> print(f"Rows to delete: {sdf.count()}")

        Delete rows and get the count:

        >>> count = ev._write.delete_from(
        >>>     table_name="primary_timeseries",
        >>>     filters=["location_id = 'usgs-01234567'"],
        >>> )
        >>> print(f"Deleted {count} rows.")

        Delete using a TableFilter object:

        >>> from teehr.models.filters import TableFilter
        >>> from teehr import Operators as ops
        >>> count = ev._write.delete_from(
        >>>     table_name="primary_timeseries",
        >>>     filters=TableFilter(
        >>>         column="location_id",
        >>>         operator=ops.eq,
        >>>         value="usgs-01234567"
        >>>     ),
        >>> )
        """
        if (
            self._ev.read_only_remote is True and
            self._ev.active_catalog.catalog_name == REMOTE_CATALOG_NAME
        ):
            raise ValueError(
                "Cannot delete from the TEEHR-Cloud warehouse in read-only remote mode."
            )

        if catalog_name is None:
            catalog_name = self._ev.active_catalog.catalog_name
        if namespace_name is None:
            namespace_name = self._ev.active_catalog.namespace_name

        full_table_name = f"{catalog_name}.{namespace_name}.{table_name}"

        # Build the WHERE clause from filters
        where_clause = None
        if filters is not None:
            sdf = self._ev.spark.table(full_table_name)
            validated_filters = self._ev._validate.sdf_filters(
                sdf=sdf,
                filters=filters,
                validate=False
            )
            where_clause = " AND ".join(validated_filters)

        # Build matching query for counting or dry run
        if where_clause:
            match_sql = f"SELECT * FROM {full_table_name} WHERE {where_clause}"
        else:
            match_sql = f"SELECT * FROM {full_table_name}"

        matching_sdf = self._ev.sql(match_sql)

        if dry_run:
            return matching_sdf

        # Count before deletion, then execute delete
        count = matching_sdf.count()

        if where_clause:
            self._ev.sql(f"DELETE FROM {full_table_name} WHERE {where_clause}")
        else:
            self._ev.sql(f"DELETE FROM {full_table_name}")

        return count

    @staticmethod
    def to_cache(
        source_data: ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame,
        cache_filepath: str | Path,
        write_schema: ArrowSchema,
        write_mode: str = "overwrite"
    ):
        """Write the DataFrame to a parquet file for caching.

        Parameters
        ----------
        source_data : ps.DataFrame | pd.DataFrame | gpd.GeoDataFrame
            The Spark, Pandas, or GeoPandas DataFrame to cache.
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
            # Convert geometry to WKB manually
            df = source_data.copy()
            crs_json = source_data.crs.to_json() if source_data.crs else None
            df['geometry'] = df.geometry.to_wkb()
            pdf = pd.DataFrame(df)

            # Now use regular pandas to_parquet with schema
            # pdf.to_parquet(cache_filepath, engine="pyarrow", schema=write_schema)

            table = pw.Table.from_pandas(pdf, schema=write_schema)

            # Add GeoParquet metadata
            geo_metadata = {
                "version": "1.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "crs": json.loads(crs_json) if crs_json else None
                    }
                }
            }
            metadata = table.schema.metadata or {}
            metadata[b"geo"] = json.dumps(geo_metadata).encode()
            table = table.replace_schema_metadata(metadata)

            pq.write_table(table, cache_filepath)

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
