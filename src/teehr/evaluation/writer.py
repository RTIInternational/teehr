"""Writer class for TEEHR evaluations."""
from typing import List

from pyspark.sql import DataFrame

from teehr.evaluation.utils import get_table_instance


# TODO: Should the Writer class contain DELETE FROM? That's how it's
# organized in the docs: https://iceberg.apache.org/docs/1.9.1/spark-writes/#delete-from

class Writer:
    """Class to handle writing evaluation results to storage."""

    def __init__(self, ev):
        """Initialize the Writer with an Evaluation instance.

        Parameters
        ----------
        ev : Evaluation
            An instance of the Evaluation class containing Spark session
            and catalog details.
        """
        self.spark = ev.spark
        self.catalog_name = ev.catalog_name
        self.schema = ev.schema_name
        self.ev = ev

    def _upsert(
        self,
        source_view: str,
        source_fields: List[str],
        target_table: str,
        uniqueness_fields: List[str],
    ):
        """Upsert the DataFrame to the specified target in the catalog."""
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        on_sql = " AND ".join(
            [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        )
        # TODO: Get source_fields from the source_view?
        update_fields = list(
            set(source_fields)
            .symmetric_difference(set(uniqueness_fields))
        )
        update_set_sql = ", ".join(
            [f"t.{fld} = s.{fld}" for fld in update_fields]
        )
        sql_query = f"""
            MERGE INTO {self.ev.catalog_name}.{self.ev.schema_name}.{target_table} t
            USING source_data s
            ON {on_sql}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self.ev.spark.sql(sql_query)
        self.ev.spark.catalog.dropTempView(f"{source_view}")

    def _append(
        self,
        source_view: str,
        target_table: str,
        uniqueness_fields: List[str],
    ):
        """Append the DataFrame to the specified target in the catalog."""
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        on_sql = " AND ".join(
            [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        )
        sql_query = f"""
            MERGE INTO {self.ev.catalog_name}.{self.ev.schema_name}.{target_table} t
            USING source_data s
            ON {on_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self.ev.spark.sql(sql_query)
        self.ev.spark.catalog.dropTempView(f"{source_view}")

    def to_warehouse(
        self,
        source_data: DataFrame | str,
        target_table: str,
        write_mode: str = "append",
        uniqueness_fields: List[str] | None = None,
    ):
        """Write the DataFrame to the specified target in the catalog.

        Parameters
        ----------
        sdf : DataFrame
            The Spark DataFrame to write.
        target_table : str
            The target table name in the catalog.
        write_mode : str, optional
            The mode to use when writing the DataFrame
            (e.g., 'append', 'overwrite'), by default "append".
        uniqueness_fields : List[str], optional
            List of fields that uniquely identify a record, by default None,
            which means the uniqueness_fields are taken from the table class.
        """
        if uniqueness_fields is None:
            uniqueness_fields = get_table_instance(
                self.ev, target_table
            ).uniqueness_fields

        if isinstance(source_data, DataFrame):
            source_data.createOrReplaceTempView("source_data")

        if write_mode == "append":
            self._append(
                source_view=source_data,
                target_table=target_table,
                uniqueness_fields=uniqueness_fields
            )
        elif write_mode == "upsert":
            self._upsert(
                source_view=source_data,
                target_table=target_table,
                uniqueness_fields=uniqueness_fields,
            )

    def to_cache(
        self,
        source_data: DataFrame,
        cache_name: str,
        write_mode: str = "overwrite"
    ):
        """Cache the DataFrame in memory for faster access.

        Parameters
        ----------
        source_data : DataFrame
            The Spark DataFrame to cache.
        cache_name : str
            The name to use for the cached table.
        write_mode : str, optional
            The mode to use when caching the DataFrame
            (e.g., 'append', 'overwrite'), by default "overwrite".
        """
        # TODO: Implement
        pass
        # if write_mode == "overwrite":
        #     self.spark.catalog.dropTempView(cache_name)
        # source_data.createOrReplaceTempView(cache_name)