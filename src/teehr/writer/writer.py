"""Writer class for TEEHR evaluations."""
from typing import List

from pyspark.sql import DataFrame

from teehr.evaluation.evaluation import Evaluation


# TODO: Should the Writer class contain DELETE FROM? That's how it's
# organized in the docs: https://iceberg.apache.org/docs/1.9.1/spark-writes/#delete-from

class Writer:
    """Class to handle writing evaluation results to storage."""

    def __init__(self, ev: Evaluation):
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
        sdf: DataFrame,
        target_table: str,
        uniqueness_fields: List[str],
    ):
        """Upsert the DataFrame to the specified target in the catalog."""
        sdf.createOrReplaceTempView("source_updates")
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        on_sql = " AND ".join(
            [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        )
        update_fields = list(
            set(sdf.fields())
            .symmetric_difference(set(uniqueness_fields))
        )
        update_set_sql = ", ".join(
            [f"t.{fld} = s.{fld}" for fld in update_fields]
        )
        sql_query = f"""
            MERGE INTO {self.ev.catalog_name}.{self.ev.schema_name}.{target_table} t
            USING source_updates s
            ON {on_sql}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self.ev.spark.sql(sql_query)
        self.ev.spark.catalog.dropTempView("source_updates")

    def _append(
        self,
        sdf: DataFrame,
        target_table: str,
        uniqueness_fields: List[str],
    ):
        """Append the DataFrame to the specified target in the catalog."""
        sdf.createOrReplaceTempView("source_updates")
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        on_sql = " AND ".join(
            [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        )
        sql_query = f"""
            MERGE INTO {self.ev.catalog_name}.{self.ev.schema_name}.{target_table} t
            USING source_updates s
            ON {on_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self.ev.spark.sql(sql_query)
        self.ev.spark.catalog.dropTempView("source_updates")

    def write_to_warehouse(
        self,
        sdf: DataFrame,
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
            List of fields that uniquely identify a record, by default None.
        """
        if write_mode == "append":
            self._append(
                sdf=sdf,
                target_table=target_table,
                uniqueness_fields=uniqueness_fields
            )
        elif write_mode == "upsert":
            self._upsert(
                sdf=sdf,
                target_table=target_table,
                uniqueness_fields=uniqueness_fields,
            )