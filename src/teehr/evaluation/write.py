"""Writer class for TEEHR evaluations."""
from typing import List
from pathlib import Path

from pyspark.sql import DataFrame
import pandas as pd
from pyarrow import schema as arrow_schema
import geopandas as gpd

from teehr.evaluation.utils import get_table_instance


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
            self.spark = ev.spark
            self.catalog_name = ev.catalog_name
            self.schema = ev.schema_name
            self.ev = ev

    def _create_or_replace(
        self,
        source_view: str,
        target_table: str,
        partition_by: List[str],
    ):
        """Upsert the DataFrame to the specified target in the catalog."""
        if partition_by is None:
            raise ValueError(
                "partition_by fields must be provided when using"
                " write_mode='create_or_replace'"
            )
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        sql_query = f"""
            CREATE OR REPLACE TABLE {self.ev.catalog_name}.{self.ev.schema_name}.{target_table}
            PARTITIONED BY ({', '.join(partition_by)})
            AS SELECT * FROM {source_view}
        """  # noqa: E501
        self.ev.spark.sql(sql_query)

    def _upsert(
        self,
        source_view: str,
        target_table: str,
        uniqueness_fields: List[str],
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
        source_fields = self.ev.spark.table(source_view).columns
        update_fields = list(
            set(source_fields)
            .symmetric_difference(set(uniqueness_fields))
        )
        update_set_sql = ", ".join(
            [f"t.{fld} = s.{fld}" for fld in update_fields]
        )
        sql_query = f"""
            MERGE INTO {self.ev.catalog_name}.{self.ev.schema_name}.{target_table} t
            USING {source_view} s
            ON {on_sql}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self.ev.spark.sql(sql_query)

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
            USING {source_view} s
            ON {on_sql}
            WHEN NOT MATCHED THEN INSERT *
        """  # noqa: E501
        self.ev.spark.sql(sql_query)

    def _overwrite(
        self,
        source_view: str,
        target_table: str,
        # uniqueness_fields: List[str],
    ):
        """Replace the target table values with matching Dataframe values."""
        # Use the <=> operator for null-safe equality comparison
        # so that two null values are considered equal.
        # on_sql = " AND ".join(
        #     [f"t.{fld} <=> s.{fld}" for fld in uniqueness_fields]
        # )
        # source_fields = self.ev.spark.table(source_view).columns
        # update_fields = list(
        #     set(source_fields)
        #     .symmetric_difference(set(uniqueness_fields))
        # )
        # update_set_sql = ", ".join(
        #     [f"t.{fld} = s.{fld}" for fld in update_fields]
        # )
        sql_query = f"""
            INSERT OVERWRITE TABLE {self.ev.catalog_name}.{self.ev.schema_name}.{target_table}
            SELECT * FROM {source_view}
        """  # noqa: E501
        self.ev.spark.sql(sql_query)

    def to_warehouse(
        self,
        source_data: DataFrame | str,
        target_table: str,
        write_mode: str = "append",
        uniqueness_fields: List[str] | None = None,
        partition_by: List[str] = None,
    ):
        """Write the DataFrame to the specified target in the catalog.

        Parameters
        ----------
        sdf : DataFrame
            The Spark DataFrame to write.
        source_data : DataFrame | str
            The Spark DataFrame or temporary view name to write.
        target_table : str
            The target table name in the catalog.
        write_mode : str, optional
            The mode to use when writing the DataFrame
            (e.g., 'append', 'overwrite'), by default "append".
        uniqueness_fields : List[str], optional
            List of fields that uniquely identify a record, by default None,
            which means the uniqueness_fields are taken from the table class.
        partition_by : List[str], optional
            List of fields to partition the table by, required if write_mode is
            'create_or_replace'.
        """
        if uniqueness_fields is None:
            table_instance = get_table_instance(
                self.ev, target_table
            )
            if table_instance is not None:
                uniqueness_fields = table_instance.uniqueness_fields

        if isinstance(source_data, DataFrame):
            source_data.createOrReplaceTempView("source_data")
            source_data = "source_data"

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
        elif write_mode == "create_or_replace":
            self._create_or_replace(
                source_view=source_data,
                target_table=target_table,
                partition_by=partition_by
            )
        # TODO: Is something like this needed?
        # elif write_mode == "overwrite":
        #     self._overwrite(
        #         source_view=source_data,
        #         target_table=target_table,
        #         # uniqueness_fields=uniqueness_fields,
        #     )
        else:
            raise ValueError(
                "write_mode must be one of 'append', 'upsert',"
                " or 'create_or_replace',."
            )

        self.ev.spark.sql("DROP VIEW IF EXISTS source_data")

    @staticmethod
    def to_cache(
        source_data: DataFrame | pd.DataFrame,
        cache_filepath: str | Path,
        write_schema: arrow_schema,
        write_mode: str = "overwrite"
    ):
        """Cache the DataFrame in memory for faster access.

        Parameters
        ----------
        source_data : DataFrame
            The Spark or Pandas DataFrame to cache.
        cache_filepath : str
            The path to use for the cached table.
        write_schema : arrow_schema
            The pyarrow schema to use when writing the parquet file.
        write_mode : str, optional
            The mode to use when caching the DataFrame
            (e.g., 'append', 'overwrite'), by default "overwrite".
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
        elif isinstance(source_data, DataFrame):
            source_data.write.mode(write_mode).parquet(cache_filepath)
        else:
            raise ValueError(
                "source_data must be a Spark or Pandas DataFrame."
            )

    def to_view(
        self,
        source_data: DataFrame | pd.DataFrame,
        view_name: str,
        temporary: bool = True,
    ):
        """Create a view from the DataFrame.

        Parameters
        ----------
        source_data : DataFrame
            The Spark or Pandas DataFrame to create a view from.
        view_name : str
            The name to use for the temporary or global view.
        temporary : bool, optional
            Whether to create a temporary (True) or a global view (False),
            by default True.
        """
        # TODO: Does this make sense to be on writer?
        if isinstance(source_data, pd.DataFrame):
            source_data = self.ev.spark.createDataFrame(source_data)
        if temporary is True:
            source_data.createOrReplaceTempView(view_name)
        else:
            source_data.createOrReplaceGlobalTempView(view_name)
