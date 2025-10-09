"""Module for iceberg schema evolution."""
import os
import time
from typing import Union
from pathlib import Path
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType


logger = logging.getLogger(__name__)

# See: https://medium.com/@tglawless/apache-iceberg-schema-evolution-automation-with-pyspark-78c6ac65ccea


def read_available_schema_versions(
    catalog_dir_path: Union[str, Path],
    catalog_name: str,
    namespace: str
) -> list[int]:
    """
    Read available schema versions from the specified catalog.

    Parameters:
      catalog_name (str): The name of the catalog.

    Returns:
      A list of integers representing the available schema versions.
    """
    schema_versions: list[int] = [
        int(d) for d in os.listdir(f'{catalog_dir_path}/migrations/{catalog_name}/{namespace}')
    ]
    return sorted(schema_versions)


def create_schema_evolution_support(
    spark: SparkSession,
    catalog_name: str,
    # namespace: str
):
    """
    Create the necessary schema evolution metadata tables in the specified catalog.

    Parameters:
      catalog_name (str): Name of the catalog where schema evolution metadata will be stored

    Returns:
      None
    """
    # Note. "databaseExists()" refers to "namespace". So does "CREATE SCHEMA".
    if not spark.catalog.databaseExists(f'{catalog_name}.schema_evolution'):
        spark.sql(f"""
          CREATE SCHEMA IF NOT EXISTS {catalog_name}.schema_evolution;
        """)
        logger.info(f"✅ Created schema: {catalog_name}.schema_evolution")

    if not spark.catalog.tableExists(f'{catalog_name}.schema_evolution.schema_version_history'):
        spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {catalog_name}.schema_evolution.schema_version_history (
            namespace STRING,
            version INT,
            applied_on BIGINT
          ) USING iceberg
        """)
        logger.info(f"✅ Created table: {catalog_name}.schema_evolution.schema_version_history")


def fetch_applied_catalog_schema_version(
    spark: SparkSession,
    catalog_name: str,
    namespace: str
) -> int:
    """
    Fetch the schema version currently applied to the specified catalog.

    Parameters:
        catalog_name (str): The name of the catalog to check.

    Returns:
        int: The schema version currently applied to the catalog.
    """
    applied_schema_version: int = 0

    create_schema_evolution_support(
      spark=spark,
      catalog_name=catalog_name,
      # namespace=namespace
    )

    # NOTE. This will fail without updating existing schema_version_history
    # tables to include namespace field.
    latest_applied_schema_version_df = spark.read \
        .format('iceberg') \
        .table(f'{catalog_name}.schema_evolution.schema_version_history') \
        .orderBy('version', ascending=False) \
        .limit(1) \
        .filter(f"namespace = '{namespace}'")

    if latest_applied_schema_version_df.count() > 0:
        applied_schema_version = latest_applied_schema_version_df.collect()[0]['version']

    return applied_schema_version


def update_applied_schema_version(
    spark: SparkSession,
    catalog_name: str,
    applied_schema_version: int,
    namespace: str
):
    """
    Update the applied schema version for a given catalog.

    Parameters:
      catalog_name (str): The name of the catalog.
      applied_schema_version (int): The new schema version applied.

    Returns:
      None
    """
    current_time_ms = time.time_ns() // 1000000
    spark.sql(f"""
        INSERT INTO {catalog_name}.schema_evolution.schema_version_history
        VALUES ('{namespace}', {applied_schema_version}, {current_time_ms})
    """)


def determine_schema_version_delta(
    available_schema_versions: list[int],
    applied_schema_version: int
) -> list[int]:
    """
    Determine the schema version delta between the latest available schema version and the currently applied schema version.

    Parameters:
      available_schema_versions (list): A list of integers representing the available schema versions.
      applied_schema_version (int): An integer representing the currently applied schema version.

    Returns:
      list: A list of integers representing the schema version delta.
    """
    schema_version_delta: list[int] = list()

    # New instance of the catalog and all schema versions must be applied.
    if applied_schema_version == 0:
        return available_schema_versions

    for i in range(len(available_schema_versions)):
        if available_schema_versions[i] == applied_schema_version:
            schema_version_delta = available_schema_versions[i + 1::]
            break

    return schema_version_delta


def load_schema_version_evolution_statements(
    migrations_dir_path: Union[str, Path],
    catalog_name: str,
    schema_version: int,
    namespace: str
) -> list[str]:
    """
    Load the SQL statements required to evolve the schema of a catalog to a specific version.

    Parameters:
      catalog_name (str): The name of the catalog.
      schema_version (int): The version number of the schema.

    Returns:
      list[str]: A list of SQL statements to execute to evolve the schema to the specified version.
    """
    schema_version_statements = []
    version_dir_name = f'{migrations_dir_path}/migrations/{catalog_name}/{namespace}/{schema_version:04}'

    schema_file_names = os.listdir(version_dir_name)
    for f in schema_file_names:
        with open(f'{version_dir_name}/{f}', 'r') as sql_file:
            # Split on empty lines to separate multiple
            # statements contained in the same file.
            schema_version_statements.extend(sql_file.read().split('\n\n'))

    schema_version_statements = [stmt.strip() for stmt in schema_version_statements]

    return schema_version_statements


def apply_schema_version_evolution_statements(
    spark: SparkSession,
    catalog_name: str,
    schema_version: int,
    namespace: str,
    evolution_statements: list[str]
):
    """
    Apply a set of schema evolution statements to a given schema in a specified catalog.

    Parameters:
      catalog_name (str): The name of the catalog
      schema_version (int): Version number of the schema to be applied
      evolution_statements (list[str]): List of SQL statements to execute for schema evolution

    Returns:
      None
    """
    # NOTE: Here in spark, "schema" = "namespace" = "database"

    logger.info(f"Applying schema version {schema_version} to {catalog_name}.{namespace}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{namespace};")
    spark.sql(f"USE {catalog_name};")
    spark.sql(f"USE SCHEMA {namespace};")

    for stmt in evolution_statements:
        spark.sql(stmt)

    update_applied_schema_version(
      spark=spark,
      catalog_name=catalog_name,
      applied_schema_version=schema_version,
      namespace=namespace
    )


def evolve_catalog_schema(
    spark: SparkSession,
    migrations_dir_path: Union[str, Path],
    catalog_name: str,
    namespace: str
):
    """
    Evolve a catalog schema by applying any new schema versions.

    Parameters
    ----------
    spark : SparkSession
        The Spark session to use for executing schema evolution statements.
    catalog_dir_path : Union[str, Path]
        The directory path where the catalog schema versions are stored.
    catalog_name : str
        The name of the catalog to evolve.
    namespace : str
        The name of the schema within the catalog to evolve.
    """
    # Shouldn't schema evolution also consider namespace?
    available_schema_versions = read_available_schema_versions(
      catalog_dir_path=migrations_dir_path,
      catalog_name=catalog_name,
      namespace=namespace
    )
    applied_schema_version = fetch_applied_catalog_schema_version(
      spark=spark,
      catalog_name=catalog_name,
      namespace=namespace
    )
    # applied_schema_version = 0
    schema_version_delta = determine_schema_version_delta(
      available_schema_versions=available_schema_versions,
      applied_schema_version=applied_schema_version
    )

    if len(schema_version_delta) == 0:
        logger.info(
          f"No new schema versions to apply to {catalog_name}.{namespace}."
        )
        return

    # schema_version_delta = [1]  # HACK

    for schema_version in schema_version_delta:
        evolution_statements = load_schema_version_evolution_statements(
          migrations_dir_path=migrations_dir_path,
          catalog_name=catalog_name,
          schema_version=schema_version,
          namespace=namespace
        )
        apply_schema_version_evolution_statements(
          spark=spark,
          catalog_name=catalog_name,
          schema_version=schema_version,
          namespace=namespace,
          evolution_statements=evolution_statements
        )
