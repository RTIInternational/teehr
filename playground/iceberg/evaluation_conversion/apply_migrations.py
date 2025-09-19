"""Module for iceberg schema evolution."""
import os
import time
# import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pathlib import Path

LOCAL_CATALOG_DIR = Path(
  Path.cwd(),
  "playground",
  "iceberg",
  "evaluation_conversion"
)

# See: https://medium.com/@tglawless/apache-iceberg-schema-evolution-automation-with-pyspark-78c6ac65ccea

# # Initialize Spark session with Iceberg configurations
# def get_spark_session(
#     catalog_name: str,
#     warehouse_path: str = "spark-warehouse"
# ) -> SparkSession:
#   """
#   Initialize a Spark session with Iceberg configurations.

#   Parameters:
#     catalog_name (str): The name of the catalog.

#   Returns:
#     SparkSession: A configured Spark session.
#   """
#   spark = (
#     SparkSession.builder
#     .appName("IcebergLocalDevelopment")
#     .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0')
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
#     .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
#     .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}/{catalog_name}")
#     .config("spark.driver.host", "localhost")
#     .getOrCreate()
#   )

#   return spark


def read_available_schema_versions(
    catalog_name: str
) -> list[int]:
    """
    Read available schema versions from the specified catalog.

    Parameters:
      catalog_name (str): The name of the catalog.

    Returns:
      A list of integers representing the available schema versions.
    """
    schema_versions: list[int] = [
        int(d) for d in os.listdir(f'{LOCAL_CATALOG_DIR}/{catalog_name}/migrations')
    ]
    return sorted(schema_versions)


def create_schema_evolution_support(
    spark: SparkSession,
    catalog_name: str
):
    """
    Create the necessary schema evolution metadata tables in the specified catalog.

    Parameters:
      catalog_name (str): Name of the catalog where schema evolution metadata will be stored

    Returns:
      None
    """
    if not spark.catalog.databaseExists(f'{catalog_name}.schema_evolution'):
        spark.sql(f"""
          CREATE SCHEMA IF NOT EXISTS {catalog_name}.schema_evolution;
        """)

    if not spark.catalog.tableExists(f'{catalog_name}.schema_evolution.schema_version_history'):
        spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {catalog_name}.schema_evolution.schema_version_history (
            version INT,
            applied_on BIGINT
          )
        """)


def fetch_applied_catalog_schema_version(
    spark: SparkSession,
    catalog_name: str
) -> int:
    """
    Fetch the schema version currently applied to the specified catalog.

    Parameters:
        catalog_name (str): The name of the catalog to check.

    Returns:
        int: The schema version currently applied to the catalog.
    """
    applied_schema_version: int = 0

    create_schema_evolution_support(spark, catalog_name)

    latest_applied_schema_version_df = spark.read \
        .format('iceberg') \
        .table(f'{catalog_name}.schema_evolution.schema_version_history') \
        .orderBy('version', ascending=False) \
        .limit(1)

    if latest_applied_schema_version_df.count() > 0:
        applied_schema_version = latest_applied_schema_version_df.collect()[0]['version']

    return applied_schema_version


def update_applied_schema_version(
    spark: SparkSession,
    catalog_name: str,
    applied_schema_version: int
):
    """
    Update the applied schema version for a given catalog.

    Parameters:
      catalog_name (str): The name of the catalog.
      applied_schema_version (int): The new schema version applied.

    Returns:
      None
    """
    schema_version_df = spark.createDataFrame(
      data=[
        (applied_schema_version, time.time_ns() // 1000000)
      ],
      schema=['version', 'applied_on']
    )

    schema_version_df \
        .writeTo(f'{catalog_name}.schema_evolution.schema_version_history') \
        .using("iceberg") \
        .append()


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
    catalog_name: str,
    schema_version: int
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
    version_dir_name = f'{LOCAL_CATALOG_DIR}/{catalog_name}/migrations/{schema_version:04}'

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
    schema_name: str,
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
    print(f"Applying schema version {schema_version} to {catalog_name}.{schema_name}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")
    spark.sql(f"USE {catalog_name};")
    spark.sql(f"USE SCHEMA {schema_name};")

    for stmt in evolution_statements:
        spark.sql(stmt)

    update_applied_schema_version(spark, catalog_name, schema_version)


def evolve_catalog_schema(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str
):
    """
    Evolve a catalog schema by applying any new schema versions.

    Parameters:
      catalog_name (str): The name of the catalog to evolve.

    Returns:
      None
    """
    available_schema_versions = read_available_schema_versions(catalog_name)
    applied_schema_version = fetch_applied_catalog_schema_version(spark, catalog_name)
    schema_version_delta = determine_schema_version_delta(available_schema_versions, applied_schema_version)

    for schema_version in schema_version_delta:
        evolution_statements = load_schema_version_evolution_statements(catalog_name, schema_version)
        apply_schema_version_evolution_statements(spark, catalog_name, schema_version, schema_name, evolution_statements)


# if __name__ == "__main__":
#     # Example usage
#     catalog_name = 'local'
#     schema_name = 'db'
#     warehouse_path = str(Path.home() / "temp" / "iceberg" / "evaluation" / "spark-warehouse")

#     spark = get_spark_session(catalog_name, warehouse_path=warehouse_path)

#     evolve_catalog_schema(spark, catalog_name, schema_name)
#     print(f"Schema evolution completed for {catalog_name}.")
#     spark.stop()