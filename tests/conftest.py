"""Defines pytest fixtures for all tests."""
import logging
import pytest
import shutil
from pathlib import Path
import glob
import json
import fastavro
from pyspark.sql.functions import regexp_replace
import time

from teehr.evaluation.spark_session_utils import create_spark_session
from teehr import Evaluation
from teehr.utilities import apply_migrations


@pytest.fixture(scope="session")
def spark_shared_session():
    """Session-level Spark session shared across all tests.

    This creates one Spark session for the entire test run and stops it
    at the end. Individual tests should NOT stop this session.
    """
    spark = create_spark_session()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def read_only_test_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test warehouse once per test SESSION (not per test function).

    This significantly speeds up tests by:
    1. Unpacking warehouse once for entire test run
    2. Sharing the warehouse across all tests
    3. Reusing the same Spark session

    Note: All tests using this fixture share the same warehouse data.
    If tests modify data, they may affect each other. Use a function-scoped
    fixture if you need isolation (but it will be much slower).
    """
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "local_warehouse_jdbc.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    warehouse_dir = temp_extract_dir / "local"
    db_uri = f"jdbc:sqlite:{warehouse_dir.as_posix()}/local_catalog.db"

    # Use the shared session directly since catalog config is immutable
    spark = spark_shared_session
    spark.conf.set(
        f"spark.sql.catalog.local.warehouse",
        warehouse_dir.as_posix()
    )
    spark.conf.set(
        f"spark.sql.catalog.local.uri",
        f"jdbc:sqlite:{warehouse_dir.as_posix()}/local_catalog.db"
    )

    # Get the existing metadata paths from the local_catalog.db
    # SQLite database.
    iceberg_df = spark.read.format("jdbc") \
        .option("url", db_uri) \
        .option("driver", "org.sqlite.JDBC") \
        .option("query", "SELECT * FROM iceberg_tables") \
        .load().toPandas()

    # Get metadata filepath prefixes from the existing table
    latest_metadata = iceberg_df.loc[
        iceberg_df['table_name'] == 'primary_timeseries', 'metadata_location'
    ].values[0]
    old_metadata_prefix = Path(latest_metadata).parents[3].as_posix()
    new_metadata_prefix = warehouse_dir.as_posix()

    # All json paths
    all_json_paths = glob.glob(f"{new_metadata_prefix}/**/*.json", recursive=True)
    all_avro_paths = glob.glob(f"{new_metadata_prefix}/**/*.avro", recursive=True)

    def update_avro_metadata(new_avro_path, old_metadata_prefix, new_metadata_prefix):

        reader = fastavro.reader(open(new_avro_path, 'rb'))
        parsed_schema = reader.writer_schema
        records = list(reader)

        # Iterate over records in the Avro file
        for record in records:
            for key, value in record.items():
                # If it is a nested dictionary, check its items
                if isinstance(value, dict):
                    for k, v in value.items():
                        if type(v) is str:
                            if old_metadata_prefix in v:
                                value[k] = v.replace(old_metadata_prefix, new_metadata_prefix)

                # If it's a list, check each element
                elif isinstance(value, list):
                    for i in range(len(value)):
                        if isinstance(value[i], dict):
                            for k, v in value[i].items():
                                if type(v) is str:
                                    if old_metadata_prefix in v:
                                        value[k] = v.replace(old_metadata_prefix, new_metadata_prefix)
                else:
                    if type(value) is str:
                        if old_metadata_prefix in value:
                            record[key] = value.replace(old_metadata_prefix, new_metadata_prefix)

        # Writing
        with open(new_avro_path, 'wb') as out:
            fastavro.writer(out, parsed_schema, records)

    # Loop avro files here
    for avro_filepath in all_avro_paths:
        update_avro_metadata(
            avro_filepath,
            old_metadata_prefix,
            new_metadata_prefix
        )

    # Function to search and replace a value in the JSON structure
    def replace_json_values(json_data, target_value, replacement_value):
        if isinstance(json_data, dict):  # If it's a dictionary, iterate over the keys and values
            for key, value in json_data.items():
                if type(value) is str:
                        if old_metadata_prefix in value:
                            json_data[key] = value.replace(old_metadata_prefix, new_metadata_prefix)
                else:
                    replace_json_values(value, target_value, replacement_value) # Recursively call for nested values

        elif isinstance(json_data, list):  # If it's a list, iterate over the items
            for index, item in enumerate(json_data):
                if old_metadata_prefix in item:
                    json_data[index] = item.replace(old_metadata_prefix, new_metadata_prefix)
                else:
                    replace_json_values(item, target_value, replacement_value)  # Recursively call for nested items

    for json_filepath in all_json_paths:
        # Read the JSON file
        with open(json_filepath, 'r') as f:
            metadata = json.load(f)

        replace_json_values(
            metadata,
            old_metadata_prefix,
            new_metadata_prefix
        )
        # Convert the modified data back to JSON
        # modified_json_content = json.dumps(metadata, indent = 4)
        modified_json_content = json.dumps(metadata)

        # Write back the modified JSON to the same file
        with open(json_filepath, 'w') as f:
            f.write(modified_json_content)

    # Update the sqlite table
    iceberg_sdf = spark.read.format("jdbc") \
        .option("url", db_uri) \
        .option("driver", "org.sqlite.JDBC") \
        .option("query", "SELECT * FROM iceberg_tables") \
        .load()


    # Replace "Guard" with "Gd" in the "position" column
    updated_sdf = iceberg_sdf.withColumn(
        "metadata_location",
        regexp_replace("metadata_location", old_metadata_prefix, new_metadata_prefix)
    )
    updated_sdf = updated_sdf.withColumn(
        "previous_metadata_location",
        regexp_replace("previous_metadata_location", old_metadata_prefix, new_metadata_prefix)
    )
    tmp_df = updated_sdf.toPandas()

    # Need to unlock the table before overwriting!
    # Write back (requires overwriting the entire table)
    updated_sdf.write.format("jdbc") \
        .option("url", db_uri) \
        .option("driver", "org.sqlite.JDBC") \
        .option("dbtable", "iceberg_tables") \
        .mode("overwrite") \
        .save()

    # Test reading the evaluation tables
    ev = Evaluation(
        temp_extract_dir,
        create_dir=False,
        spark=spark,
        check_evaluation_version=False
    )

    # Remove .crc files -- these interfere with register_table
    crc_files = glob.glob(f"{new_metadata_prefix}/**/.*.crc", recursive=True)
    [Path(filepath).unlink() for filepath in crc_files]

    # Execute the register_table procedure
    for row in tmp_df.itertuples():
        table_name = row.table_name
        metadata_file = row.metadata_location
        ev.spark.sql(f"""
        CALL local.system.register_table(
            table => 'teehr.{table_name}',
            metadata_file => '{metadata_file}'
        )
        """).show()

    yield ev
    # Don't stop the shared Spark session - it's managed by spark_shared_session fixture
    # The session is reused across all tests for performance


@pytest.fixture(scope="session")
def read_only_evaluation_template(spark_shared_session, tmp_path_factory):
    """Session-level evaluation fixture with template cloned."""
    base_dir = tmp_path_factory.getbasetemp()
    spark = spark_shared_session
    warehouse_dir = Path(base_dir) / "session-scoped-warehouse"

    # Create Evaluation with custom namespace
    ev = Evaluation(
        dir_path=warehouse_dir,
        create_dir=True,
        spark=spark,
    )
    ev.clone_template()

    yield ev


@pytest.fixture(scope="function")
def read_write_evaluation_template(read_only_evaluation_template, request):
    """Function-level evaluation fixture with template cloned to a new namespace."""
    ev = read_only_evaluation_template

    # NOTE: Could I re-create the local_catalog.db entirely here instead
    # or as well?
    # self.set_active_catalog("local")  # Creates the JDBC .db file
    # But then you'd have to re-register all the tables too...

    # Clear any temp views from previous tests to ensure isolation
    temp_views = ev.spark.sql("SHOW VIEWS").filter("isTemporary = true").collect()
    for view in temp_views:
        try:
            ev.spark.catalog.dropTempView(view.viewName)
        except Exception:
            pass  # View may already be dropped
    ev.spark.catalog.clearCache()

    # Save the original namespace to restore after test
    original_namespace = ev.local_catalog.namespace_name

    # Create unique namespace per test using test name
    test_name = request.node.name.replace("[", "_").replace("]", "_")
    test_namespace = f"{int(time.time() / 1e5)}_{test_name}"

    # Create the namespace in Iceberg. Creates the namespace but not the directory yet.
    ev.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{test_namespace}")

    # Override the namespace for this evaluation
    ev.local_catalog.namespace_name = test_namespace

    # Set up the tables in the new namespace.
    apply_migrations.evolve_catalog_schema(
        spark=ev.spark,
        migrations_dir_path=ev.dir_path / "local",
        local_catalog_name="local",
        local_namespace_name="teehr",
        target_catalog_name="local",
        target_namespace_name=test_namespace
    )

    # Clean up the catalog? Go back to original namespace and snapshot?

    yield ev
    # ev.spark.catalog.clearCache()  # not sure if necessary
    # # After the test reset the namespace name to original value to maintain isolation
    # ev.local_catalog.namespace_name = original_namespace
    # # Cleanup: Drop the namespace after test to remove all tables and data
    # try:
    #     ev.spark.sql(f"DROP NAMESPACE IF EXISTS local.{test_namespace} CASCADE")
    # except Exception as e:
    #     print(f"Warning: Could not drop namespace {test_namespace}: {e}")


# To hide warnings from py4j during pytest shutdown
def pytest_configure(config):
    """Configure pytest and py4j logging."""
    # Suppress py4j logging errors during shutdown
    py4j_logger = logging.getLogger("py4j")
    py4j_logger.setLevel(logging.ERROR)  # Only show errors, not info messages
    # Alternatively, to completely silence py4j:
    # py4j_logger.disabled = True


# ==============================================================================
# ============================Below are unused==================================
# ==============================================================================

# @pytest.fixture(scope="module")
# def cached_joined_timeseries_df(evaluation_v0_3_module):
#     """Cached joined timeseries DataFrame for read-only tests.

#     This fixture loads the joined_timeseries data once per module and
#     caches it in Spark memory. Use this for tests that only read data
#     and don't modify it. This can significantly speed up metric calculation
#     and query tests.
#     """
#     df = evaluation_v0_3_module.joined_timeseries.to_sdf()
#     df.cache()
#     yield df
#     df.unpersist()


# @pytest.fixture(scope="module")
# def cached_primary_timeseries_df(evaluation_v0_3_module):
#     """Cached primary timeseries DataFrame for read-only tests."""
#     df = evaluation_v0_3_module.primary_timeseries.to_sdf()
#     df.cache()
#     yield df
#     df.unpersist()
