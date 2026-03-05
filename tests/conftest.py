"""Defines pytest fixtures for all tests."""
import logging
import pytest
import shutil
from pathlib import Path
import time

from teehr.evaluation.spark_session_utils import create_spark_session
from teehr import Evaluation
from teehr.utilities import apply_migrations
from teehr.utilities.import_evaluation import update_metadata_paths


@pytest.fixture(scope="session")
def spark_shared_session():
    """Session-level Spark session shared across all tests.

    This creates one Spark session for the entire test run and stops it
    at the end. Individual tests should NOT stop this session.
    """
    spark = create_spark_session()
    yield spark
    spark.stop()

@pytest.fixture(scope="function")
def function_scope_two_location_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test ensemble warehouse for each test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "two_location_test_warehouse.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session
    )
    yield ev

@pytest.fixture(scope="function")
def function_scope_small_ensemble_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test ensemble warehouse for each test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "ensemble_test_warehouse_small.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session
    )
    yield ev

@pytest.fixture(scope="function")
def function_scope_large_ensemble_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test ensemble warehouse for each test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "ensemble_test_warehouse_large.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session
    )
    yield ev

@pytest.fixture(scope="function")
def function_scope_test_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test warehouse once per test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "local_warehouse_jdbc.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session
    )
    yield ev


@pytest.fixture(scope="function")
def function_scope_evaluation_template(session_scope_evaluation_template, request):
    """Function-level evaluation fixture with template cloned to a new namespace."""
    ev = session_scope_evaluation_template

    # NOTE: Could I re-create the catalog db entirely here instead
    # or as well?
    # self._set_active_catalog("local")  # Creates the JDBC .db file
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
    migrations_dir = Path(__file__).parents[1] / "src/teehr/migrations"
    apply_migrations.evolve_catalog_schema(
        spark=ev.spark,
        migrations_dir_path=migrations_dir,
        target_catalog_name="local",
        target_namespace_name=test_namespace
    )

    # Clean up the catalog? Go back to original namespace and snapshot?

    yield ev
    ev.spark.catalog.clearCache()  # not sure if necessary
    # After the test reset the namespace name to original value to maintain isolation
    ev.local_catalog.namespace_name = original_namespace
    ev._set_active_catalog("local")  # Reset active catalog to original


@pytest.fixture(scope="module")
def module_scope_test_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test warehouse once per test module."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "local_warehouse_jdbc.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session
    )
    yield ev

@pytest.fixture(scope="session")
def session_scope_test_warehouse(tmp_path_factory, spark_shared_session):
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

    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session,
        database_name="local_catalog.db"
    )

    yield ev


@pytest.fixture(scope="session")
def session_scope_evaluation_template(spark_shared_session, tmp_path_factory):
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

    yield ev

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
