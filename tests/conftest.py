"""Defines pytest fixtures for all tests."""
import logging
import pytest
import shutil
from pathlib import Path
import time

from teehr.evaluation.spark_session_utils import create_spark_session
from teehr import LocalReadWriteEvaluation
from teehr.utilities.import_evaluation import update_metadata_paths

from tests.evaluation_test_class import TestEvaluation


def _cleanup_spark(ev):
    """Drop all temporary views and clear the Spark cache."""
    temp_views = ev.spark.sql("SHOW VIEWS").filter("isTemporary = true").collect()
    for view in temp_views:
        try:
            ev.spark.catalog.dropTempView(view.viewName)
        except Exception:
            pass  # View may already be dropped
    ev.spark.catalog.clearCache()


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
    test_data_dir = Path.cwd() / "tests" / "data" / "test_warehouse_data"
    tar_file = test_data_dir / "two_location_test_warehouse.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir / "two_location_test_warehouse",
        spark=spark_shared_session
    )
    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="function")
def function_scope_small_ensemble_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test ensemble warehouse for each test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "test_warehouse_data"
    tar_file = test_data_dir / "ensemble_test_warehouse_small.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir / "ensemble_test_warehouse_small",
        spark=spark_shared_session
    )
    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="function")
def function_scope_large_ensemble_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test ensemble warehouse for each test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "test_warehouse_data"
    tar_file = test_data_dir / "ensemble_test_warehouse_large.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir / "ensemble_test_warehouse_large",
        spark=spark_shared_session
    )
    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="function")
def function_scope_test_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test warehouse once per test function."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "test_warehouse_data"
    tar_file = test_data_dir / "three_location_test_warehouse.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir / "three_location_test_warehouse",
        spark=spark_shared_session
    )
    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="function")
def function_scope_evaluation_template(spark_shared_session, tmp_path_factory):
    """Function-level evaluation fixture with template cloned.

    This creates a new evaluation instance for each test function, using the
    same Spark session and a new temporary directory for the local catalog.
    The local catalog namespace is also unique per test to ensure isolation.
    """
    base_dir = tmp_path_factory.getbasetemp()
    spark = spark_shared_session
    dir_path = Path(base_dir) / "function-scoped-warehouse"
    # Create LocalReadWriteEvaluation with custom namespace
    ev = TestEvaluation(
        dir_path=dir_path,
        namespace_name=f"test_namespace_{int(time.time() * 1000)}",  # Unique namespace per test
        create_dir=True,
        spark=spark,
    )
    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="module")
def module_scope_test_warehouse(tmp_path_factory, spark_shared_session):
    """Unpack test warehouse once per test module."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data"
    tar_file = test_data_dir / "three_location_test_warehouse.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)
    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session
    )
    yield ev
    _cleanup_spark(ev)


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
    tar_file = test_data_dir / "three_location_test_warehouse.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    ev = update_metadata_paths(
        dir_path=temp_extract_dir,
        spark=spark_shared_session,
    )

    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="session")
def session_scope_evaluation_template(spark_shared_session, tmp_path_factory):
    """Session-level evaluation fixture with template cloned."""
    base_dir = tmp_path_factory.getbasetemp()
    spark = spark_shared_session
    warehouse_dir = Path(base_dir) / "session-scoped-warehouse"

    # Create LocalReadWriteEvaluation with custom namespace
    ev = LocalReadWriteEvaluation(
        dir_path=warehouse_dir,
        create_dir=True,
        spark=spark,
    )

    yield ev
    _cleanup_spark(ev)


# To hide warnings from py4j during pytest shutdown
def pytest_configure(config):
    """Configure pytest and py4j logging."""
    # Suppress py4j logging errors during shutdown
    py4j_logger = logging.getLogger("py4j")
    py4j_logger.setLevel(logging.ERROR)  # Only show errors, not info messages
    # Alternatively, to completely silence py4j:
    # py4j_logger.disabled = True

