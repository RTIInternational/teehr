"""Defines pytest fixtures for all tests.

Function scope: When a fixture must be isolated for each test function
(e.g., because the test modifies data).

Module scope: When a fixture can be shared across all tests in a module
(e.g., read-only data that is expensive to set up).

Session scope: When a fixture can be shared across the entire test session
(e.g., a Spark session or a large test warehouse that is read-only).

In all fixtures returning an Evaluation instance,
any un-registered TEEHR migrations are applied.
"""
import logging
import pytest
import shutil
from pathlib import Path
import time

from teehr.evaluation.spark_session_utils import create_spark_session
from teehr import LocalReadWriteEvaluation
from teehr.utilities.import_evaluation import update_metadata_paths


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
    """Extract and import the two location test warehouse for each test function.

    This contains two locations with USGS observations and NWM 3.0 retrospective streamflow.

    The joined_timeseries_view as written to "joined_timeseries" with attrs and
    calculated fields (month, year, water year, season) were also added.

    See: tests/data/test_warehouse_setup/create_two_locations_test_warehouse.py for how this was created.
    """
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
    """Extract and import the small ensemble test warehouse for each test function.

    This contains a small HEFS ensemble sample with USGS observations at the Platte River.
    Climatology/normals and benchmark forecast timeseries are also included.
    The joined_timeseries_view as written to "joined_timeseries" without attrs or UDFs.

    See: tests/data/test_warehouse_setup/create_small_ensemble_test_warehouse.py for how this was created.
    """
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
    """Extract and import the large ensemble test warehouse for each test function.

    This contains several secondary ensemble configurations including enspost, LSTM, QQM, and "raw".
    Benchmark forecasts based on daily normals are also included, as well as observations.
    The joined_timeseries_view as written to "joined_timeseries" without attrs but includes
    calculated fields (month, year, water year, season).

    See: tests/data/test_warehouse_setup/create_large_ensemble_test_warehouse.py for how this was created.
    """
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
    """Extract and import the three location test warehouse for each test function.

    This contains three locations with USGS observations and NWM 3.0 retrospective streamflow.
    Drainage area, ecoregion, and 2-yr discharge attributes are included for each location.
    The joined_timeseries_view as written to "joined_timeseries" with attrs and
    calculated fields (month, year, water year, season).

    See: tests/data/test_warehouse_setup/create_three_location_test_warehouse.py for how this was created.
    """
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

    This creates an "empty" evaluation for each test function.

    Domain tables are populated per migrations but no data is loaded.
    """
    base_dir = tmp_path_factory.getbasetemp()
    spark = spark_shared_session
    dir_path = Path(base_dir) / "function-scoped-warehouse"
    # Create LocalReadWriteEvaluation with custom namespace
    ev = LocalReadWriteEvaluation(
        dir_path=dir_path,
        namespace_name=f"test_namespace_{int(time.time() * 1000)}",  # Unique namespace per test
        create_dir=True,
        spark=spark,
    )
    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="module")
def module_scope_test_warehouse(tmp_path_factory, spark_shared_session):
    """Extract and import the three location test warehouse for each test module.

    This contains three locations with USGS observations and NWM 3.0 retrospective streamflow.
    Drainage area, ecoregion, and 2-yr discharge attributes are included for each location.
    The joined_timeseries_view as written to "joined_timeseries" with attrs and
    calculated fields (month, year, water year, season).

    See: tests/data/test_warehouse_setup/create_three_location_test_warehouse.py for how this was created.
    """
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


@pytest.fixture(scope="session")
def session_scope_test_warehouse(tmp_path_factory, spark_shared_session):
    """Extract and import the three location test warehouse for each test SESSION (not per test function).

    This contains three locations with USGS observations and NWM 3.0 retrospective streamflow.
    Drainage area, ecoregion, and 2-yr discharge attributes are included for each location.
    The joined_timeseries_view as written to "joined_timeseries" with attrs and
    calculated fields (month, year, water year, season).

    This can be used for tests that don't modify data and want to avoid the overhead
    of initializing the warehouse multiple times.

    See: tests/data/test_warehouse_setup/create_three_location_test_warehouse.py for how this was created..
    """
    test_data_dir = Path.cwd() / "tests" / "data" / "test_warehouse_data"
    tar_file = test_data_dir / "three_location_test_warehouse.tar.gz"
    temp_extract_dir = tmp_path_factory.mktemp("warehouse_session") / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    ev = update_metadata_paths(
        dir_path=temp_extract_dir / "three_location_test_warehouse",
        spark=spark_shared_session,
    )

    yield ev
    _cleanup_spark(ev)


@pytest.fixture(scope="session")
def session_scope_evaluation_template(spark_shared_session, tmp_path_factory):
    """Session-level evaluation template fixture.

    This creates an "empty" evaluation for each test session. This can be used
    for tests that don't modify data and want to avoid the overhead of initializing
    the warehouse multiple times.

    Domain tables are populated per migrations but no data is loaded.
    """
    base_dir = tmp_path_factory.getbasetemp()
    spark = spark_shared_session
    warehouse_dir = Path(base_dir) / "session-scoped-warehouse"
    # Create LocalReadWriteEvaluation
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

