"""Defines pytest fixtures for all tests."""
import pytest
import shutil
from pathlib import Path

from teehr.evaluation.spark_session_utils import create_spark_session
from teehr import Evaluation


@pytest.fixture(scope="session")
def spark_session():
    """Optimized Spark session for tests."""
    spark = create_spark_session(
        app_name="TEEHR Tests",
        update_configs={
            "spark.sql.shuffle.partitions": "4",  # Default is 200, way too high for tests
            "spark.ui.enabled": "false",  # Disable UI for faster startup
            "spark.ui.showConsoleProgress": "false",  # Less console noise
            "spark.sql.adaptive.enabled": "false",  # Faster for small test data
        }
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def cached_test_warehouse_v0_3(tmp_path_factory):
    """Unpack test warehouse once per test session.

    This significantly speeds up tests by avoiding repeated tar.gz unpacking.
    The warehouse is unpacked once and reused across all tests.
    """
    test_data_dir = Path.cwd() / "tests" / "data" / "v0_3_test_study"
    tar_file = test_data_dir / "local_warehouse.tar.gz"

    # Create persistent temp dir for this session
    session_tmpdir = tmp_path_factory.mktemp("test_warehouse_v0_3")
    temp_extract_dir = session_tmpdir / "warehouse"

    # Unpack only once per test session
    shutil.unpack_archive(tar_file, temp_extract_dir)

    return temp_extract_dir


@pytest.fixture(scope="function")
def evaluation_v0_3(tmpdir, spark_session, cached_test_warehouse_v0_3):
    """Fast evaluation setup using cached warehouse data.

    This fixture uses the session-scoped cached warehouse to avoid
    repeated tar extraction. Each test gets its own isolated Evaluation
    instance with a fresh database in its own tmpdir.
    """
    (Path(tmpdir) / "local").mkdir(parents=True, exist_ok=True)

    spark = spark_session.newSession()
    spark.conf.set(
        "spark.sql.catalog.local.warehouse",
        (Path(tmpdir) / "local").as_posix()
    )
    spark.conf.set(
        "spark.sql.catalog.local.uri",
        f"jdbc:sqlite:{(Path(tmpdir) / 'local').as_posix()}/local_catalog.db"
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS local.teehr")

    tables_to_recreate = [
        "primary_timeseries",
        "secondary_timeseries",
        "joined_timeseries",
        "locations",
        "location_attributes",
        "location_crosswalks",
        "units",
        "variables",
        "attributes",
        "configurations"
    ]

    # Recreate tables from cached warehouse (no tar extraction!)
    for table_name in tables_to_recreate:
        data_dir = cached_test_warehouse_v0_3 / "local" / "teehr" / table_name / "data"
        if data_dir.exists():
            df = spark.read.parquet(str(data_dir))
            df.writeTo(f"local.teehr.{table_name}").using("iceberg").create()

    ev = Evaluation(
        Path(tmpdir),
        create_dir=False,
        spark=spark,
        check_evaluation_version=False
    )

    return ev


@pytest.fixture(scope="module")
def evaluation_v0_3_module(tmp_path_factory, spark_session, cached_test_warehouse_v0_3):
    """Module-scoped evaluation for read-only tests.

    Use this fixture when multiple tests in a module can safely share
    the same evaluation data without modifying it. This provides even
    better performance for read-only operations.
    """
    module_tmpdir = tmp_path_factory.mktemp("module_eval_v0_3")
    (module_tmpdir / "local").mkdir(parents=True, exist_ok=True)

    spark = spark_session.newSession()
    spark.conf.set(
        "spark.sql.catalog.local.warehouse",
        (module_tmpdir / "local").as_posix()
    )
    spark.conf.set(
        "spark.sql.catalog.local.uri",
        f"jdbc:sqlite:{(module_tmpdir / 'local').as_posix()}/local_catalog.db"
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS local.teehr")

    tables_to_recreate = [
        "primary_timeseries",
        "secondary_timeseries",
        "joined_timeseries",
        "locations",
        "location_attributes",
        "location_crosswalks",
        "units",
        "variables",
        "attributes",
        "configurations"
    ]

    for table_name in tables_to_recreate:
        data_dir = cached_test_warehouse_v0_3 / "local" / "teehr" / table_name / "data"
        if data_dir.exists():
            df = spark.read.parquet(str(data_dir))
            df.writeTo(f"local.teehr.{table_name}").using("iceberg").create()

    ev = Evaluation(
        module_tmpdir,
        create_dir=False,
        spark=spark,
        check_evaluation_version=False
    )

    return ev


@pytest.fixture(scope="module")
def cached_joined_timeseries_df(evaluation_v0_3_module):
    """Cached joined timeseries DataFrame for read-only tests.

    This fixture loads the joined_timeseries data once per module and
    caches it in Spark memory. Use this for tests that only read data
    and don't modify it. This can significantly speed up metric calculation
    and query tests.
    """
    df = evaluation_v0_3_module.joined_timeseries.to_sdf()
    df.cache()
    yield df
    df.unpersist()


@pytest.fixture(scope="module")
def cached_primary_timeseries_df(evaluation_v0_3_module):
    """Cached primary timeseries DataFrame for read-only tests."""
    df = evaluation_v0_3_module.primary_timeseries.to_sdf()
    df.cache()
    yield df
    df.unpersist()