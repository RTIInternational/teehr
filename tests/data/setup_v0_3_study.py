"""Fixtures for v0.3 study tests."""
from pathlib import Path
from teehr import Evaluation, Configuration, Attribute
import shutil
import teehr

import logging

logger = logging.getLogger(__name__)

TEST_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)
GEO_FILEPATH = Path(TEST_DATA_DIR, "geo")
TEST_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)
GEO_FILEPATH = Path(TEST_DATA_DIR, "geo")


def setup_v0_3_study(tmpdir, spark_session):
    """Set up a v0.3 study post-haste."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "v0_3_test_study"
    tar_file = test_data_dir / "local_warehouse.tar.gz"
    temp_extract_dir = Path(tmpdir) / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    # Initialize Spark with new tmpdir location
    (Path(tmpdir) / "local").mkdir(parents=True, exist_ok=True)

    spark = spark_session.newSession()  # Don't alter the fixture
    # or spark_session.getActiveSession()?

    # Initialize Spark with new tmpdir location and db uri
    spark.conf.set(
        f"spark.sql.catalog.local.warehouse",
        (Path(tmpdir) / "local").as_posix()
    )
    spark.conf.set(
        f"spark.sql.catalog.local.uri",
        f"jdbc:sqlite:{(Path(tmpdir) / 'local').as_posix()}/local_catalog.db"
    )
    # Create the database
    spark.sql("CREATE DATABASE IF NOT EXISTS local.teehr")

    # Define tables to recreate
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

    # For each table, read the parquet data files and recreate the Iceberg table
    for table_name in tables_to_recreate:
        old_table_dir = temp_extract_dir / "local" / "teehr" / table_name / "data"
        # Read all parquet files for this table
        df = spark.read.parquet(str(old_table_dir))
        # Create the Iceberg table
        df.writeTo(f"local.teehr.{table_name}").using("iceberg").create()
        # Removed expensive .count() call for better performance

    # Clean up temp extraction directory
    shutil.rmtree(temp_extract_dir)
    ev = Evaluation(
        Path(tmpdir),
        create_dir=False,
        spark=spark,
        check_evaluation_version=False
    )
    return ev


def setup_v0_3_study_rewrite_table_paths(tmpdir, spark_session):
    """This copies in a hadoop-based warehouse and re-writes the tables with jdbc."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "v0_3_test_study"
    tar_file = test_data_dir / "local_warehouse_jdbc.tar.gz"
    # Unpack to a temporary location to access data files
    temp_extract_dir = Path(tmpdir) / "temp_warehouse"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    ev = teehr.Evaluation(
        dir_path=temp_extract_dir,
        spark=spark_session
    )
    # Register the local warehouse.
    ev.rewrite_table_paths()

    return ev


def setup_v0_3_study_from_scratch(tmpdir):
    """Set up a v0.3 study."""
    ev = Evaluation(dir_path=tmpdir, create_dir=True)

    # Enable logging
    ev.enable_logging()

    # Clone the template
    ev.clone_template()

    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            type="primary",
            description="setup_v0_3_study primary configuration"
        )
    )

    # Load the timeseries data and map over the fields and set constants
    ev.primary_timeseries.load_parquet(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "usgs_observations"
        }
    )

    # Load the crosswalk data
    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )

    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            type="secondary",
            description="setup_v0_3_study secondary configuration"
        )
    )

    # Load the secondary timeseries data and map over the fields and set constants
    ev.secondary_timeseries.load_parquet(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "nwm30_retrospective"
        }
    )

    # Add some attributes
    ev.attributes.add(
        [
            Attribute(
                name="drainage_area",
                type="continuous",
                description="Drainage area in square kilometers"
            ),
            Attribute(
                name="ecoregion",
                type="categorical",
                description="Ecoregion"
            ),
            Attribute(
                name="year_2_discharge",
                type="continuous",
                description="2-yr discharge in cubic meters per second"
            ),
        ]
    )

    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=GEO_FILEPATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
    )

    # Create the joined timeseries
    ev.joined_timeseries.create(add_attrs=True, execute_scripts=True)

    return ev
