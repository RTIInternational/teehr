"""Fixtures for v0.3 study tests."""
from pathlib import Path
import glob
from teehr import Evaluation, Configuration, Attribute
import shutil
import teehr
from teehr.evaluation.spark_session_utils import create_spark_session
from pyspark.sql.functions import when, col
from pyspark.sql.functions import regexp_replace

import tempfile

import logging
import json
import io
import fastavro

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


def setup_v0_3_study_ORIGINAL(tmpdir, spark_session):
    """Set up a v0.3 study post-haste."""
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "v0_3_test_study"
    tar_file = test_data_dir / "local_warehouse.tar.gz"
    temp_extract_dir = Path(tmpdir) / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    # Initialize Spark with new tmpdir location
    (Path(tmpdir) / "local").mkdir(parents=True, exist_ok=True)

    spark = spark_session.newSession()  # Don't alter the fixture
    # or spark_session.newSession()?

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


def setup_v0_3_study(tmpdir, spark_session):
    """This copies in a hadoop-based warehouse and re-writes the tables with jdbc.

    Notes
    -----
    This is heavily borrowed from:
    https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/update_iceberg_metadata.py
    """
    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "v0_3_test_study"
    tar_file = test_data_dir / "local_warehouse_jdbc.tar.gz"
    temp_extract_dir = Path(tmpdir) / "temp_extract"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    warehouse_dir = temp_extract_dir / "local"
    db_uri = f"jdbc:sqlite:{warehouse_dir.as_posix()}/local_catalog.db"

    # spark = create_spark_session()
    spark = spark_session(
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


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        setup_v0_3_study_rewrite_table_paths(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tempdir
            ),
            create_spark_session()
        )