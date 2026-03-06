"""Utilities for shared evaluation."""
import logging
from typing import Union
from pathlib import Path
import glob
import json

import pandas as pd
import fastavro  # <<-- new dependency for avro file manipulation
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession

from teehr.evaluation.spark_session_utils import create_spark_session
import teehr
from teehr.const import LOCAL_CATALOG_DB_NAME


logger = logging.getLogger(__name__)


def _catalog_uri_is_configured(spark, catalog_name):
    """
    Checks if a local catalog URI is configured in the Spark session.
    Returns True if configured, False otherwise.
    """
    conf_key = f"spark.sql.catalog.{catalog_name}.uri"
    try:
        value = spark.conf.get(conf_key)
        logger.info(f"Spark catalog URI for '{catalog_name}': {value}")
        return True
    except Exception:
        return False


def update_metadata_paths(
    dir_path: Union[str, Path],
    spark: SparkSession = None,
    catalog_name: str = "local",
    namespace_name: str = "teehr",
    database_name: str = LOCAL_CATALOG_DB_NAME,
) -> pd.DataFrame:
    """Import a shared evaluation from a directory path.

    Notes
    -----
    Much of this is borrowed from here: https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/update_iceberg_metadata.py

    The need for this should go away once relative metadata paths are supported, which are currently
    in the v4 Iceberg spec but not yet supported in Spark.
    """
    if spark is None:
        spark = create_spark_session()

    if _catalog_uri_is_configured(spark, catalog_name):
        # If spark catalog database already exists, we need to clear
        # the existing tables in the catalog to avoid conflicts
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace_name}").collect()
        for table in tables:
            table_name = table.tableName
            # Use "DROP TABLE IF EXISTS" to avoid errors if a table is transiently missing
            # The PURGE option removes the underlying data files as well
            try:
                spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{namespace_name}.{table_name} PURGE")
                print(f"Dropped table: {table_name}")
            except Exception as e:
                print(f"Error dropping table {table_name}: {e}")

    warehouse_dir = Path(dir_path) / catalog_name
    db_uri = f"jdbc:sqlite:{warehouse_dir.as_posix()}/{database_name}"
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        warehouse_dir.as_posix()
    )
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.uri",
        f"jdbc:sqlite:{warehouse_dir.as_posix()}/{database_name}"
    )

    # Get the existing metadata paths from the catalog db
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
    updated_tables_df = updated_sdf.toPandas()

    # Need to unlock the table before overwriting!
    # Write back (requires overwriting the entire table)
    updated_sdf.write.format("jdbc") \
        .option("url", db_uri) \
        .option("driver", "org.sqlite.JDBC") \
        .option("dbtable", "iceberg_tables") \
        .mode("overwrite") \
        .save()

    # Test reading the evaluation tables
    ev = teehr.LocalReadWriteEvaluation(
        dir_path,
        create_dir=False,
        spark=spark,
        check_evaluation_version=False
    )

    # Remove .crc files -- these interfere with register_table
    crc_files = glob.glob(f"{new_metadata_prefix}/**/.*.crc", recursive=True)
    [Path(filepath).unlink() for filepath in crc_files]

    # Execute the register_table procedure
    for row in updated_tables_df.itertuples():
        table_name = row.table_name
        metadata_file = row.metadata_location
        ev.sql(f"""
        CALL local.system.register_table(
            table => 'teehr.{table_name}',
            metadata_file => '{metadata_file}'
        )
        """).show()

    return ev