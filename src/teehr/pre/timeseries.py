from typing import Union
from pathlib import Path
import duckdb
from teehr.pre.utils import validate_database_structure

import logging

logger = logging.getLogger(__name__)


def read_insert_file(conn, src, dest, field_mapping):
    """Read a file and insert into the primary_timeseries table."""
    conn.sql(f"""
        INSERT INTO
                primary_timeseries
        SELECT
            {field_mapping['reference_time']}::DATETIME as reference_time,
            {field_mapping['value_time']}::DATETIME as value_time,
            {field_mapping['configuration_name']}::VARCHAR as configuration_name,
            {field_mapping['unit_name']}::VARCHAR as unit_name,
            {field_mapping['variable_name']}::VARCHAR as variable_name,
            {field_mapping['value']}::FLOAT as value,
            {field_mapping['location_id']}::VARCHAR as location_id,
        FROM read_parquet('{src}');
    """)
    conn.sql(f"""
        COPY primary_timeseries TO '{dest}';
    """)
    conn.sql("TRUNCATE TABLE primary_timeseries;")


def convert_primary_timeseries(
    input_filepath: Union[str, Path],
    database_path: Union[str, Path],
    pattern: str = "**/*.parquet",
    field_mapping: dict = None
):
    """Convert primary timeseries data.
    Data can be in either parquet or csv format.
    """
    input_filepath = Path(input_filepath)
    logger.info(f"Converting primary timeseries data from {input_filepath}")

    if not validate_database_structure(database_path):
        raise ValueError("Database structure is not valid.")

    if not field_mapping:
        logger.debug("No field mapping provided. Using default field mapping.")
        field_mapping = {
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration_name": "configuration_name",
            "unit_name": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        }

    # setup validation database
    conn = duckdb.connect(database=":memory:")

    conn.sql(f"""
    CREATE TABLE IF NOT EXISTS units (
            name VARCHAR PRIMARY KEY,
            long_name VARCHAR,
            aliases VARCHAR[],
    );
    INSERT INTO units SELECT *
    FROM read_csv(
        '{database_path}/units/units.csv',
        delim = '|',
        header = true,
        columns = {{'name': 'VARCHAR','long_name': 'VARCHAR','aliases': 'VARCHAR[]'}}
    );
    """)

    conn.sql(f"""
    CREATE TABLE IF NOT EXISTS configurations (
            name VARCHAR PRIMARY KEY,
            type VARCHAR,
            description VARCHAR
    );
    INSERT INTO configurations SELECT *
    FROM read_csv(
        '{database_path}/configurations/configurations.csv',
        delim = '|',
        header = true,
        columns = {{'name': 'VARCHAR','type': 'VARCHAR','description': 'VARCHAR'}}
    );
    """)

    conn.sql(f"""
    CREATE TABLE IF NOT EXISTS variables (
            name VARCHAR PRIMARY KEY,
            long_name VARCHAR
    );
    INSERT INTO variables SELECT *
    FROM read_csv(
        '{database_path}/variables/variables.csv',
        delim = '|',
        header = true,
        columns = {{'name': 'VARCHAR','long_name': 'VARCHAR'}}
    );
    """)

    conn.sql(f"""
    INSTALL spatial;
    LOAD spatial;
    CREATE TABLE IF NOT EXISTS locations (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            geometry GEOMETRY
    );
    INSERT INTO locations
        SELECT id, name, ST_GeomFromWKB(geometry) as geometry
        FROM read_parquet('{database_path}/locations/locations.parquet');
    """)

    conn.sql("""
        CREATE TABLE primary_timeseries (
            reference_time DATETIME,
            value_time DATETIME,
            configuration_name VARCHAR,
            unit_name VARCHAR,
            variable_name VARCHAR,
            value FLOAT,
            location_id VARCHAR,
            FOREIGN KEY (configuration_name) REFERENCES configurations (name),
            FOREIGN KEY (unit_name) REFERENCES units (name),
            FOREIGN KEY (variable_name) REFERENCES variables (name),
            FOREIGN KEY (location_id) REFERENCES locations (id)
        );
    """)

    if input_filepath.is_dir():
        # recursively convert all files in directory
        for file in input_filepath.glob(f"{pattern}"):
            dest_part = file.relative_to(input_filepath)
            dest = Path(database_path, "primary_timeseries", dest_part)
            read_insert_file(conn, file, dest, field_mapping)
    else:
        dest = Path(database_path, "primary_timeseries", input_filepath.name)
        read_insert_file(conn, input_filepath, dest, field_mapping)
