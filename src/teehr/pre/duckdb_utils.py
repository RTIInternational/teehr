"""Functions for creating and inserting data to a DuckDB in-memory database."""
from teehr.pre.utils import logger
from pathlib import Path
from typing import Union
import teehr.const as const

import duckdb


def create_database_tables(conn: duckdb.DuckDBPyConnection):
    """Create database tables."""
    logger.info("Creating database tables.")
    conn.sql("""
        CREATE TABLE IF NOT EXISTS units (
            name VARCHAR PRIMARY KEY,
            long_name VARCHAR,
            aliases VARCHAR[],
        );

        CREATE TABLE IF NOT EXISTS configurations (
            name VARCHAR PRIMARY KEY,
            type VARCHAR,
            description VARCHAR
        );

        CREATE TABLE IF NOT EXISTS variables (
            name VARCHAR PRIMARY KEY,
            long_name VARCHAR
            -- interval_seconds INTEGER,
        );

        CREATE TABLE IF NOT EXISTS attributes (
            name VARCHAR PRIMARY KEY,
            type VARCHAR,
            description VARCHAR
        );

        INSTALL spatial;
        LOAD spatial;
        CREATE TABLE IF NOT EXISTS locations (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            geometry GEOMETRY
        );

        CREATE TABLE IF NOT EXISTS location_crosswalks (
            primary_location_id VARCHAR,
            secondary_location_id VARCHAR UNIQUE,
            PRIMARY KEY (secondary_location_id, primary_location_id),
            FOREIGN KEY (primary_location_id) REFERENCES locations (id)
        );

        CREATE TABLE IF NOT EXISTS primary_timeseries (
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

        CREATE TABLE IF NOT EXISTS secondary_timeseries (
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
            FOREIGN KEY (location_id) REFERENCES location_crosswalks
                (secondary_location_id)
        );

        CREATE TABLE IF NOT EXISTS location_attributes(
            location_id VARCHAR,
            attribute_name VARCHAR,
            value VARCHAR,
            FOREIGN KEY (attribute_name) REFERENCES attributes (name),
            FOREIGN KEY (location_id) REFERENCES locations (id)
        );

        CREATE TABLE IF NOT EXISTS joined_timeseries(
            reference_time DATETIME,
            value_time DATETIME,
            configuration_name VARCHAR,
            unit_name VARCHAR,
            variable_name VARCHAR,
            primary_location_id VARCHAR,
            secondary_location_id VARCHAR,
            primary_value FLOAT,
            secondary_value FLOAT,
            FOREIGN KEY (configuration_name) REFERENCES configurations (name),
            FOREIGN KEY (unit_name) REFERENCES units (name),
            FOREIGN KEY (variable_name) REFERENCES variables (name),
            FOREIGN KEY (secondary_location_id)
                REFERENCES location_crosswalks (secondary_location_id),
            FOREIGN KEY (primary_location_id) REFERENCES locations(id)
        );
    """)


def insert_units(conn: duckdb.DuckDBPyConnection, dataset_path: str):
    """Insert units into the database."""
    logger.info("Inserting units from dataset to database.")
    filepath = Path(dataset_path, const.UNITS_DIR, const.UNITS_FILE)
    conn.sql(f"""
        INSERT INTO units SELECT *
        FROM read_csv(
            '{filepath}',
            delim = '|',
            header = true,
            columns = {{'name': 'VARCHAR','long_name': 'VARCHAR','aliases': 'VARCHAR[]'}}
        );
    """) # noqa


def insert_configurations(conn: duckdb.DuckDBPyConnection, dataset_path: str):
    """Insert configurations into the database."""
    logger.info("Inserting configurations from dataset to database.")
    filepath = Path(
        dataset_path, const.CONFIGURATIONS_DIR, const.CONFIGURATIONS_FILE
    )
    conn.sql(f"""
        INSERT INTO configurations SELECT *
        FROM read_csv(
            '{filepath}',
            delim = '|',
            header = true,
            columns = {{'name': 'VARCHAR','type': 'VARCHAR','description': 'VARCHAR'}}
        );
    """) # noqa


def insert_variables(conn: duckdb.DuckDBPyConnection, dataset_path: str):
    """Insert variables into the database."""
    logger.info("Inserting variables from dataset to database.")
    filepath = Path(dataset_path, const.VARIABLES_DIR, const.VARIABLES_FILE)
    conn.sql(f"""
        INSERT INTO variables SELECT *
        FROM read_csv(
            '{filepath}',
            delim = '|',
            header = true,
            columns = {{'name': 'VARCHAR','long_name': 'VARCHAR'}}
        );
    """)


def insert_attributes(conn: duckdb.DuckDBPyConnection, dataset_path: str):
    """Insert attributes into the database."""
    logger.info("Inserting attributes from dataset to database.")
    filepath = Path(
        dataset_path, const.ATTRIBUTES_DIR, const.ATTRIBUTES_FILE
    )
    conn.sql(f"""
        INSERT INTO attributes SELECT *
        FROM read_csv(
            '{filepath}',
            delim = '|',
            header = true,
            columns = {{'name': 'VARCHAR','type': 'VARCHAR', 'description': 'VARCHAR'}}
        );
    """) # noqa


def insert_locations(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    in_pattern: str = "**/*.parquet"
):
    """Insert locations into the database."""
    logger.info("Inserting locations.")
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(in_pattern))) == 0:
            logger.error(f"No parquet files in {in_path}.")
            raise FileNotFoundError
        in_path = str(in_path) + in_pattern

    logger.debug(f"Inserting locations from {in_path}.")

    conn.sql(f"""
        INSERT INTO locations
        SELECT id, name, ST_GeomFromWKB(geometry) as geometry
        FROM read_parquet('{in_path}');
    """)


def insert_location_crosswalks(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    in_pattern: str = "**/*.parquet"
):
    """Insert location crosswalks into the database."""
    logger.info("Inserting location crosswalks.")
    in_path = Path(in_path)
    if in_path.is_dir():
        if len(list(in_path.glob(in_pattern))) == 0:
            logger.error(f"No parquet files in {in_path}.")
            raise FileNotFoundError
        in_path = str(in_path) + in_pattern

    logger.debug(f"Inserting location crosswalks from {in_path}.")

    conn.sql(f"""
        INSERT INTO location_crosswalks SELECT *
        FROM read_parquet('{in_path}');
    """)


def insert_location_attributes(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    in_pattern: str = "**/*.parquet"
):
    """Insert location_attributes into the database."""
    logger.info("Inserting location_attributes.")
    in_path = Path(in_path)
    if in_path.is_dir():
        if len(list(in_path.glob(in_pattern))) == 0:
            logger.error(f"No parquet files in {in_path}.")
            raise FileNotFoundError
        in_path = str(in_path) + in_pattern

    logger.debug(f"Inserting location_attributes from {in_path}.")

    conn.sql(f"""
        INSERT INTO location_attributes SELECT *
        FROM read_parquet('{in_path}');
    """)
