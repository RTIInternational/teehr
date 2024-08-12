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
            long_name VARCHAR
        );

        CREATE TABLE IF NOT EXISTS configurations (
            name VARCHAR PRIMARY KEY,
            type VARCHAR,
            description VARCHAR
        );

        CREATE TABLE IF NOT EXISTS variables (
            name VARCHAR PRIMARY KEY,
            long_name VARCHAR
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


def insert_units(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.csv"
):
    """Insert units into the database from CSV."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No CSV files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting units from {in_path}.")

    conn.sql(f"""
        INSERT INTO units SELECT *
        FROM read_csv(
            '{in_path}',
            header = true,
            columns = {{'name': 'VARCHAR','long_name': 'VARCHAR'}}
        );
    """) # noqa


def insert_configurations(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.csv"
):
    """Insert configurations into the database from CSV."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No CSV files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting configurations from {in_path}.")

    conn.sql(f"""
        INSERT INTO configurations SELECT *
        FROM read_csv(
            '{in_path}',
            header = true,
            columns = {{'name': 'VARCHAR','type': 'VARCHAR','description': 'VARCHAR'}}
        );
    """) # noqa


def insert_variables(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.csv"
):
    """Insert variables into the database from CSV."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No CSV files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting variables from {in_path}.")

    conn.sql(f"""
        INSERT INTO variables SELECT *
        FROM read_csv(
            '{in_path}',
            header = true,
            columns = {{'name': 'VARCHAR','long_name': 'VARCHAR'}}
        );
    """)


def insert_attributes(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.csv"
):
    """Insert attributes into the database from CSV."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No CSV files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting attributes from {in_path}.")

    conn.sql(f"""
        INSERT INTO attributes SELECT *
        FROM read_csv(
            '{in_path}',
            header = true,
            columns = {{'name': 'VARCHAR','type': 'VARCHAR', 'description': 'VARCHAR'}}
        );
    """) # noqa


def insert_locations(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.parquet"
):
    """Insert locations into the database from Parquet."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No Parquet files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting locations from {in_path}.")

    conn.sql(f"""
        INSERT INTO locations
        SELECT id, name, ST_GeomFromWKB(geometry) as geometry
        FROM read_parquet('{in_path}');
    """)


def insert_location_crosswalks(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.parquet"
):
    """Insert location crosswalks into the database."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No Parquet files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting location crosswalks from {in_path}.")

    conn.sql(f"""
        INSERT INTO location_crosswalks SELECT *
        FROM read_parquet('{in_path}');
    """)


def insert_location_attributes(
    conn: duckdb.DuckDBPyConnection,
    in_path: Union[Path, str],
    pattern: str = "**/*.parquet"
):
    """Insert location_attributes into the database."""
    in_path = Path(in_path)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.info(f"No Parquet files in {in_path}/{pattern}.")
        in_path = str(in_path) + pattern

    logger.debug(f"Inserting location_attributes from {in_path}.")

    conn.sql(f"""
        INSERT INTO location_attributes SELECT *
        FROM read_parquet('{in_path}');
    """)


# Load from dataset
def load_units_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert units into the database."""
    logger.info("Inserting units from dataset to database.")
    filepath = Path(dataset_path, const.UNITS_DIR, const.UNITS_FILE)
    insert_units(conn, filepath)


def load_configurations_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert configurations into the database."""
    logger.info("Inserting configurations from dataset to database.")
    filepath = Path(
        dataset_path, const.CONFIGURATIONS_DIR, const.CONFIGURATIONS_FILE
    )
    insert_configurations(conn, filepath)


def load_variables_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert variables into the database."""
    logger.info("Inserting variables from dataset to database.")
    filepath = Path(dataset_path, const.VARIABLES_DIR, const.VARIABLES_FILE)
    insert_variables(conn, filepath)


def load_attributes_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert attributes into the database."""
    logger.info("Inserting attributes from dataset to database.")
    filepath = Path(
        dataset_path, const.ATTRIBUTES_DIR, const.ATTRIBUTES_FILE
    )
    insert_attributes(conn, filepath)


def load_locations_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert locations into the database."""
    logger.info("Inserting locations from dataset to database.")
    filepath = Path(dataset_path, const.LOCATIONS_DIR)
    insert_locations(conn, filepath)


def load_location_crosswalks_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert location crosswalks into the database."""
    logger.info("Inserting location crosswalks from dataset to database.")
    filepath = Path(dataset_path, const.LOCATION_CROSSWALKS_DIR)
    insert_location_crosswalks(conn, filepath)


def load_location_attributes_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Insert location attributes into the database."""
    logger.info("Inserting location attributes from dataset to database.")
    filepath = Path(dataset_path, const.LOCATION_ATTRIBUTES_DIR)
    insert_location_attributes(conn, filepath)


def load_dataset_to_database(
    conn: duckdb.DuckDBPyConnection,
    dataset_path: Union[Path, str]
):
    """Load a dataset to the database."""
    load_units_from_dataset(conn, dataset_path)
    load_configurations_from_dataset(conn, dataset_path)
    load_variables_from_dataset(conn, dataset_path)
    load_attributes_from_dataset(conn, dataset_path)
    load_locations_from_dataset(conn, dataset_path)
    load_location_crosswalks_from_dataset(conn, dataset_path)
    load_location_attributes_from_dataset(conn, dataset_path)
