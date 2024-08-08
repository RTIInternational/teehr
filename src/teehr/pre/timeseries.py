from typing import Union
from pathlib import Path
import duckdb
from teehr.pre.utils import validate_dataset_structure
from teehr.pre.utils import merge_field_mappings
from teehr.pre.duckdb_utils import (
    create_database_tables,
    insert_configurations,
    insert_units,
    insert_variables,
    insert_locations,
    insert_location_crosswalks,
)
import teehr.const as const

import logging

logger = logging.getLogger(__name__)


def read_insert_timeseries(
    conn: duckdb.DuckDBPyConnection,
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    timeseries_type: str,
    field_mapping: dict
):
    """Read a file and insert into the primary_timeseries table."""
    logger.info(f"Reading and inserting primary timeseries data from {in_filepath}")
    if timeseries_type == "primary":
        table = "primary_timeseries"
    elif timeseries_type == "secondary":
        table = "secondary_timeseries"
    else:
        raise ValueError("Invalid timeseries type.")

    conn.sql(f"""
        INSERT INTO
                {table}
        SELECT
            {field_mapping['reference_time']}::DATETIME
                as reference_time,
            {field_mapping['value_time']}::DATETIME
                as value_time,
            {field_mapping['configuration_name']}::VARCHAR
                as configuration_name,
            {field_mapping['unit_name']}::VARCHAR
                as unit_name,
            {field_mapping['variable_name']}::VARCHAR
                as variable_name,
            {field_mapping['value']}::FLOAT
                as value,
            {field_mapping['location_id']}::VARCHAR
                as location_id,
        FROM read_parquet('{in_filepath}');
    """)
    conn.sql(f"""
        COPY {table} TO '{out_filepath}';
    """)
    conn.sql(f"TRUNCATE TABLE {table};")


def validate_and_insert_timeseries(
    path: Union[str, Path],
    dataset_path: Union[str, Path],
    timeseries_type: str,
    pattern: str = "**/*.parquet",
    field_mapping: dict = None
):
    """Validate and insert primary timeseries data.

    Parameters
    ----------
    path : Union[str, Path]
        Directory path or file path to the primary timeseries data.
    dataset_path : Union[str, Path]
        Path to the dataset.
    timeseries_type : str
        The type of timeseries data.
        Valid values: "primary", "secondary"
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}

    """
    path = Path(path)
    logger.info(f"Converting timeseries data from {path}")

    if not validate_dataset_structure(dataset_path):
        raise ValueError("Database structure is not valid.")

    default_field_mapping = {
        "reference_time": "reference_time",
        "value_time": "value_time",
        "configuration_name": "configuration_name",
        "unit_name": "unit_name",
        "variable_name": "variable_name",
        "value": "value",
        "location_id": "location_id"
    }
    if field_mapping:
        logger.debug("Merging user field_mapping with default field mapping.")
        field_mapping = merge_field_mappings(
            default_field_mapping,
            field_mapping
        )
    else:
        logger.debug("Using default field mapping.")
        field_mapping = default_field_mapping

    # swap keys and values for user provided field mapping
    # required for consistiency with other functions
    field_mapping = {v: k for k, v in field_mapping.items()}

    # setup validation database
    conn = duckdb.connect()

    # create tables if they do not exist
    create_database_tables(conn)

    # insert domains
    insert_units(conn, dataset_path)
    insert_configurations(conn, dataset_path)
    insert_variables(conn, dataset_path)

    # insert locations
    insert_locations(
        conn,
        Path(dataset_path, const.LOCATIONS_DIR)
    )

    if timeseries_type == "primary":
        timeseries_dir = Path(dataset_path, const.PRIMARY_TIMESERIES_DIR)
    elif timeseries_type == "secondary":
        timeseries_dir = Path(dataset_path, const.SECONDARY_TIMESERIES_DIR)
        insert_location_crosswalks(
            conn,
            Path(dataset_path, const.LOCATION_CROSSWALKS_DIR)
        )
    else:
        raise ValueError("Invalid timeseries type.")

    if path.is_dir():
        # recursively convert all files in directory
        for in_filepath in path.glob(f"{pattern}"):
            dest_part = in_filepath.relative_to(path)
            out_filepath = Path(timeseries_dir, dest_part)
            read_insert_timeseries(
                conn, in_filepath,
                out_filepath,
                timeseries_type,
                field_mapping
            )
    else:
        out_filepath = Path(timeseries_dir, path.name)
        read_insert_timeseries(
            conn,
            path,
            out_filepath,
            timeseries_type,
            field_mapping
        )
