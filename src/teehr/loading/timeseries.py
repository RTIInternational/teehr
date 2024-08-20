"""Convert and insert timeseries data into the dataset."""
from typing import Union
from pathlib import Path
import duckdb
import pandas as pd
from teehr.loading.utils import (
    validate_dataset_structure,
    merge_field_mappings,
    validate_constant_values_dict
)
from teehr.loading.duckdb_sql import (
    create_database_tables,
    load_configurations_from_dataset,
    load_units_from_dataset,
    load_variables_from_dataset,
    insert_locations,
    insert_location_crosswalks,
)
from teehr.models.dataset.table_models import Timeseries
import teehr.const as const

import logging

logger = logging.getLogger(__name__)


def convert_single_timeseries(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    field_mapping: dict,
    constant_field_values: dict = None,
    **kwargs
):
    """Convert timeseries data to parquet format.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    out_filepath : Union[str, Path]
        The output file path.
    field_mapping : dict
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    constant_field_values : dict, optional
        A dictionary mapping field names to constant values.
        format: {field_name: value}
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv() or pd.read_parquet().

    Steps:
    1. Read the file
    2. Rename the columns based on the field_mapping
    3. Add constant field values to dataframe,
        this may result in too many columns.
    4. Subset only the columns in the field_mapping
    5. Write the dataframe to parquet format

    """
    in_filepath = Path(in_filepath)
    out_filepath = Path(out_filepath)

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        timeseries = pd.read_parquet(in_filepath, **kwargs)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        timeseries = pd.read_csv(in_filepath, **kwargs)
    else:
        raise ValueError("Unsupported file type.")

    timeseries.rename(columns=field_mapping, inplace=True)

    if constant_field_values:
        for field, value in constant_field_values.items():
            timeseries[field] = value

    timeseries = timeseries[field_mapping.values()]

    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    timeseries.to_parquet(out_filepath)


def convert_timeseries(
    in_path: Union[str, Path],
    out_path: Union[str, Path],
    field_mapping: dict = None,
    constant_field_values: dict = None,
    pattern: str = "**/*.parquet",
    **kwargs
):
    """Convert timeseries data to parquet format.

    Parameters
    ----------
    in_path : Union[str, Path]
        The input file path.
    out_path : Union[str, Path]
        The output file path.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    constant_field_values : dict, optional
        A dictionary mapping field names to constant values.
        format: {field_name: value}
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv() or pd.read_parquet().

    Can convert CSV or Parquet files.

    """
    in_path = Path(in_path)
    out_path = Path(out_path)
    logger.info(f"Converting timeseries data: {in_path}")

    default_field_mapping = {}
    for field in Timeseries.get_field_names():
        if field not in default_field_mapping.values():
            default_field_mapping[field] = field

    if field_mapping:
        logger.debug("Merging user field_mapping with default field mapping.")
        field_mapping = merge_field_mappings(
            default_field_mapping,
            field_mapping
        )
    else:
        logger.debug("Using default field mapping.")
        field_mapping = default_field_mapping

    # verify constant_field_values keys are in field_mapping values
    if constant_field_values:
        validate_constant_values_dict(
            constant_field_values,
            field_mapping.values()
        )

    files_converted = 0
    if in_path.is_dir():
        # recursively convert all files in directory
        logger.info(f"Recursively converting all files in {in_path}/{pattern}")
        for in_filepath in in_path.glob(f"{pattern}"):
            relative_name = in_filepath.relative_to(in_path)
            out_filepath = Path(out_path, relative_name)
            out_filepath = out_filepath.with_suffix(".parquet")
            convert_single_timeseries(
                in_filepath,
                out_filepath,
                field_mapping,
                constant_field_values,
                **kwargs
            )
            files_converted += 1
    else:
        out_filepath = Path(out_path, in_path.name)
        out_filepath = out_filepath.with_suffix(".parquet")
        convert_single_timeseries(
            in_path,
            out_filepath,
            field_mapping,
            constant_field_values,
            **kwargs
        )
        files_converted += 1
    logger.info(f"Converted {files_converted} files.")


def validate_and_insert_single_timeseries(
    conn: duckdb.DuckDBPyConnection,
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    timeseries_type: str,
):
    """Read a file and insert into the primary_timeseries table."""
    logger.info(f"Reading and inserting timeseries data from {in_filepath}")
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
            reference_time::DATETIME as reference_time,
            value_time::DATETIME as value_time,
            configuration_name::VARCHAR as configuration_name,
            unit_name::VARCHAR as unit_name,
            variable_name::VARCHAR as variable_name,
            value::FLOAT as value,
            location_id::VARCHAR as location_id
        FROM read_parquet('{in_filepath}');
    """)
    conn.sql(f"""
        COPY {table} TO '{out_filepath}';
    """)
    conn.sql(f"TRUNCATE TABLE {table};")


def validate_and_insert_timeseries(
    in_path: Union[str, Path],
    dataset_path: Union[str, Path],
    timeseries_type: str,
    pattern: str = "**/*.parquet",
):
    """Validate and insert primary timeseries data.

    Parameters
    ----------
    in_path : Union[str, Path]
        Directory path or file path to the primary timeseries data.
    dataset_path : Union[str, Path]
        Path to the dataset.
    timeseries_type : str
        The type of timeseries data.
        Valid values: "primary", "secondary"
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    """
    in_path = Path(in_path)
    logger.info(f"Validating and inserting timeseries data from {in_path}")

    if not validate_dataset_structure(dataset_path):
        raise ValueError("Database structure is not valid.")

    # setup validation database
    conn = duckdb.connect()

    # create tables if they do not exist
    create_database_tables(conn)

    # insert domains
    load_units_from_dataset(conn, dataset_path)
    load_configurations_from_dataset(conn, dataset_path)
    load_variables_from_dataset(conn, dataset_path)

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

    if in_path.is_dir():
        # recursively convert all files in directory
        logger.info(
            "Recursively validating and inserting all files "
            f"in {in_path}/{pattern}"
        )
        for in_filepath in in_path.glob(f"{pattern}"):
            relative_path = in_filepath.relative_to(in_path)
            out_filepath = Path(timeseries_dir, relative_path)
            validate_and_insert_single_timeseries(
                conn,
                in_filepath,
                out_filepath,
                timeseries_type
            )
    else:
        out_filepath = Path(timeseries_dir, in_path.name)
        validate_and_insert_single_timeseries(
            conn,
            in_path,
            out_filepath,
            timeseries_type,
        )
