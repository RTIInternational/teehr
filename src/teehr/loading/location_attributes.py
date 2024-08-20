"""Module for importing location attributes from a file."""
from typing import Union
from pathlib import Path
from teehr.loading.duckdb_sql import (
    create_database_tables,
    insert_locations,
    insert_location_attributes,
    load_attributes_from_dataset
)
from teehr.loading.utils import (
    validate_dataset_structure,
)
from teehr.models.dataset.table_models import LocationAttribute
from teehr.loading.utils import merge_field_mappings
import teehr.const as const
import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def convert_single_location_attributes(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    field_mapping: dict,
    **kwargs
):
    """Convert location_attributes data to parquet format.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    out_filepath : Union[str, Path]
        The output file path.
    field_mapping : dict
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv() or pd.read_parquet().
    """
    in_filepath = Path(in_filepath)
    out_filepath = Path(out_filepath)

    logger.info(f"Converting location attributes data from: {in_filepath}")

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        location_attributes = pd.read_parquet(in_filepath, **kwargs)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        location_attributes = pd.read_csv(in_filepath, **kwargs)
    else:
        raise ValueError("Unsupported file type.")

    # rename fields if field_mapping provided
    location_attributes.rename(columns=field_mapping, inplace=True)

    # make sure dataframe only contains required fields
    location_attributes = location_attributes[field_mapping.values()]

    # write to parquet
    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    location_attributes.to_parquet(out_filepath)


def convert_location_attributes(
    in_path: Union[str, Path],
    out_dirpath: Union[str, Path],
    field_mapping: dict = None,
    pattern: str = "**/*.parquet",
    **kwargs
):
    """Convert crosswalk data to parquet format.

    Parameters
    ----------
    in_path : Union[str, Path]
        The input file or directory path.
    out_dirpath : Union[str, Path]
        The output directory path.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv() or pd.read_parquet().
    """
    in_path = Path(in_path)
    out_dirpath = Path(out_dirpath)
    logger.info(f"Converting attributes data: {in_path}")

    default_field_mapping = {}
    for field in LocationAttribute.get_field_names():
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

    files_converted = 0
    if in_path.is_dir():
        # recursively convert all files in directory
        logger.info(f"Recursively converting all files in {in_path}/{pattern}")
        for in_filepath in in_path.glob(f"{pattern}"):
            relative_name = in_filepath.relative_to(in_path)
            out_filepath = Path(out_dirpath, relative_name)
            out_filepath = out_filepath.with_suffix(".parquet")
            convert_single_location_attributes(
                in_filepath,
                out_filepath,
                field_mapping,
                **kwargs
            )
            files_converted += 1
    else:
        out_filepath = Path(out_dirpath, in_path.name)
        out_filepath = out_filepath.with_suffix(".parquet")
        convert_single_location_attributes(
            in_path,
            out_filepath,
            field_mapping,
            **kwargs
        )
        files_converted += 1
    logger.info(f"Converted {files_converted} files.")


def validate_and_insert_single_location_attributes(
    conn: duckdb.DuckDBPyConnection,
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
):
    """Validate and insert location crosswalk data."""
    logger.info(f"Validating and inserting crosswalk data from {in_filepath}")

    # read and insert provided crosswalk data
    insert_location_attributes(
        conn,
        in_filepath
    )

    conn.sql(f"COPY location_attributes TO '{out_filepath}';")

    conn.sql("TRUNCATE location_attributes;")


def validate_and_insert_location_attributes(
    in_path: Union[str, Path],
    dataset_dir: Union[str, Path],
    pattern: str = "**/*.parquet",
):
    """Validate and insert location attributes data."""
    logger.info(
        f"Validating and inserting location attributes data from {in_path}"
    )

    if not validate_dataset_structure(dataset_dir):
        raise ValueError("Database structure is not valid.")

    # setup validation database
    conn = duckdb.connect()
    create_database_tables(conn)

    insert_locations(
        conn,
        Path(dataset_dir, const.LOCATIONS_DIR)
    )

    load_attributes_from_dataset(conn, dataset_dir)

    location_attributes_dir = Path(dataset_dir, const.LOCATION_ATTRIBUTES_DIR)

    if in_path.is_dir():
        # recursively convert all files in directory
        logger.info(
            "Recursively validating and inserting "
            f"all files in: {in_path}/{pattern}"
        )
        for in_filepath in in_path.glob(f"{pattern}"):
            relative_path = in_filepath.relative_to(in_path)
            out_filepath = Path(location_attributes_dir, relative_path)
            validate_and_insert_single_location_attributes(
                conn,
                in_filepath,
                out_filepath,
            )
    else:
        out_filepath = Path(location_attributes_dir, in_path.name)
        validate_and_insert_single_location_attributes(
            conn,
            in_path,
            out_filepath
        )
