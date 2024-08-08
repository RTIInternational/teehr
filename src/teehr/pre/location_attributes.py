"""Module for importing crosswalks from a file."""
from typing import Union
from pathlib import Path
from teehr.pre.duckdb_utils import (
    create_database_tables,
    insert_location_attributes,
    insert_attributes,
    insert_locations,
)
from teehr.pre.utils import (
    validate_dataset_structure,
)
from teehr.pre.utils import merge_field_mappings
import teehr.const as const
import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def convert_single_location_attributes(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    field_mapping: dict = None
):
    """Convert location attributes data to parquet format.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    out_filepath : Union[str, Path]
        The output file path.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    """
    in_filepath = Path(in_filepath)
    out_filepath = Path(out_filepath)

    default_field_mapping = {
        "location_id": "location_id",
        "attribute_name": "attribute_name",
        "value": "value"
    }

    logger.info(f"Converting location attributes data from {in_filepath}")
    if field_mapping:
        logger.debug("Merging user field_mapping with default field mapping.")
        field_mapping = merge_field_mappings(
            default_field_mapping,
            field_mapping
        )
    else:
        logger.debug("Using default field mapping.")
        field_mapping = default_field_mapping

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        location_attributes = pd.read_parquet(in_filepath)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        location_attributes = pd.read_csv(in_filepath)
    else:
        raise ValueError("Unsupported file type.")

    # rename fields if field_mapping provided
    location_attributes.rename(columns=field_mapping, inplace=True)

    # make sure dataframe only contains required fields
    location_attributes = location_attributes[field_mapping.values()]

    # make sure all dataframe columns are string
    location_attributes = location_attributes.astype(str)

    # write to parquet
    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    location_attributes.to_parquet(out_filepath)


def convert_location_attributes(
    in_path: Union[str, Path],
    out_path: Union[str, Path],
    pattern: str = "**/*.parquet",
    field_mapping: dict = None
):
    """Convert location attributes data to parquet format.

    Parameters
    ----------
    in_path : Union[str, Path]
        The input file path. Can be file or directory.
        Must match type of out_path.
        If directory, all files matching pattern will be converted.
    out_path : Union[str, Path]
        The output file path. Can be file or directory.
        Must match type of in_path.
        If file, must be a parquet file.
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    """
    in_path = Path(in_path)
    out_path = Path(out_path)

    logger.info(f"Converting location attributes data from {in_path}")

    if not in_path.is_dir() == out_path.is_dir():
        raise ValueError(
            "Input and output paths must both be directories or files."
        )

    if out_path.is_file():
        if not out_path.suffix == ".parquet":
            logger.error("Output file must be a parquet file.")
            raise ValueError("Output file must be a parquet file.")

    if in_path.is_dir():
        out_path.mkdir(parents=True, exist_ok=True)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.error(f"No files match pattern '{pattern}' in '{in_path}'.")
            raise FileNotFoundError

    if in_path.is_dir():
        for in_filepath in in_path.glob(pattern):
            dest_part = in_filepath.relative_to(in_path)
            out_filepath = Path(out_path, dest_part).with_suffix(".parquet")
            convert_single_location_attributes(in_filepath, out_filepath, field_mapping)
    else:
        convert_single_location_attributes(in_path, out_path, field_mapping)


def validate_and_insert_location_attributes(
    in_path: Union[str, Path],
    dataset_dir: Union[str, Path],
    pattern: str = "**/*.parquet"
):
    """Validate and insert location_attributes data."""
    in_path = Path(in_path)

    logger.info(
        f"Validating and inserting location_attributes data from {in_path}."
    )

    if not validate_dataset_structure(dataset_dir):
        raise ValueError("Database structure is not valid.")

    # setup validation database
    conn = duckdb.connect()
    create_database_tables(conn)
    insert_locations(conn, Path(dataset_dir, const.LOCATIONS_DIR))
    insert_attributes(conn, dataset_dir)

    if in_path.is_dir():
        if len(list(in_path.glob(pattern))) == 0:
            logger.error(f"No parquet files in {in_path}.")
            raise FileNotFoundError

    if in_path.is_dir():
        for in_filepath in in_path.glob(pattern):
            relative_path = in_filepath.relative_to(in_path)
            insert_location_attributes(
                conn,
                in_filepath
            )

            # export to dataset_dir
            output_filepath = Path(
                dataset_dir, const.LOCATION_ATTRIBUTES_DIR, relative_path
            )
            conn.sql(f"COPY location_attributes TO '{output_filepath}';")
            conn.sql("TRUNCATE location_attributes;")
    else:
        out_filename = in_path.name
        insert_location_attributes(
            conn,
            in_path
        )

        # export to dataset_dir
        output_filepath = Path(
            dataset_dir, const.LOCATION_ATTRIBUTES_DIR, out_filename
        )
        conn.sql(f"COPY location_attributes TO '{output_filepath}';")
        conn.sql("TRUNCATE location_attributes;")


