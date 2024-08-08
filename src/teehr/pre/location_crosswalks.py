"""Module for importing crosswalks from a file."""
from typing import Union
from pathlib import Path
from teehr.pre.duckdb_utils import (
    create_database_tables,
    insert_locations,
    insert_location_crosswalks,
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


def convert_location_crosswalks(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    field_mapping: dict = None
):
    """Convert crosswalk data to parquet format.

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

    logger.info(f"Converting crosswalk data from: {in_filepath}")

    default_field_mapping = {
        "primary_location_id": "primary_location_id",
        "secondary_location_id": "secondary_location_id"
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

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        crosswalks = pd.read_parquet(in_filepath)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        crosswalks = pd.read_csv(in_filepath)
    else:
        raise ValueError("Unsupported file type.")

    # rename fields if field_mapping provided
    crosswalks.rename(columns=field_mapping, inplace=True)

    # make sure dataframe only contains required fields
    crosswalks = crosswalks[field_mapping.values()]

    # write to parquet
    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    crosswalks.to_parquet(out_filepath)


def validate_and_insert_location_crosswalks(
    in_filepath: Union[str, Path],
    dataset_dir: Union[str, Path]
):
    """Validate and insert crosswalk data."""
    in_filepath = Path(in_filepath)
    in_filename = in_filepath.name

    logger.info(f"Validating and inserting crosswalk data from {in_filepath}")

    if not validate_dataset_structure(dataset_dir):
        raise ValueError("Database structure is not valid.")

    # setup validation database
    conn = duckdb.connect()
    create_database_tables(conn)

    # read and insert location data from dataset
    insert_locations(
        conn,
        Path(dataset_dir, const.LOCATIONS_DIR)
    )

    # read and insert provided crosswalk data
    insert_location_crosswalks(
        conn,
        in_filepath
    )

    # export to dataset_dir
    output_filepath = Path(
        dataset_dir, const.LOCATION_CROSSWALKS_DIR, in_filename
    )
    conn.sql(f"COPY location_crosswalks TO '{output_filepath}';")
