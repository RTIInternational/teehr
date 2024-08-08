"""Module for importing locations from a file."""
from typing import Union
from pathlib import Path
import teehr.pre.utils as tpu
import duckdb
from teehr.pre.duckdb_utils import (
    create_database_tables,
    insert_locations,
)
from teehr.pre.utils import (
    validate_dataset_structure,
)
from teehr.pre.utils import merge_field_mappings
import teehr.const as const

import logging

logger = logging.getLogger(__name__)


def convert_locations(
        input_filepath: Union[str, Path],
        output_filepath: Union[str, Path],
        field_mapping: dict = None,
        **kwargs
):
    """Import locations from a file.

    This function reads a geodata file, formats for TEEHR,
    converts to geoparquet and writes to disk..

    Parameters
    ----------
    input_filepath : Union[str, Path]
        The input file path.
    output_filepath : Union[str, Path]
        The output file path.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    **kwargs
        Additional keyword arguments are passed to GeoPandas read_file().

    Returns
    -------
    None

    field_mapping = {"id_field": "id", "name_field": "name"}
    Note, not consistent with the field_mapping in pre/timeseries.py

    Future: Add support for writing attr to attr table.
    """
    logger.info(
        f"Converting locations from {input_filepath} to {output_filepath}."
    )

    # read file
    gdf = tpu.read_spatial_file(input_filepath, **kwargs)

    # convert to EPSG:4326
    gdf.to_crs("EPSG:4326", inplace=True)

    # rename fields if field_mapping provided
    default_field_mapping = {
        "id": "id",
        "name": "name",
        "geometry": "geometry"
    }
    if field_mapping:
        logger.debug("Merging user field_mapping with default field mapping.")
        field_mapping = merge_field_mappings(
            default_field_mapping,
            field_mapping
        )
        gdf.rename(columns=field_mapping, inplace=True)

    # check that gdf contains required fields
    required_fields = ["id", "name", "geometry"]
    for field in required_fields:
        if field not in gdf.columns:
            raise KeyError(f"Field {field} not found in input file.")

    # ensure fields are correct type
    gdf["id"] = gdf["id"].astype(str)
    gdf["name"] = gdf["name"].astype(str)

    # write to parquet
    output_filepath = Path(output_filepath)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    gdf[required_fields].to_parquet(output_filepath)

    logger.info(f"Locations written to {output_filepath}.")


def validate_and_insert_locations(
    input_filepath: Union[str, Path],
    dataset_dir: Union[str, Path]
):
    """Validate locations and copy to database."""
    logger.info(f"Validating locations from {input_filepath}")

    output_filename = Path(input_filepath).name

    if not validate_dataset_structure(dataset_dir):
        raise ValueError("Database structure is not valid.")

    output_filepath = Path(dataset_dir, const.LOCATIONS_DIR, output_filename)

    conn = duckdb.connect()

    create_database_tables(conn)
    insert_locations(conn, input_filepath)

    conn.sql(f"""
        COPY (
            SELECT id, name, ST_AsWKB(geometry) as geometry FROM locations
        ) TO '{output_filepath}';
    """)
    logger.info(f"Locations copied to {output_filepath}.")
