"""Module for importing locations from a file."""
from typing import Union
from pathlib import Path
import teehr.pre.utils as tpu
import duckdb

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
    **kwargs
        Additional keyword arguments are passed to GeoPandas read_file().

    Returns
    -------
    None

    field_mapping = {"id_field": "id", "name_field": "name"}

    Future: Add support for writing attr to attr table.
    """
    logger.info(f"Converting locations from {input_filepath} to {output_filepath}.")

    # read file
    gdf = tpu.read_spatial_file(input_filepath, **kwargs)

    # convert to EPSG:4326
    gdf.to_crs("EPSG:4326", inplace=True)

    # rename fields if field_mapping provided
    if field_mapping:
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
    output_filepath: Union[str, Path],
):
    """Validate locations and copy to database."""
    logger.info(f"Validating locations from {input_filepath} and copying to {output_filepath}.")

    conn = duckdb.connect()

    conn.sql("""
        INSTALL spatial;
        LOAD spatial;
        CREATE TABLE IF NOT EXISTS locations (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                geometry GEOMETRY
        );
    """)

    conn.sql(f"""
        INSERT INTO locations
        SELECT id, name, ST_GeomFromWKB(geometry) as geometry
        FROM read_parquet('{input_filepath}');
    """)

    conn.sql(f"""
        COPY locations TO '{output_filepath}';
    """)
    logger.info(f"Locations copied to {output_filepath}.")
