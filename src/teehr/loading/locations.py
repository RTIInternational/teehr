"""Module for importing locations from a file."""
from typing import Union
from pathlib import Path
import teehr.loading.utils as tpu
import logging

logger = logging.getLogger(__name__)


def convert_single_locations(
    in_filepath: Union[str, Path],
    field_mapping: dict,
    **kwargs
):
    """Import locations from a file.

    This function reads a geodata file, formats for TEEHR,
    converts to geoparquet and writes to disk..

    Parameters
    ----------
    in_filepath : Union[str, Path]
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
        f"Converting locations from {in_filepath}."
    )

    # read file
    locations = tpu.read_spatial_file(in_filepath, **kwargs)

    # convert to EPSG:4326
    locations.to_crs("EPSG:4326", inplace=True)

    locations.rename(columns=field_mapping, inplace=True)

    return locations
