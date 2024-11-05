"""Module for importing locations from a file."""
from typing import Union
from pathlib import Path
import teehr.loading.utils as tpu
import geopandas as gpd
import pandera as pa
from teehr.loading.utils import (
    validate_dataset_structure,
)
from teehr.models.tables import Location
from teehr.loading.utils import merge_field_mappings

import logging

logger = logging.getLogger(__name__)


def convert_single_locations(
        input_filepath: Union[str, Path],
        output_filepath: Union[str, Path],
        field_mapping: dict,
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
        f"Converting locations from {input_filepath}."
    )

    # read file
    locations = tpu.read_spatial_file(input_filepath, **kwargs)

    # convert to EPSG:4326
    locations.to_crs("EPSG:4326", inplace=True)

    locations.rename(columns=field_mapping, inplace=True)

    # ensure fields are correct type
    locations["id"] = locations["id"].astype(str)
    locations["name"] = locations["name"].astype(str)

    # subset to only the fields in the field_mapping
    locations = locations[field_mapping.values()]

    # write to parquet
    output_filepath = Path(output_filepath)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    locations.to_parquet(output_filepath)

    logger.info(f"Locations written to {output_filepath}.")


def convert_locations(
    in_path: Union[str, Path],
    out_dirpath: Union[str, Path],
    field_mapping: dict = None,
    pattern: str = "**/*.parquet",
    **kwargs
):
    """Convert timeseries data to parquet format.

    Parameters
    ----------
    in_path : Union[str, Path]
        The input file path.
    out_dirpath : Union[str, Path]
        The output file path.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    **kwargs
        Additional keyword arguments are passed to GeoPandas read_file().

    Can convert any file format that GeoPandas can read.
    """
    in_path = Path(in_path)
    out_dirpath = Path(out_dirpath)

    default_field_mapping = {}
    for field in Location.get_field_names():
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
            convert_single_locations(
                in_filepath,
                out_filepath,
                field_mapping,
                **kwargs
            )
            files_converted += 1
    else:
        logger.info(f"Converting locations data: {in_path}")
        out_filepath = Path(out_dirpath, in_path.name)
        out_filepath = out_filepath.with_suffix(".parquet")
        convert_single_locations(
            in_path,
            out_filepath,
            field_mapping,
            **kwargs
        )
        files_converted += 1
    logger.info(f"Converted {files_converted} files.")


def validate_and_insert_single_locations(
    input_filepath: Union[str, Path],
    output_filepath: Union[str, Path],
):
    """Validate locations and copy to database."""
    logger.info(f"Validating and inserting locations from {input_filepath}")

    gdf = gpd.read_parquet(input_filepath)

    schema = pa.DataFrameSchema(
        columns={
            "id": pa.Column(str, coerce=True),
            "name": pa.Column(str, coerce=True),
            "geometry": pa.Column("geometry")
        },
        strict="filter"
    )

    validated_gdf = schema.validate(gdf)

    validated_gdf.to_parquet(output_filepath, index=False)

    logger.info(f"Validated locations saved to {output_filepath}.")


def validate_and_insert_locations(
    ev,
    in_path: Union[str, Path],
    pattern: str = "**/*.parquet",
):
    """Validate and insert locations."""
    if not validate_dataset_structure(ev.dataset_dir):
        raise ValueError("Database structure is not valid.")

    locations_dir = ev.locations_dir

    if in_path.is_dir():
        # recursively convert all files in directory
        logger.info(
            "Recursively validating and inserting "
            f"all files in {in_path}/{pattern}"
        )
        for in_filepath in in_path.glob(f"{pattern}"):
            relative_path = in_filepath.relative_to(in_path)
            out_filepath = Path(locations_dir, relative_path)
            validate_and_insert_single_locations(
                in_filepath,
                out_filepath,
            )
    else:
        logger.info(f"Validate and insert locations from: {in_path}")
        out_filepath = Path(locations_dir, in_path.name)
        validate_and_insert_single_locations(
            in_path,
            out_filepath
        )
