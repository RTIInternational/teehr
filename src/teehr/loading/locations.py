"""Module for importing locations from a file."""
from typing import Union
from pathlib import Path
import teehr.loading.utils as tpu
import teehr.models.pandera_dataframe_schemas as schemas
from teehr.loading.utils import merge_field_mappings
import teehr.models.pandera_dataframe_schemas as schemas
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

    schema = schemas.locations_schema(type="pandas")
    validated_df = schema.validate(locations)

    # write to parquet
    output_filepath = Path(output_filepath)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_parquet(output_filepath)

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
    for field in schemas.locations_schema(type="pandas").columns.keys():
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
