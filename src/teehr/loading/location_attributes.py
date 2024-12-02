"""Module for importing location attributes from a file."""
from typing import Union
from pathlib import Path
from teehr.loading.utils import read_and_convert_netcdf_to_df
import teehr.models.pandera_dataframe_schemas as schemas
from teehr.loading.utils import merge_field_mappings
import pandas as pd
import teehr.models.pandera_dataframe_schemas as schemas

import logging

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
    elif in_filepath.suffix == ".nc":
        # read and convert netcdf file
        location_attributes = read_and_convert_netcdf_to_df(
            in_filepath,
            field_mapping,
            **kwargs
        )
    else:
        raise ValueError("Unsupported file type.")

    # rename fields if field_mapping provided
    location_attributes.rename(columns=field_mapping, inplace=True)

    # validate schema
    schema = schemas.location_attributes_schema(type="pandas")
    validated_df = schema.validate(location_attributes)

    # write to parquet
    logger.info(f"Writing location attributes to: {out_filepath}")
    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_parquet(out_filepath)


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
    for field in schemas.location_attributes_schema(type="pandas").columns.keys():
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
