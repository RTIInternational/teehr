"""Module for importing location crosswalks from a file."""
from typing import Union
from pathlib import Path
from teehr.loading.utils import (
    validate_dataset_structure,
    read_and_convert_netcdf_to_df
)
from teehr.models.tables import LocationCrosswalk
from teehr.loading.utils import merge_field_mappings
import logging
import pandas as pd

import pandera.pyspark as pa
import pyspark.sql.types as T

logger = logging.getLogger(__name__)


def convert_single_location_crosswalks(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    field_mapping: dict,
    **kwargs
):
    """Convert location_crosswalks data to parquet format.

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

    logger.info(f"Converting location crosswalks data from: {in_filepath}")

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        location_crosswalks = pd.read_parquet(in_filepath, **kwargs)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        location_crosswalks = pd.read_csv(in_filepath, **kwargs)
    elif in_filepath.suffix == ".nc":
        # read and convert netcdf file
        location_crosswalks = read_and_convert_netcdf_to_df(
            in_filepath,
            field_mapping,
            **kwargs
        )
    else:
        raise ValueError("Unsupported file type.")

    # rename fields if field_mapping provided
    location_crosswalks.rename(columns=field_mapping, inplace=True)

    # make sure dataframe only contains required fields
    location_crosswalks = location_crosswalks[field_mapping.values()]

    # write to parquet
    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    location_crosswalks.to_parquet(out_filepath)


def convert_location_crosswalks(
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
    logger.info(f"Converting crosswalks data: {in_path}")

    default_field_mapping = {}
    for field in LocationCrosswalk.get_field_names():
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
            convert_single_location_crosswalks(
                in_filepath,
                out_filepath,
                field_mapping,
                **kwargs
            )
            files_converted += 1
    else:
        out_filepath = Path(out_dirpath, in_path.name)
        out_filepath = out_filepath.with_suffix(".parquet")
        convert_single_location_crosswalks(
            in_path,
            out_filepath,
            field_mapping,
            **kwargs
        )
        files_converted += 1
    logger.info(f"Converted {files_converted} files.")


def validate_and_insert_location_crosswalks(
    ev,
    in_path: Union[str, Path],
    pattern: str = "**/*.parquet",
):
    """Validate and insert location crosswalks data."""
    logger.info(
        f"Validating and inserting location crosswalks data from {in_path}"
    )

    if not validate_dataset_structure(ev.dataset_dir):
        raise ValueError("Database structure is not valid.")

    if not validate_dataset_structure(ev.dataset_dir):
        raise ValueError("Database structure is not valid.")

    location_ids = ev.locations.distinct_values("id")

    if in_path.is_dir():
        files = [str(f) for f in in_path.glob(f"{pattern}")]
    else:
        files = [str(in_path)]

    loc_xwalks = (ev.spark.read.format("parquet").load(files))

    # define schema
    schema = pa.DataFrameSchema(
        columns={
            "primary_location_id": pa.Column(
                T.StringType,
                pa.Check.isin(location_ids),
                coerce=True
            ),
            "secondary_location_id": pa.Column(
                T.StringType,
                coerce=True
            )
        },
        strict=True
    )
    validated_loc_xwalks = schema(loc_xwalks.select(*schema.columns))

    errors = validated_loc_xwalks.pandera.errors

    if len(errors) > 0:
        raise ValueError(f"Validation errors: {errors}")

    loc_xwalks_dir = ev.location_crosswalks_dir
    loc_xwalks_dir.mkdir(parents=True, exist_ok=True)

    (
        validated_loc_xwalks
        .select(list(schema.columns.keys()))
        .write
        # .partitionBy("configuration_name", "variable_name")
        .format("parquet")
        .mode("overwrite")
        .save(str(loc_xwalks_dir))
    )