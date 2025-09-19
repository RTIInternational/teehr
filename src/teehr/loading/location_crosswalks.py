"""Module for importing location crosswalks from a file."""
from typing import Union
from pathlib import Path
from teehr.loading.utils import read_and_convert_netcdf_to_df
import pandas as pd

import logging

logger = logging.getLogger(__name__)


def convert_single_location_crosswalks(
    in_filepath: Union[str, Path],
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

    return location_crosswalks

