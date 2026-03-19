"""Convert and insert timeseries data into the dataset."""
from typing import Union
from pathlib import Path
import pandas as pd
from teehr.loading.utils import (
    read_and_convert_netcdf_to_df,
    read_and_convert_xml_to_df
)

import logging

logger = logging.getLogger(__name__)


def convert_single_timeseries(
    in_filepath: Union[str, Path],
    field_mapping: dict,
    **kwargs
) -> pd.DataFrame:
    """Convert timeseries data to a pandas DataFrame.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    field_mapping : dict
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv(), pd.read_parquet(), or xr.open_dataset().

    Returns
    -------
    pd.DataFrame

    Notes
    -----
    The input file can be in CSV, Parquet, NetCDF, or XML format.
    The field_mapping is used to rename the columns in the resulting DataFrame
    to match the TEEHR data model. The function will read the file, convert it
    to a DataFrame, rename the columns based on the field_mapping, and return
    the resulting DataFrame.
    """
    in_filepath = Path(in_filepath)

    logger.info(f"Converting timeseries data from: {in_filepath}")

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        timeseries = pd.read_parquet(in_filepath, **kwargs)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        timeseries = pd.read_csv(in_filepath, **kwargs)
    elif in_filepath.suffix == ".nc":
        # read and convert netcdf file
        timeseries = read_and_convert_netcdf_to_df(
            in_filepath,
            field_mapping,
            **kwargs
        )
    elif in_filepath.suffix == ".xml":
        # read and convert xml file
        timeseries = read_and_convert_xml_to_df(
            in_filepath,
            field_mapping,
            **kwargs
        )
    else:
        raise ValueError("Unsupported file type.")

    timeseries.rename(columns=field_mapping, inplace=True)

    return timeseries
