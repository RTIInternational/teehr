"""Module for importing configuration pairs from a file."""
from typing import Union
from pathlib import Path
from teehr.loading.utils import single_file_to_dataframe
import pandas as pd


def convert_single_configuration_pairs(
    in_filepath: Union[str, Path],
    field_mapping: dict,
    **kwargs
) -> pd.DataFrame:
    """Convert configuration_pairs data to a pandas DataFrame.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    field_mapping : dict
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv() or pd.read_parquet().

    Returns
    -------
    pd.DataFrame
    """
    return single_file_to_dataframe(
        in_filepath=in_filepath,
        field_mapping=field_mapping,
        table_name="configuration_pairs",
        **kwargs
    )
