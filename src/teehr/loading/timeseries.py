"""Convert and insert timeseries data into the dataset."""
from typing import Union
from pathlib import Path
import pandas as pd
from teehr.loading.utils import (
    merge_field_mappings,
    validate_constant_values_dict,
    read_and_convert_netcdf_to_df,
    read_and_convert_xml_to_df,
    # convert_datetime_ns_to_ms
)
import teehr.models.pandera_dataframe_schemas as schemas

import logging

logger = logging.getLogger(__name__)


def convert_single_timeseries(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path],
    field_mapping: dict,
    constant_field_values: dict = None,
    timeseries_type: str = None,
    **kwargs
):
    """Convert timeseries data to parquet format.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    out_filepath : Union[str, Path]
        The output file path.
    field_mapping : dict
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    constant_field_values : dict, optional
        A dictionary mapping field names to constant values.
        format: {field_name: value}
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv(), pd.read_parquet(), or xr.open_dataset().

    Steps:
    1. Read the file
    2. Rename the columns based on the field_mapping
    3. Add constant field values to dataframe,
        this may result in too many columns.
    4. Subset only the columns in the field_mapping
    5. Write the dataframe to parquet format

    """
    in_filepath = Path(in_filepath)
    out_filepath = Path(out_filepath)

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

    if constant_field_values:
        for field, value in constant_field_values.items():
            timeseries[field] = value

    # timeseries = timeseries[field_mapping.values()]

    if timeseries_type == "primary":
        validated_df = schemas.primary_timeseries_schema(type="pandas").validate(timeseries)
    elif timeseries_type == "secondary":
        validated_df = schemas.secondary_timeseries_schema(type="pandas").validate(timeseries)
    else:
        raise ValueError("Invalid timeseries type.")

    # validated_df = convert_datetime_ns_to_ms(validated_df)

    logger.info(f"Writing timeseries data to: {out_filepath}")
    out_filepath.parent.mkdir(parents=True, exist_ok=True)
    validated_df.to_parquet(out_filepath)


def convert_timeseries(
    in_path: Union[str, Path],
    out_path: Union[str, Path],
    field_mapping: dict = None,
    constant_field_values: dict = None,
    timeseries_type: str = None,
    pattern: str = "**/*.parquet",
    **kwargs
):
    """Convert timeseries data to parquet format.

    Parameters
    ----------
    in_path : Union[str, Path]
        The input file path.
    out_path : Union[str, Path]
        The output file path.
    field_mapping : dict, optional
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    constant_field_values : dict, optional
        A dictionary mapping field names to constant values.
        format: {field_name: value}
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv() or pd.read_parquet().

    Can convert CSV or Parquet files.

    """
    in_path = Path(in_path)
    out_path = Path(out_path)
    logger.info(f"Converting timeseries data: {in_path}")

    default_field_mapping = {}
    if timeseries_type == "primary":
        fields = schemas.primary_timeseries_schema(type="pandas").columns.keys()
    elif timeseries_type == "secondary":
        fields = schemas.secondary_timeseries_schema(type="pandas").columns.keys()
    else:
        raise ValueError("Invalid timeseries type.")

    for field in fields:
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

    # verify constant_field_values keys are in field_mapping values
    if constant_field_values:
        validate_constant_values_dict(
            constant_field_values,
            field_mapping.values()
        )

    files_converted = 0
    if in_path.is_dir():
        # recursively convert all files in directory
        logger.info(f"Recursively converting all files in {in_path}/{pattern}")
        for in_filepath in in_path.glob(f"{pattern}"):
            relative_name = in_filepath.relative_to(in_path)
            out_filepath = Path(out_path, relative_name)
            out_filepath = out_filepath.with_suffix(".parquet")
            convert_single_timeseries(
                in_filepath,
                out_filepath,
                field_mapping,
                constant_field_values,
                timeseries_type=timeseries_type,
                **kwargs
            )
            files_converted += 1
    else:
        out_filepath = Path(out_path, in_path.name)
        out_filepath = out_filepath.with_suffix(".parquet")
        convert_single_timeseries(
            in_path,
            out_filepath,
            field_mapping,
            constant_field_values,
            timeseries_type=timeseries_type,
            **kwargs
        )
        files_converted += 1
    logger.info(f"Converted {files_converted} files.")


def validate_and_insert_timeseries(
    ev,
    in_path: Union[str, Path],
    timeseries_type: str,
    pattern: str = "**/*.parquet",
):
    """Validate and insert primary timeseries data.

    Parameters
    ----------
    ev : Evaluation
        The Evaluation object.
    in_path : Union[str, Path]
        Directory path or file path to the primary timeseries data.
    timeseries_type : str
        The type of timeseries data.
        Valid values: "primary", "secondary"
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    """
    in_path = Path(in_path)
    logger.info(f"Validating and inserting timeseries data from {in_path}")

    if timeseries_type == "primary":
        table = ev.primary_timeseries
    elif timeseries_type == "secondary":
        table = ev.secondary_timeseries
    else:
        raise ValueError("Invalid timeseries type.")

    # Read the converted files to Spark DataFrame
    df = table._read_files(in_path, pattern)

    # Validate using the _validate() method
    validated_df = table._validate(df)

    # Write to the table
    table._write_spark_df(validated_df)

    # Reload the table
    table._load_table()
