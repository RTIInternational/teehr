"""Convert and insert timeseries data into the dataset."""
from typing import Union
import concurrent.futures
from pathlib import Path
import pandas as pd
from pyspark.sql.functions import lit
from teehr.loading.utils import (
    merge_field_mappings,
    validate_constant_values_dict,
    read_and_convert_netcdf_to_df,
    read_and_convert_xml_to_df
    # convert_datetime_ns_to_ms
)
import teehr.models.pandera_dataframe_schemas as schemas
from teehr.models.table_enums import TableWriteEnum
from teehr.const import MAX_CPUS

import logging

logger = logging.getLogger(__name__)

# The target file size in bytes of cached parquet files.
TARGET_FILE_SIZE = 128 * 1e6

# The correction factor adjusts for differences in
# in-memory and on-disk sizes. Do not change.
CORRECTION_FACTOR = 0.735


def convert_single_timeseries(
    in_filepath: Union[str, Path],
    out_filepath: Union[str, Path, None],
    field_mapping: dict,
    constant_field_values: dict = None,
    timeseries_type: str = None,
    return_dataframe=False,
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

    if return_dataframe:
        logger.info(f"Returning dataframe for: {in_filepath}")
        return validated_df.copy()

    if out_filepath is None:
        raise ValueError("Output file path is required.")
    out_filepath = Path(out_filepath)
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
    max_workers: int = MAX_CPUS,
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
    max_workers : int, optional
        The maximum number of workers to use for parallel processing.
        If None, the default value is used.
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

    if in_path.is_dir():
        filepaths = sorted(list(in_path.glob(f"{pattern}")))
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for in_filepath in filepaths:
                futures.append(
                    executor.submit(
                        convert_single_timeseries,
                        in_filepath,
                        None,
                        field_mapping,
                        constant_field_values,
                        timeseries_type=timeseries_type,
                        return_dataframe=True,
                        **kwargs
                    )
                )
            output_dfs = []
            total_size = 0
            files_converted = 0
            starting_filepath = filepaths[files_converted]
            for future in concurrent.futures.as_completed(futures):
                tmp_df = future.result()
                total_size += tmp_df.memory_usage().sum()
                output_dfs.append(tmp_df)
                if (total_size * CORRECTION_FACTOR >= TARGET_FILE_SIZE) | \
                        (files_converted == len(futures) - 1):
                    df = pd.concat(output_dfs, ignore_index=True)
                    ending_filepath = filepaths[files_converted]
                    output_filepath = Path(f"{starting_filepath.stem}_to_{ending_filepath.stem}").with_suffix(".parquet")
                    out_filepath = Path(out_path, output_filepath)
                    out_filepath.parent.mkdir(parents=True, exist_ok=True)
                    df.to_parquet(out_filepath)
                    output_dfs = []
                    total_size = 0
                    starting_filepath = filepaths[files_converted]
                files_converted += 1
    else:
        files_converted = 0
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
    write_mode: TableWriteEnum = "append",
    drop_duplicates: bool = True,
    drop_overlapping_assimilation_values: bool = False
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
    write_mode : TableWriteEnum, optional (default: "append")
        The write mode for the table. Options are "append" or "upsert".
        If "append", the Evaluation table will be appended with new data
        that does not already exist.
        If "upsert", existing data will be replaced and new data that
        does not exist will be appended.
    drop_duplicates : bool, optional (default: True)
        Whether to drop duplicates in the dataframe before writing
        to the table.
    drop_overlapping_assimilation_values: Optional[bool] = True
        Whether to drop overlapping assimilation values. Default is True.
        If True, values that overlap in value_time are dropped, keeping those with
        the most recent reference_time. In this case, all reference_time values
        are set to None. If False, overlapping values are kept and reference_time
        is retained.
    """ # noqa
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

    if drop_overlapping_assimilation_values:
        df = df.withColumn("reference_time", lit(None))

    # Validate using the _validate() method
    validated_df = table._validate(
        df=df,
        drop_duplicates=drop_duplicates
    )

    # Write to the table
    table._write_spark_df(
        validated_df,
        write_mode=write_mode
    )

    # Reload the table
    table._load_table()
