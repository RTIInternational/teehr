"""Module defining shared functions for processing NWM point data."""
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import re
import logging

import numpy as np
import pandas as pd
import pyarrow as pa

from teehr.fetching.utils import (
    write_timeseries_parquet_file,
    split_dataframe,
    format_nwm_configuration_metadata,
    parse_nwm_json_paths,
    combine_and_open_kerchunk_refs,
)
from teehr.fetching.models.utils import TimeseriesTypeEnum
from teehr.fetching.const import (
    VALUE,
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME,
    MEMBER,
)

logger = logging.getLogger(__name__)


def process_chunk_of_files(
    df: pd.DataFrame,
    location_ids: Iterable[int],
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
    process_by_z_hour: bool,
    ignore_missing_file: bool,
    overwrite_output: bool,
    nwm_version: str,
    variable_mapper: Dict[str, Dict[str, Dict[str, str]]],
    timeseries_type: TimeseriesTypeEnum,
    drop_overlapping_assimilation_values: bool,
):
    """Assemble a table for a chunk of NWM files."""
    location_ids = np.array(location_ids).astype(int)

    schema = pa.schema(
        [
            (VALUE, pa.float32()),
            (REFERENCE_TIME, pa.timestamp("ms")),
            (LOCATION_ID, pa.string()),
            (VALUE_TIME, pa.timestamp("ms")),
            (CONFIGURATION_NAME, pa.string()),
            (VARIABLE_NAME, pa.string()),
            (UNIT_NAME, pa.string()),
            (MEMBER, pa.string())
        ]
    )
    valid_paths = [fp for fp in df.filepath.tolist() if fp is not None]
    df_valid = df[[fp is not None for fp in df.filepath.tolist()]].reset_index(drop=True)

    if not valid_paths:
        raise FileNotFoundError(
            "No NWM files for specified input configuration were found in GCS!"
        )

    ds, read_mask = combine_and_open_kerchunk_refs(
        json_paths=valid_paths,
        ignore_missing_file=ignore_missing_file,
        storage_options={"target_options": {"anon": True}},
    )
    df_valid = df_valid[read_mask].reset_index(drop=True)

    try:
        ds = ds.sel(feature_id=location_ids)
    except KeyError as e:
        missing = np.setdiff1d(location_ids, ds.feature_id.values.astype(int))
        raise ValueError(
            f"{missing.size} of {len(location_ids)} location_ids not found in the "
            f"NWM '{configuration}' output: {missing[:10].tolist()}"
        ) from e

    vals = ds[variable_name].astype("float32").values
    nwm_units = ds[variable_name].units
    n_files, n_locations = vals.shape

    if variable_mapper is None:
        teehr_variable_name = variable_name
        teehr_units = nwm_units
    else:
        teehr_variable_name = variable_mapper[VARIABLE_NAME].get(
            variable_name, {}
        ).get("name", variable_name)
        teehr_units = variable_mapper[UNIT_NAME].get(nwm_units, {}).get("name", nwm_units)

    ref_times = [
        pd.to_datetime(r.day) + pd.to_timedelta(int(r.z_hour[1:3]), unit="h")
        for r in df_valid.itertuples()
    ]
    ref_times_arr = np.repeat(ref_times, n_locations)
    valid_times_arr = np.repeat(ds.time.values, n_locations)
    teehr_location_ids = [
        f"{nwm_version}-{fid}" for fid in ds.feature_id.values.astype(int)
    ]
    location_ids_tiled = np.tile(teehr_location_ids, n_files)

    teehr_config = format_nwm_configuration_metadata(
        nwm_config_name=configuration,
        nwm_version=nwm_version,
    )
    num_vals = vals.size

    output_table = pa.table(
        {
            VALUE: vals.flatten(),
            REFERENCE_TIME: ref_times_arr,
            LOCATION_ID: location_ids_tiled,
            VALUE_TIME: valid_times_arr,
            CONFIGURATION_NAME: num_vals * [teehr_config["name"]],
            VARIABLE_NAME: num_vals * [teehr_variable_name],
            UNIT_NAME: num_vals * [teehr_units],
            MEMBER: num_vals * [teehr_config["member"]],
        },
        schema=schema,
    )

    df.sort_values(by="filepath", inplace=True)
    if process_by_z_hour:
        row = df.iloc[0]
        filename = f"{row.day}T{row.z_hour[1:3]}.parquet"
    else:
        # Use start and end dates including forecast hour or t-minus hour (assimilation)
        # for the output file name.
        if "assim" in configuration:
            start_tm_hour = re.search(r'\.tm(\d+)\.', df.filepath.iloc[0]).group(1)
            end_tm_hour = re.search(r'\.tm(\d+)\.', df.filepath.iloc[-1]).group(1)
            start = f"{df.day.iloc[0]}T{df.z_hour.iloc[0][1:3]}M{start_tm_hour}"
            end = f"{df.day.iloc[-1]}T{df.z_hour.iloc[-1][1:3]}M{end_tm_hour}"
        else:
            start_forecast_hour = re.search(r'\.f(\d+)\.', df.filepath.iloc[0]).group(1)
            end_forecast_hour = re.search(r'\.f(\d+)\.', df.filepath.iloc[-1]).group(1)
            start = f"{df.day.iloc[0]}T{df.z_hour.iloc[0][1:3]}F{start_forecast_hour}"
            end = f"{df.day.iloc[-1]}T{df.z_hour.iloc[-1][1:3]}F{end_forecast_hour}"
        filename = f"{start}_{end}.parquet"

    if drop_overlapping_assimilation_values and "assim" in configuration:
        # Set reference_time to NaT for assimilation values
        df_output = output_table.to_pandas()
        df_output.loc[:, REFERENCE_TIME] = pd.NaT
        output_table = pa.Table.from_pandas(df_output, schema=schema)

    write_timeseries_parquet_file(
        Path(output_parquet_dir, filename),
        overwrite_output,
        output_table,
        timeseries_type
    )


def fetch_and_format_nwm_points(
    file_paths: List[Optional[str]],
    location_ids: Iterable[int],
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
    process_by_z_hour: bool,
    stepsize: int,
    ignore_missing_file: bool,
    overwrite_output: bool,
    nwm_version: str,
    variable_mapper: Dict[str, Dict[str, Dict[str, str]]],
    timeseries_type: TimeseriesTypeEnum,
    drop_overlapping_assimilation_values: bool,
):
    """Fetch NWM point data and save as parquet files.

    Accepts a list of kerchunk reference file paths (S3/local .json, or local .parq)
    as produced by ``generate_json_paths``. ``None`` entries are filtered out before
    processing. Each chunk is combined into a single xarray Dataset via kerchunk.
    Intended to be refactored to use VirtualiZarr in a future release.

    Parameters
    ----------
    file_paths : List[Optional[str]]
        Resolved file paths from ``generate_json_paths``. May contain
        ``None`` entries for files that should be skipped.
    location_ids : Iterable[int]
        Array specifying NWM IDs of interest.
    configuration : str
        NWM forecast category.
    variable_name : str
        Name of the NWM data variable to download.
    output_parquet_dir : str
        Path to the directory for the final parquet files.
    process_by_z_hour : bool
        A boolean flag that determines the method of grouping files
        for processing.
    stepsize : int
        The number of json files to process at one time.
    ignore_missing_file : bool
        Flag specifying whether or not to fail if a missing NWM
        file is encountered
        True = skip and continue
        False = fail.
    overwrite_output : bool
        Flag specifying whether or not to overwrite output files if
        they already exist.  True = overwrite; False = fail.
    nwm_version : str
        Specified NWM version.
    variable_mapper : Dict[str, Dict[str, Dict[str, str]]]
        A mapping dictionary for variable names and units.
    timeseries_type : TimeseriesTypeEnum
        The type of timeseries being processed.
    drop_overlapping_assimilation_values : bool
        Whether to drop assimilation values that overlap in value_time.
    """
    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    # Filter None entries (files skipped for remote mode with no S3 JSON)
    non_null_paths = [p for p in file_paths if p is not None]
    if not non_null_paths:
        raise FileNotFoundError(
            "No NWM files could be resolved for the given configuration."
        )

    df_refs = parse_nwm_json_paths(non_null_paths)

    if process_by_z_hour:
        # Option #1. Groupby day and z_hour
        gps = df_refs.groupby(["day", "z_hour"])
        dfs = [df for _, df in gps]
    else:
        # Option #2. Chunk by some number of files
        dfs = split_dataframe(df_refs, stepsize)

    logger.info(f"Processing {len(dfs)} chunks of files for configuration: {configuration}, variable: {variable_name}.")

    for df in dfs:
        process_chunk_of_files(
            df,
            location_ids,
            configuration,
            variable_name,
            output_parquet_dir,
            process_by_z_hour,
            ignore_missing_file,
            overwrite_output,
            nwm_version,
            variable_mapper,
            timeseries_type,
            drop_overlapping_assimilation_values,
        )
