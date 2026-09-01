"""Module defining shared functions for processing NWM point data."""
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import re
import logging

import numpy as np
import pandas as pd
import pyarrow as pa
from obspec_utils.registry import ObjectStoreRegistry

from teehr.fetching.utils import (
    write_timeseries_parquet_file,
    split_dataframe,
    format_nwm_configuration_metadata,
    map_variable_and_unit_name,
    parse_nwm_json_paths,
    combine_and_open_kerchunk_refs,
    build_kerchunk_registry,
)
from teehr.fetching.models.utils import TimeseriesTypeEnum
from teehr.utils.concurrency import (
    available_memory,
    map_blocking,
    resolve_budget,
    run_sync,
    set_concurrency,
    use_process_pool,
)
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

# Rough peak for one worker reading a chunk of files. Workers get half of what
# is free so the parent -- which may hold a Spark session and the loaded data --
# keeps the rest.
CHUNK_MEMORY_PER_WORKER = 1200 * 1024**2


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
    registry: Optional[ObjectStoreRegistry] = None,
    max_concurrent_files: Optional[int] = None,
) -> Optional[Path]:
    """Assemble a table for a chunk of NWM files.

    ``registry`` should be a single ObjectStoreRegistry built once (via
    ``build_kerchunk_registry``) covering every file across all chunks in the
    run, so obstore's stores/connection pools are reused across chunks rather
    than rebuilt per chunk. Built fresh from this chunk alone if omitted.

    ``max_concurrent_files`` bounds how many of this chunk's reference files
    are read at once. It defaults to the process-wide budget -- the same number
    ``set_concurrency(io=...)`` sets. Callers running several chunks at the
    same time (mapped Prefect tasks, say) should divide that budget among
    them, since the two levels multiply.

    Returns the path to the parquet file written for this chunk, or ``None``
    if the chunk produced no data.
    """
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
        variable_name=variable_name,
        location_ids=location_ids,
        ignore_missing_file=ignore_missing_file,
        registry=registry,
        max_concurrent_files=max_concurrent_files,
    )
    df_valid = df_valid[read_mask].reset_index(drop=True)

    vals = ds[variable_name].astype("float32").values
    nwm_units = ds[variable_name].units
    n_files, n_locations = vals.shape

    teehr_variable_name, teehr_units = map_variable_and_unit_name(
        variable_name, nwm_units, variable_mapper
    )

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

    return write_timeseries_parquet_file(
        Path(output_parquet_dir, filename),
        overwrite_output,
        output_table,
        timeseries_type
    )


def build_file_chunks(
    file_paths: List[Optional[str]],
    process_by_z_hour: bool,
    stepsize: int,
) -> List[pd.DataFrame]:
    """Group resolved kerchunk reference file paths into chunks for processing.

    Each returned dataframe is a chunk that can be passed to
    ``process_chunk_of_files`` to fetch, format, and write a single output
    parquet file. Exposed separately from ``fetch_and_format_nwm_points`` so
    external callers (e.g. a Prefect flow) can build the chunk list once and
    parallelize calls to ``process_chunk_of_files`` themselves.

    Parameters
    ----------
    file_paths : List[Optional[str]]
        Resolved file paths from ``generate_json_paths``. May contain
        ``None`` entries for files that should be skipped.
    process_by_z_hour : bool
        A boolean flag that determines the method of grouping files
        for processing. True groups by day and z_hour. False chunks
        files sequentially into groups, whose size is determined by
        stepsize.
    stepsize : int
        The number of json files to process at one time. Used if
        process_by_z_hour is set to False.

    Returns
    -------
    List[pd.DataFrame]
        A list of dataframes, each representing one chunk of files to be
        passed to ``process_chunk_of_files``.
    """
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

    return dfs


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
    chunk_workers: Optional[int] = None,
) -> List[Path]:
    """Fetch NWM point data and save as parquet files.

    Accepts reference file paths (local or S3 .json) as produced by
    ``generate_json_paths``; ``None`` entries are filtered out first. Files are
    grouped into chunks, and each chunk is read through VirtualiZarr into one
    xarray Dataset, subset to ``location_ids``, and written as a single parquet
    file.

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
    chunk_workers : Optional[int]
        Number of worker processes used to process chunks of files. Default is
        1, which processes them one at a time. Likely only worth raising when
        fetching a long time period on a machine with many cores. Ignored when
        already running inside a worker process.

    Returns
    -------
    List[Path]
        Paths to the parquet files written, in chunk order. Chunks that
        produced no data are omitted.
    """
    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    dfs = build_file_chunks(file_paths, process_by_z_hour, stepsize)

    budget = resolve_budget()
    # Off by default: measured break-even is ~144 files and no better than
    # sequential past it. Reading each chunk is already parallel across its
    # files, and that work waits on the network rather than the GIL, so extra
    # processes add startup and memory without adding throughput. Kept as an
    # option because that balance depends on the machine and the data.
    workers = chunk_workers if chunk_workers is not None else 1
    memory = available_memory()
    if workers > 1 and memory:
        affordable = max(1, int(memory * 0.5) // CHUNK_MEMORY_PER_WORKER)
        if affordable < workers:
            # Exceeding this gets workers killed with no traceback, so cap it
            # rather than let the run die halfway through.
            logger.warning(
                f"Reducing chunk_workers from {workers} to {affordable}:"
                f" a worker peaks near"
                f" {CHUNK_MEMORY_PER_WORKER // 1024**2}MB and only"
                f" {memory // 1024**2}MB is free."
            )
            workers = affordable
    n_files = int(sum(len(df) for df in dfs))
    in_processes = (
        workers > 1
        and len(dfs) > 1
        and use_process_pool(n_files, workers, min_items=2)
    )
    workers = min(workers, len(dfs)) if in_processes else 1

    logger.info(
        f"Processing {n_files} files in {len(dfs)} chunks for configuration:"
        f" {configuration}, variable: {variable_name}"
        f"{f' across {workers} processes' if in_processes else ''}."
    )
    if in_processes:
        logger.info(
            f"Each process reads up to {max(1, budget.io // workers)} files at"
            f" once using {max(1, budget.cpu // workers)} threads."
        )

    non_null_paths = [path for path in file_paths if path is not None]
    if not non_null_paths:
        raise FileNotFoundError(
            "No NWM files for specified input configuration were found in GCS!"
        )
    if in_processes:
        # Both budgets are split, not just the network one: each worker reads
        # its chunk with its own thread pool, and an undivided cpu budget means
        # workers x cpu threads each holding a parsed reference -- enough memory
        # to get the workers killed. Workers also build their own registries,
        # since obstore pools can't cross a process boundary.
        io_share = max(1, budget.io // workers)
        cpu_share = max(1, budget.cpu // workers)

        def _log_chunk(index: int, filepath: Optional[Path]) -> None:
            logger.info(
                f"Chunk {index + 1} of {len(dfs)}: "
                f"{Path(filepath).name if filepath else 'no data'}"
            )

        output_paths = run_sync(map_blocking(
            process_chunk_of_files,
            dfs,
            workers=workers,
            processes=workers,
            initializer=set_concurrency,
            initargs=(io_share, cpu_share),
            on_complete=_log_chunk,
            args=(
                location_ids,
                configuration,
                variable_name,
                str(output_parquet_dir),
                process_by_z_hour,
                ignore_missing_file,
                overwrite_output,
                nwm_version,
                variable_mapper,
                timeseries_type,
                drop_overlapping_assimilation_values,
                None,
                io_share,
            ),
        ))
    else:
        # Built once and reused across every chunk, so obstore's stores and
        # connection pools are not rebuilt per chunk.
        registry = build_kerchunk_registry(non_null_paths)
        output_paths = []
        for number, df in enumerate(dfs, start=1):
            # Logged before the work, not after: a chunk takes a while, and
            # silence until it finishes looks like a hang.
            logger.info(
                f"Chunk {number} of {len(dfs)}: reading {len(df)} files"
                f" starting {df.day.iloc[0]} {df.z_hour.iloc[0]}"
            )
            filepath = process_chunk_of_files(
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
                registry,
            )
            if filepath is None:
                logger.info(f"Chunk {number} of {len(dfs)} produced no data.")
            else:
                logger.debug(f"Chunk {number} wrote {Path(filepath).name}")
            output_paths.append(filepath)

    written = [path for path in output_paths if path is not None]
    logger.info(f"Wrote {len(written)} files from {len(dfs)} chunks.")

    return written
