"""Module defining common utilities for fetching and processing NWM data."""
from pathlib import Path
from typing import Union, Optional, Iterable, List, Dict, Tuple
from datetime import datetime
from datetime import timedelta
from concurrent.futures import Executor
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
import asyncio
import base64
import logging
import os
import re
import json
import fnmatch
import itertools
import struct
import threading
import warnings
from warnings import warn

import obstore
import ujson  # fast json
from obstore.store import from_url
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.manifests import ManifestStore
from virtualizarr.manifests.manifest import validate_and_normalize_path_to_uri
from virtualizarr.parsers import HDFParser
from virtualizarr.parsers.kerchunk.translator import manifestgroup_from_kerchunk_refs
import zarr
from zarr.errors import UnstableSpecificationWarning
from zarr.storage import ObjectStore
import pandas as pd
import numpy as np
import xarray as xr
import pyarrow as pa
import pandera

from teehr.evaluation.write import Write as writer
from teehr.utils.concurrency import (
    gather_bounded,
    map_blocking,
    resolve_budget,
    run_in_executor,
    run_sync,
    thread_pool,
    use_process_pool,
)
from teehr.fetching.models.utils import (
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.fetching.models.utils import (
    SupportedNWMOperationalVersionsEnum,
    NWMChunkByEnum
)
from teehr.fetching.const import (
    DEFAULT_S3_REGION,
    NWM_BUCKET,
    NWM_HAWAII_VARIABLE_MAPPER,
    NWM_S3_JSON_PATH,
    S3_BUCKET_REGIONS,
    NWM31_START_DATE,
    NWM30_START_DATE,
    NWM21_START_DATE,
    NWM20_START_DATE,
    NWM12_START_DATE,
    NWM_VARIABLE_MAPPER,
    NWM_CONFIGURATION_DESCRIPTIONS,
    NWM_CONFIGURATION_DESCRIPTIONS_BY_VERSION,
    NWM_VERSION_ATTRS,
    NWM_VERSION_ATTR_VALUES,
    NWM_VERSION_BOUNDARIES,
    NWM_VERSION_SWITCHOVER_GRACE,
    UNIT_NAME,
    VARIABLE_NAME
)
import teehr.models.pandera_dataframe_schemas as schemas

TZ_PATTERN = re.compile(r't[0-9]+z')
DAY_PATTERN = re.compile(r'nwm.[0-9]+')

# NWM files carry `crs` as a one-byte char variable, and zarr warns that the
# dtype has no stable Zarr V3 specification every time it builds one -- a few
# thousand times over a large fetch. Nothing here writes zarr arrays: the dtype
# comes from the source files and the variable is dropped immediately after
# reading, so the warning is noise no caller can act on.
warnings.filterwarnings("ignore", category=UnstableSpecificationWarning)

logger = logging.getLogger(__name__)

# Caps the reference-building worker count so it fits in RAM. Worker peaks vary
# with the interpreter and wheel set -- ~440MB on py3.14, ~1180MB in CI -- so
# this covers the larger with room for the parent. Re-measure on the target
# image if the count looks wrong; test_reference_work_memory_is_bounded guards
# only the part teehr controls.
REFERENCE_WORKER_MEMORY = 2000 * 1024**2

# Fewest files worth the ~3s per worker startup; measured break-even.
REFERENCE_BUILD_MIN_ITEMS = 32

# Peak bytes one in-flight file costs, so concurrency can be bounded by memory
# rather than by core count.
#
# Point: a whole-domain chunk decoded to float64. Measured 25MB (v3.0) to 37MB
# with a million locations requested; barely varies with how many are asked
# for, since 3 and 100,000 decode the same chunk.
POINT_READ_MEMORY = 40 * 1024**2

# Grid: scales with the window, which spans a HUC10 to all of CONUS. Two
# float64 copies per cell fits the measurements (283MB for the full
# 3840x4608 grid), floored because even a one-cell window decodes the whole
# source chunk it falls in (8.6MB measured).
GRID_READ_BYTES_PER_CELL = 16
GRID_READ_MEMORY_FLOOR = 16 * 1024**2


def grid_window_memory(n_rows: int, n_cols: int) -> int:
    """Peak bytes reading one gridded window of this size is expected to cost."""
    return max(
        n_rows * n_cols * GRID_READ_BYTES_PER_CELL, GRID_READ_MEMORY_FLOOR
    )


# Requested files under one prefix above which one listing beats one HEAD each.
# Measured on the pre-built reference bucket: a 1728-key listing takes 0.38s,
# 432 heads at io=48 take 1.08s.
LIST_INSTEAD_OF_HEAD_MIN_KEYS = 100

# Requested ids re-checked against a file's own feature_id when that is cheap.
# A mismatched feature_id means a different routing network, so every element
# moves -- a sample catches it.
FEATURE_ID_SAMPLE_SIZE = 64

# obstore's default backoff spends its 10 retries in ~2s, which is too fast for
# a connection an object store resets under load: the same request usually
# succeeds seconds later. A 1s initial backoff spreads the same retries over
# tens of seconds, still bounded by the 3 minute retry_timeout default.
REMOTE_RETRY_CONFIG = {"backoff": {"init_backoff": timedelta(seconds=1)}}

# Coordinates embedded in a reference rather than pointed at: scalars and the
# grid axes are small, and inlining them saves the read path a round trip per
# file. feature_id is deliberately absent -- 2.7M values for CONUS -- and is
# read from the source file when a chunk is opened. Names absent from a given
# file are ignored, so one list covers point and gridded output.
INLINE_COORDINATES = ["time", "reference_time", "x", "y"]


def start_on_z_hour(
    start_z_hour: int,
    gcs_component_paths: List[str]
):
    """Limit the start date to a specified z-hour."""
    logger.info(f"Limiting the start date to z-hour: {start_z_hour}.")
    return_list = []
    for i, path in enumerate(gcs_component_paths):
        res = re.search(DAY_PATTERN, path).group()
        day = res.split(".")[1]
        tz = re.search(TZ_PATTERN, path).group()
        if i == 0:
            formatted_start_date = day
        if day == formatted_start_date:
            if int(tz[1:-1]) >= start_z_hour:
                return_list.append(path)
        else:
            return_list.append(path)
    return return_list


def end_on_z_hour(
    end_z_hour: int,
    gcs_component_paths: List[str]
):
    """Limit the end date to a specified z-hour."""
    logger.info(f"Limiting the end date to z-hour: {end_z_hour}.")
    return_list = []
    reversed_list = sorted(gcs_component_paths, reverse=True)
    for i, path in enumerate(reversed_list):
        res = re.search(DAY_PATTERN, path).group()
        day = res.split(".")[1]
        tz = re.search(TZ_PATTERN, path).group()
        if i == 0:
            formatted_end_date = day
        if day == formatted_end_date:
            if int(tz[1:-1]) <= end_z_hour:
                return_list.append(path)
        else:
            return_list.append(path)
    return sorted(return_list)


def parse_nwm_gcs_paths(
    component_paths: List[str],
    nwm_configuration: str,
) -> pd.DataFrame:
    """Parse the reference and valid times from the paths."""
    logger.debug("Parsing day and z-hour from component paths.")
    tz_pattern = re.compile(r't([0-9]+)z')
    tm_pattern = re.compile(r'tm([0-9]+)')
    parsed_data = []
    for path in component_paths:
        filename = Path(path).name
        res = re.search(DAY_PATTERN, path).group()
        day = res.split(".")[1]
        z_hour = re.search(tz_pattern, filename).group(1)
        tm_hour = re.search(tm_pattern, filename).group(1)
        reference_time = datetime.strptime(day, "%Y%m%d") + timedelta(hours=int(z_hour))
        # Hawaii has 15-minute intervals, so we need to account for that.
        # (Hawaii forcing analysis has hourly intervals)
        if "hawaii" in nwm_configuration and "forcing" not in nwm_configuration:
            value_time = reference_time - timedelta(hours=int(tm_hour[0:2])) - timedelta(minutes=int(tm_hour[2:4]))
        else:
            value_time = reference_time - timedelta(hours=int(tm_hour))
        parsed_data.append({
            "day": day,
            "z_hour": z_hour,
            "tm_hour": tm_hour,
            "filepath": path,
            "value_time": value_time,
            "reference_time": reference_time
        })
    df = pd.DataFrame(parsed_data)
    return df


def remove_overlapping_assim_validtimes(
    parsed_df: pd.DataFrame,
) -> pd.DataFrame:
    """Drop overlapping value_times, keeping most recent reference time."""
    logger.debug("Parsing day and z-hour from component paths.")
    sorted_df = parsed_df.sort_values(by=["reference_time", "value_time"], ascending=True)
    dropped_df = sorted_df.drop_duplicates(
        subset=["value_time"],
        keep="last"
    ).reset_index(drop=True)
    return dropped_df


def parse_nwm_json_paths(
    json_paths: List[str]
) -> pd.DataFrame:
    """Parse the day and z-hour from the json paths, returning a DataFrame."""
    logger.debug("Parsing day and z-hour from json paths.")
    days = []
    z_hours = []
    for path in json_paths:
        filename = Path(path).name
        if path.split(":")[0] == "s3":
            res = re.search(DAY_PATTERN, path).group()
            days.append(res.split(".")[1])
            z_hours.append(re.search(TZ_PATTERN, filename).group())
        else:
            days.append(filename.split(".")[1])
            z_hours.append(filename.split(".")[3])

    return pd.DataFrame(
        {"day": days, "z_hour": z_hours, "filepath": json_paths}
    )


def format_nwm_configuration_metadata(
    nwm_config_name: str,
    nwm_version: str
) -> Dict[str, str]:
    """Format the NWM configuration name and member for the Evaluation.

    Returns a dictionary with the formatted configuration name and member,
    which is parsed from the NWM configuration name if it's an ensemble
    (ie., medium range or long range streamflow).

    Descriptions come from the version's own table where it has one,
    otherwise the shared table.
    """
    ev_member = None
    # Try to parse the member from the configuration name.
    if bool(re.search(r"_mem[0-9]+", nwm_config_name)):
        ev_config_name, ev_member = nwm_config_name.split("_mem")
        ev_config_name = nwm_version + "_" + ev_config_name
        nwm_config_name = re.sub(r'\d+', '', nwm_config_name)
    else:
        ev_config_name = nwm_version + "_" + nwm_config_name
    # Get the config description.
    descriptions = NWM_CONFIGURATION_DESCRIPTIONS_BY_VERSION.get(
        nwm_version, NWM_CONFIGURATION_DESCRIPTIONS
    )
    ev_config_desc = descriptions.get(
        nwm_config_name, "NWM operational forecasts"  # default description
    )
    return {
        "name": ev_config_name,
        "member": ev_member,
        "description": ev_config_desc
    }


def map_variable_and_unit_name(
    variable_name: str,
    nwm_units: str,
    variable_mapper: Optional[Dict[str, Dict[str, Dict[str, str]]]],
) -> Tuple[str, str]:
    """Map an NWM variable/unit name to TEEHR's names via ``variable_mapper``.

    Falls back to the original NWM names for anything not found in the
    mapper, or if no mapper is provided at all (``variable_mapper is None``).
    Shared by point and grid fetching, which otherwise each reimplemented
    this lookup slightly differently.
    """
    if variable_mapper is None:
        return variable_name, nwm_units
    teehr_variable_name = variable_mapper[VARIABLE_NAME].get(
        variable_name, {}
    ).get("name", variable_name)
    teehr_units = variable_mapper[UNIT_NAME].get(nwm_units, {}).get("name", nwm_units)
    return teehr_variable_name, teehr_units


def get_nwm_variable_mapper(nwm_configuration: str) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Return the NWM variable mapper for the given NWM configuration."""
    logger.info(f"Getting schema variable mapper for NWM configuration: {nwm_configuration}.")
    if "hawaii" in nwm_configuration:
        variable_mapper = NWM_HAWAII_VARIABLE_MAPPER
    else:
        variable_mapper = NWM_VARIABLE_MAPPER
    return variable_mapper


def validate_operational_start_end_date(
    nwm_version: str,
    start_date: Union[datetime, pd.Timestamp],
    end_date: Union[datetime, pd.Timestamp]
):
    """Make sure start/end dates work with specified NWM version.

    Compares at z-hour resolution, since a switch lands on a forecast cycle,
    not midnight: v3.0 begins at ``2023-09-19 t12z``, so that day's t00z-t11z
    are still v2.2. Comparing whole days would accept up to 14 hours of the
    neighbouring version. See :data:`NWM_VERSION_BOUNDARIES`.
    """
    logger.debug("Checking dates against NWM version.")

    if end_date < start_date:
        raise ValueError(
            "The end date must be greater than or equal to the start date."
        )

    err_msg = (
        f"The specified start and end dates ({start_date} - {end_date}) "
        f"fall outside {nwm_version} operational data availability."
    )
    v3_err_msg = (
        f"The specified start date ({start_date}) is before the NWM "
        f"v3.0 release date ({NWM30_START_DATE})"
    )

    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm31:
        if start_date < NWM31_START_DATE:
            raise ValueError(
                f"The specified start date ({start_date}) is before the NWM "
                f"v3.1 release date ({NWM31_START_DATE})"
            )
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        if start_date < NWM30_START_DATE:
            raise ValueError(v3_err_msg)
        if end_date >= NWM31_START_DATE:
            raise ValueError(err_msg)
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm22:
        if (end_date >= NWM30_START_DATE) | (start_date < NWM21_START_DATE):
            raise ValueError(err_msg)
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm21:
        if (end_date >= NWM30_START_DATE) | (start_date < NWM21_START_DATE):
            raise ValueError(err_msg)
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm20:
        if (end_date >= NWM21_START_DATE) | (start_date < NWM20_START_DATE):
            raise ValueError(err_msg)
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm12:
        if (end_date >= NWM20_START_DATE) | (start_date < NWM12_START_DATE):
            raise ValueError(err_msg)


def generate_json_paths(
    kerchunk_method: str,
    gcs_component_paths: List[str],
    json_dir: str,
    ignore_missing_file: bool,
    io_concurrency: Optional[int] = None,
    cpu_workers: Optional[int] = None,
) -> List[str]:
    """Generate file paths to Kerchunk reference json files.

    Parameters
    ----------
    kerchunk_method : str
        Specifies the preference in creating Kerchunk reference json files.
    gcs_component_paths : List[str]
        Paths to NWM netcdf files in GCS.
    json_dir : str
        Local directory for caching created json files.
    ignore_missing_file : bool
        Flag specifying whether or not to fail if a missing
        NWM file is encountered.
    io_concurrency : Optional[int]
        Bounds the s3 check for pre-built references.
    cpu_workers : Optional[int]
        Bounds building the references that are missing.

    Returns
    -------
    List[str]
        List of filepaths to json files locally and/or in s3.
    """
    logger.info(f"Generating json paths. kerchunk_method: {kerchunk_method}")

    if kerchunk_method == SupportedKerchunkMethod.local:
        # Create them manually first
        json_paths = build_zarr_references_virtualizarr(
            gcs_component_paths,
            json_dir,
            ignore_missing_file,
            cpu_workers,
        )

    elif kerchunk_method == SupportedKerchunkMethod.remote:
        # Use whatever pre-builts exist, skipping the rest
        s3_path_list = [f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json" for gcs_path in gcs_component_paths]
        file_check_output = check_if_files_exist(s3_path_list, io_concurrency)
        json_paths = [path for path, exists in file_check_output.items() if exists]
        missing_files = [path for path, exists in file_check_output.items() if not exists]
        logger.info(
            f"Mode: {kerchunk_method}. Found {len(json_paths)} pre-built jsons in s3,"
            f" skipping {len(missing_files)} missing files."
        )

    elif kerchunk_method == SupportedKerchunkMethod.auto:
        # Use whatever pre-builts exist, and create the missing
        s3_path_list = [f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json" for gcs_path in gcs_component_paths]
        file_check_output = check_if_files_exist(s3_path_list, io_concurrency)
        json_paths = [path for path, exists in file_check_output.items() if exists]
        missing_files = [path for path, exists in file_check_output.items() if not exists]
        logger.info(
            f"Mode: {kerchunk_method}. Found {len(json_paths)} pre-built jsons in s3,"
            f" building references for {len(missing_files)} files."
        )

        if len(missing_files) > 0:
            # Set back to gcs paths and strip the .json extension
            missing_files = [
                path.replace(NWM_S3_JSON_PATH, "gcs:/").replace(".json", "") for path in missing_files
            ]
            json_paths.extend(
                build_zarr_references_virtualizarr(
                    missing_files,
                    json_dir,
                    ignore_missing_file,
                    cpu_workers,
                )
            )

    return json_paths


def _drop_nan_values(
    df: pd.DataFrame,
    subset_columns=["value"]
) -> pd.DataFrame:
    """Drop NaN values from the timeseries dataframe."""
    if df[subset_columns].isnull().values.any():
        logger.debug(
            "NaN values were encountered, dropping from the dataframe."
        )
        df = df.dropna(subset=subset_columns).reset_index(drop=True)
        if df.index.size == 0:
            return None
    return df


def _write_parquet_atomically(
    df: pd.DataFrame,
    filepath: Path,
    write_schema: pa.Schema,
) -> None:
    """Write a cache file so a partial one can never be left behind.

    Writing straight to ``filepath`` means an interruption -- a cancelled
    chunk, an OOM kill, a Ctrl-C -- leaves a truncated parquet file there. A
    later run would then skip it as already written and fail to read it, or
    quietly load partial data. Writing beside it and renaming into place makes
    the file appear complete or not at all, since rename is atomic within a
    directory.

    Parameters
    ----------
    df : pd.DataFrame
        Validated data to write.
    filepath : Path
        Final destination.
    write_schema : pa.Schema
        Arrow schema to write with.
    """
    tmp = filepath.with_name(f".{filepath.name}.{os.getpid()}.tmp")
    try:
        writer.to_cache(
            source_data=df,
            cache_filepath=tmp,
            write_schema=write_schema,
        )
        os.replace(tmp, filepath)
    finally:
        tmp.unlink(missing_ok=True)


def write_timeseries_parquet_file(
    filepath: Path,
    overwrite_output: bool,
    data: Union[pa.Table, pd.DataFrame],
    timeseries_type: TimeseriesTypeEnum
) -> Optional[Path]:
    """Write the output timeseries parquet file.

    Includes logic controlling whether or not to overwrite an existing file.

    Parameters
    ----------
    filepath : Path
        Path to the output parquet file.
    overwrite_output : bool
        Flag controlling overwrite behavior.
    data : Union[pa.Table, pd.DataFrame]
        The output data as either a dataframe or pyarrow table.

    Returns
    -------
    Optional[Path]
        ``filepath`` if the file holds data afterwards -- whether this call
        wrote it or it already existed and was left alone -- and ``None`` if
        nothing was written because the data was empty or failed validation.
        Callers driving this from a workflow can use the result to load only
        what actually landed.
    """
    logger.debug(f"Writing parquet file: {filepath}")

    # Before the conversion and validation below, all of which is thrown away
    # when the file is already there.
    if filepath.is_file() and not overwrite_output:
        logger.info(
            f"{filepath.name} already exists and overwrite_output=False;"
            " skipping"
        )
        return filepath

    if isinstance(data, pa.Table):
        df = data.to_pandas()
    else:
        df = data

    df = _drop_nan_values(df)

    if df is None:
        logger.warning(
            f"The dataframe is empty after dropping NaN values; "
            f"skipping writing to {filepath.name}."
        )
        return None

    if timeseries_type == TimeseriesTypeEnum.primary:
        schema = schemas.primary_timeseries_schema(type="pandas")
        write_schema = schemas.primary_timeseries_schema(type="arrow")
    elif timeseries_type == TimeseriesTypeEnum.secondary:
        schema = schemas.secondary_timeseries_schema(type="pandas")
        write_schema = schemas.secondary_timeseries_schema(type="arrow")

    try:
        # This is a bit of a workaround until we refactor the fetching code
        schema_cols = schema.columns
        for col_name, col_schema in schema_cols.items():
            if col_name not in df.columns:
                # Check if column is nullable
                is_nullable = getattr(col_schema, 'nullable', True)
                if is_nullable:
                    df[col_name] = None

        validated_df = schema.validate(df, lazy=True)
    except pandera.errors.SchemaErrors as exc:
        msg = json.dumps(exc.message, indent=2)
        logger.error(
            f"Validation error: {msg}"
            f"\nThis file '{filepath}' will be skipped."
        )
        return None

    if filepath.is_file():
        logger.info(f"Overwriting {filepath.name}")

    _write_parquet_atomically(validated_df, filepath, write_schema)

    return filepath


def list_to_np(lst):
    """Convert list to a tuple."""
    return tuple([np.array(a) for a in lst])


def _s3_region(url: str) -> str:
    """Get the region an s3 bucket lives in, from :data:`S3_BUCKET_REGIONS`."""
    bucket = url.split("://", 1)[-1].split("/", 1)[0]
    return S3_BUCKET_REGIONS.get(bucket, DEFAULT_S3_REGION)


def _public_store(url: str):
    """Anonymous store for ``url``, pinned to its bucket's own region.

    Pinning matters wherever teehr runs: without it obstore uses the ambient
    AWS_REGION, and a bucket elsewhere answers with a bare 301 that surfaces
    as "Received redirect without LOCATION".
    """
    kwargs = {"skip_signature": True, "retry_config": REMOTE_RETRY_CONFIG}
    if url.startswith("s3://"):
        kwargs["region"] = _s3_region(url)
    return from_url(url, **kwargs)


def public_zarr_store(url: str) -> ObjectStore:
    """Create a read-only zarr store for a public s3 zarr, backed by obstore.

    Used instead of ``fsspec.get_mapper`` so the retrospective reads go
    through the same object store, region pinning, and retry budget as the
    rest of fetching. obstore fetches chunks concurrently on its own, so
    callers do not need ``chunks=`` (and the dask chunk manager it requires)
    to read a selection in parallel.
    """
    return ObjectStore(_public_store(url), read_only=True)


async def _list_prefix(store, prefix: str) -> List[str]:
    """Every key under ``prefix``, following pagination."""
    return [meta["path"] for meta in await obstore.list(store, prefix).collect_async()]


def _plan_existence_checks(
    file_path_list: List[str],
) -> Tuple[Dict[str, List[str]], List[str], List[str]]:
    """Decide which prefixes to list and which files to check one by one.

    One listing answers for every file under a prefix, and NWM fetches ask
    about hundreds from the same one. Below
    :data:`LIST_INSTEAD_OF_HEAD_MIN_KEYS` a listing costs more than the heads
    it saves, so small prefixes stay file by file.

    Returns the paths grouped by prefix, the prefixes worth listing, and the
    paths to check individually.
    """
    by_prefix: Dict[str, List[str]] = {}
    for path in file_path_list:
        by_prefix.setdefault(path.rsplit("/", 1)[0] + "/", []).append(path)

    listable = [
        prefix for prefix, paths in by_prefix.items()
        if len(paths) >= LIST_INSTEAD_OF_HEAD_MIN_KEYS
    ]
    listed = set(listable)
    heads = [
        path for prefix, paths in by_prefix.items()
        if prefix not in listed for path in paths
    ]
    return by_prefix, listable, heads


async def _check_if_files_exist_async(
    file_path_list: List[str],
    io_concurrency: Optional[int] = None,
) -> Dict[str, bool]:
    """Async implementation backing :func:`check_if_files_exist`."""
    stores: Dict[str, object] = {}

    def _resolve(path: str) -> Tuple[object, str]:
        # e.g. "s3://bucket/some/key.json" -> store for "s3://bucket/", "some/key.json"
        scheme, _, rest = path.partition("://")
        bucket, _, key = rest.partition("/")
        prefix = f"{scheme}://{bucket}/"
        if prefix not in stores:
            stores[prefix] = _public_store(prefix)
        return stores[prefix], key

    limit = resolve_budget(io=io_concurrency).io
    by_prefix, listable, heads = _plan_existence_checks(file_path_list)

    async def _run(item: Tuple[str, str]) -> Tuple[str, str, object]:
        kind, target = item
        store, key = _resolve(target)
        if kind == "list":
            return kind, target, set(await _list_prefix(store, key))
        try:
            await store.head_async(key)
            return kind, target, True
        except FileNotFoundError:
            return kind, target, False

    # Listings and heads share one bounded pass rather than a gather_bounded
    # each: separate semaphores would each allow `limit`, putting twice the
    # intended concurrency on the store.
    work = [("list", prefix) for prefix in listable]
    work += [("head", path) for path in heads]
    outcomes = await gather_bounded(_run, work, limit=limit)

    results: Dict[str, bool] = {}
    for kind, target, outcome in outcomes:
        if kind == "head":
            results[target] = outcome
            continue
        for path in by_prefix[target]:
            results[path] = _resolve(path)[1] in outcome
    # Returned in the order asked for; callers pair it with their path list.
    return {path: results[path] for path in file_path_list}


def check_if_files_exist(
    file_path_list: List[str],
    io_concurrency: Optional[int] = None,
) -> Dict[str, bool]:
    """Check for existence of S3 files.

    Parameters
    ----------
    file_path_list : List[str]
        Remote paths to check.
    io_concurrency : Optional[int]
        Checks to run at once. Defaults to the process-wide budget.
    """
    return run_sync(
        _check_if_files_exist_async(file_path_list, io_concurrency)
    )


def _json_path_to_url(path: str) -> str:
    """Normalize a kerchunk reference path (local or remote) to a URL with a scheme."""
    if path.startswith(("s3://", "gcs://", "gs://", "http://", "https://", "file://")):
        return path
    return Path(path).resolve().as_uri()


def build_kerchunk_registry(json_paths: List[str]) -> ObjectStoreRegistry:
    """Build an ObjectStoreRegistry covering both NWM buckets and any local ref dirs.

    Registers the GCS bucket under both the "gcs://" and "gs://" schemes, since
    kerchunk references built from GCS paths use "gcs://" while obstore/VirtualiZarr
    expect "gs://". Also registers a local filesystem store for each distinct local
    directory present in ``json_paths``, since remote (pre-built) and local (freshly
    built) reference paths can be mixed in the same call.

    Exposed publicly (rather than kept module-private) so a caller processing
    many chunks in one run (e.g. ``fetch_and_format_nwm_points``) can build one
    registry covering every file up front and pass it into each
    ``combine_and_open_kerchunk_refs`` call, instead of a fresh registry (and
    fresh obstore store/connection pool) being constructed per chunk.
    """
    gcs_store = from_url(
        f"gs://{NWM_BUCKET}/",
        skip_signature=True,
        retry_config=REMOTE_RETRY_CONFIG,
    )
    s3_bucket = NWM_S3_JSON_PATH.split("://")[1]
    stores = {
        f"gcs://{NWM_BUCKET}/": gcs_store,
        f"gs://{NWM_BUCKET}/": gcs_store,
        # some kerchunk references encode chunk locations via GCS's public HTTPS
        # frontend rather than the "gcs://" scheme.
        "https://storage.googleapis.com/": from_url(
            "https://storage.googleapis.com/",
            retry_config=REMOTE_RETRY_CONFIG,
        ),
        f"s3://{s3_bucket}/": _public_store(f"s3://{s3_bucket}/"),
    }

    local_dirs = {
        Path(path).resolve().parent
        for path in json_paths
        if not path.startswith(("s3://", "gcs://", "gs://", "http://", "https://"))
    }
    for local_dir in local_dirs:
        stores[local_dir.as_uri() + "/"] = from_url(local_dir.as_uri() + "/")

    return ObjectStoreRegistry(stores)


def _resolve_kerchunk_templates(refs: Dict) -> Dict:
    """Resolve kerchunk's legacy ``{{key}}`` path templates in a references dict.

    Some pre-built NWM kerchunk-reference JSONs (e.g. those on the
    ``ciroh-nwm-zarr-copy`` S3 bucket) use kerchunk's old templating scheme: a
    top-level ``templates`` mapping (e.g. ``{"u": "https://.../file.nc"}``)
    that chunk manifest entries reference via the literal string ``"{{u}}"``,
    to keep repeated per-chunk paths short. VirtualiZarr's kerchunk translator
    doesn't resolve this templating, so do it ourselves before handing the
    refs off to VirtualiZarr.
    """
    templates = refs.get("templates")
    if not templates:
        return refs
    for key, value in templates.items():
        placeholder = "{{" + key + "}}"
        for ref in refs["refs"].values():
            if isinstance(ref, list) and ref and isinstance(ref[0], str):
                ref[0] = ref[0].replace(placeholder, value)
    return refs


def _is_null_or_nan(value) -> bool:
    """Return True if ``value`` is JSON ``null`` or a raw (undecoded) float NaN."""
    return value is None or (isinstance(value, float) and value != value)


def _fix_kerchunk_fill_values(refs: Dict) -> Dict:
    """Normalize a variable's fill-value metadata so VirtualiZarr can read it.

    Handles two related problems seen across both classic-kerchunk-built and
    VirtualiZarr-built references, both really about inconsistent/implicit
    fill-value conventions that VirtualiZarr's stricter reading doesn't
    tolerate the way xarray's classic (fsspec-based) kerchunk engine does:

    1. **Missing CF attribute.** Some references never set a CF
       ``_FillValue``/``missing_value`` attribute at all for a variable (e.g.
       NWM forcing's ``RAINRATE`` in the community S3-hosted CONUS
       references), relying on the reader falling back to the zarr array's
       own ``fill_value`` for masking (xarray's classic zarr backend does
       this via ``use_zarr_fill_value_as_mask``). VirtualiZarr's internal
       ``xr.open_zarr(..., zarr_format=3, ...)`` call doesn't opt into that
       fallback, so the real (and otherwise-correct) fill value is silently
       dropped -- e.g. ``.rio.nodata`` then comes back ``None`` instead of
       the correct value, breaking anything relying on it (such as
       zonal-weights generation). Fix: copy the zarr fill_value into
       ``_FillValue`` whenever neither CF attribute is already present.
    2. **Undecodable float encoding.** For float-dtype arrays, xarray's zarr
       backend requires the fill value to be represented as a string
       wherever it appears -- but the *two* locations need two *different*
       string encodings:

       - ``.zarray``'s own ``fill_value`` is decoded by zarr-python's
         ``ArrayV2Metadata.from_dict``, which accepts the literal string
         ``"NaN"`` for a null/NaN value (and plain JSON numbers otherwise).
       - ``.zattrs``'s ``_FillValue``/``missing_value`` is separately decoded
         by xarray's ``FillValueCoder.decode``, which for float dtypes
         *always* requires a base64-encoded little-endian double -- for any
         value, not just NaN (matching how kerchunk itself already encodes
         some float attribute values it can't represent natively in JSON,
         e.g. NWM's ``RAINRATE`` again). A raw (undecoded) float or JSON
         ``null`` in either location raises ``TypeError: Failed to decode
         fill_value: expected str or bytes ...``; passing the wrong kind of
         string (e.g. the literal ``"NaN"`` where base64 is expected)
         instead raises ``binascii.Error: Incorrect padding``.
    """
    for key in list(refs["refs"].keys()):
        if not key.endswith("/.zarray"):
            continue
        zattrs_key = key[: -len(".zarray")] + ".zattrs"
        if zattrs_key not in refs["refs"]:
            continue

        zarray = ujson.loads(refs["refs"][key])
        zattrs = ujson.loads(refs["refs"][zattrs_key])
        zarray_changed = False
        zattrs_changed = False

        try:
            dtype_kind = np.dtype(zarray.get("dtype")).kind
        except TypeError:
            # Structured/unrecognized dtype string; leave fill-value alone.
            dtype_kind = None

        # Coordinates are skipped: their zarr fill_value is HDF5's storage
        # default rather than a missing marker, and masking against it
        # promotes the array to float (feature_id becomes float64) and calls a
        # real id missing. Data variables do need it -- NWM packs forcing as
        # scaled integers whose nodata sentinel lives only in fill_value.
        # Byte-string dtypes are excluded because FillValueCoder.decode raises
        # on them (NWM's "crs" variable).
        is_coordinate = zattrs.get("_ARRAY_DIMENSIONS") == [key.split("/")[0]]
        if dtype_kind in ("f", "c", "b", "i", "u") and not is_coordinate \
                and "_FillValue" not in zattrs and "missing_value" not in zattrs \
                and zarray.get("fill_value") is not None:
            zattrs["_FillValue"] = zarray["fill_value"]
            zattrs_changed = True

        if dtype_kind == "f":
            if _is_null_or_nan(zarray.get("fill_value")):
                zarray["fill_value"] = "NaN"
                zarray_changed = True

            for attr_name in ("_FillValue", "missing_value"):
                value = zattrs.get(attr_name)
                if isinstance(value, str) or attr_name not in zattrs:
                    continue
                as_double = float("nan") if _is_null_or_nan(value) else float(value)
                zattrs[attr_name] = base64.standard_b64encode(
                    struct.pack("<d", as_double)
                ).decode()
                zattrs_changed = True

        if zarray_changed:
            refs["refs"][key] = ujson.dumps(zarray)
        if zattrs_changed:
            refs["refs"][zattrs_key] = ujson.dumps(zattrs)
    return refs


async def _download_kerchunk_refs(url: str, registry: ObjectStoreRegistry) -> bytes:
    """Download a single kerchunk reference JSON."""
    filepath = validate_and_normalize_path_to_uri(url, fs_root=Path.cwd().as_uri())
    store, path_after_prefix = registry.resolve(filepath)
    resp = await store.get_async(path_after_prefix)
    return memoryview(await resp.buffer_async()).tobytes()


def _feature_id_positions(
    feature_ids: np.ndarray,
    location_ids: np.ndarray,
) -> np.ndarray:
    """Find where each requested location sits in a file's feature_id coordinate.

    Selecting by position (``.isel``) rather than by label (``.sel``) avoids
    building a pandas index over every feature in the file -- 2.7M of them for
    CONUS -- once per file, which dominated the cost of reading a chunk.

    Parameters
    ----------
    feature_ids : np.ndarray
        The file's full feature_id coordinate.
    location_ids : np.ndarray
        Requested NWM feature ids.

    Returns
    -------
    np.ndarray
        Positions into ``feature_ids``, in the order of ``location_ids``.

    Raises
    ------
    ValueError
        If any requested id is not in the file.
    """
    feature_ids = np.asarray(feature_ids).astype(np.int64, copy=False)
    location_ids = np.asarray(location_ids).astype(np.int64, copy=False)

    # NWM output happens to be sorted, but don't rely on it: an unsorted file
    # would otherwise map to the wrong rows rather than fail.
    sorter = None
    if not np.all(feature_ids[:-1] <= feature_ids[1:]):
        sorter = np.argsort(feature_ids)

    positions = np.searchsorted(feature_ids, location_ids, sorter=sorter)
    positions = np.clip(positions, 0, feature_ids.size - 1)
    if sorter is not None:
        positions = sorter[positions]

    found = feature_ids[positions] == location_ids
    if not found.all():
        missing = location_ids[~found]
        raise ValueError(
            f"{missing.size} of {len(location_ids)} location_ids not found in "
            f"the NWM output: {missing[:10].tolist()}"
        )
    return positions


@lru_cache(maxsize=1)
def _warm_zarr_version_lookup() -> None:
    """Resolve zarr's version once, before any pool of readers starts.

    xarray's ``_zarr_v3`` is uncached, so every ``open_zarr`` -- and therefore
    every reference read -- calls ``importlib.metadata.version("zarr")``, which
    walks each ``sys.path`` entry. pyspark puts every jar it resolves on that
    path, and the aws bundles have tens of thousands of entries, so a cold
    lookup can take seconds. Workers all miss the cold cache at once and
    redundantly walk the same jars, holding the GIL: measured at 105s for one
    18-file chunk in a notebook with a spark session, and ~0 once warm.

    Call this before starting the workers, not inside them -- ``lru_cache`` is
    not atomic, so they would still race through the miss together.
    """
    try:
        version("zarr")
    except PackageNotFoundError:
        # zarr is a hard dependency, so this should not happen; if it somehow
        # does, xarray's own lookup will raise where it matters.
        logger.debug("Could not resolve zarr's version to warm the lookup.")


def _manifest_store_from_refs(
    content: bytes,
    registry: ObjectStoreRegistry,
) -> ManifestStore:
    """Turn a downloaded kerchunk reference JSON into a ManifestStore.

    Blocking and CPU-bound, so this must not run on the event loop.
    VirtualiZarr's own ``KerchunkJSONParser`` covers the same ground but can't
    expand the ``{{u}}`` templates the pre-built community references use,
    hence :func:`_resolve_kerchunk_templates`.
    """
    refs = ujson.loads(content)
    refs = _resolve_kerchunk_templates(refs)
    refs = _fix_kerchunk_fill_values(refs)
    return ManifestStore(
        group=manifestgroup_from_kerchunk_refs(refs), registry=registry
    )


class _FeatureIdPositions:
    """Resolve requested NWM ids to positions, reusing the work across files.

    Every file in a run repeats the same feature_id coordinate -- 2.7M values
    for CONUS -- so resolving positions per file re-reads and re-decompresses
    it once per file for an answer that does not change.

    Positions are keyed on feature_id's shape and dtype, both free from the
    file's metadata. That is the honest key: positions mean nothing across a
    differently sized coordinate, and a resize is the only way feature_id can
    vary within one call -- dates are clamped to a single NWM version by
    ``validate_operational_start_end_date`` and the domain is fixed by the
    configuration, so only a network revision (v2.2's 2,776,738 vs v3.0's
    2,776,734) or a different domain can occur, and both change the shape.

    Where feature_id is chunked, a sample of the requested ids is also
    re-read from each file, which checks contents rather than just size.

    Bind one instance to the run's ``location_ids`` and share it across files;
    it is safe to use from several threads.
    """

    def __init__(self, location_ids: Iterable[int]):
        self.location_ids = np.asarray(location_ids).astype(np.int64, copy=False)
        self._positions: Dict[Tuple, np.ndarray] = {}
        self._sample_counter = itertools.count()
        self._lock = threading.Lock()

    def resolve(self, group: zarr.Group) -> np.ndarray:
        """Positions of ``location_ids`` in this file's feature_id."""
        array = group["feature_id"]
        key = (tuple(array.shape), str(array.dtype))

        positions = self._positions.get(key)
        if positions is None:
            # Held across the read so files racing here wait for the first
            # one's answer instead of each fetching feature_id themselves --
            # without it every file in flight misses the empty cache at once
            # and the cache saves nothing.
            with self._lock:
                positions = self._positions.get(key)
                if positions is None:
                    positions = _feature_id_positions(
                        array[:], self.location_ids
                    )
                    self._positions[key] = positions
                    return positions

        if array.nchunks > 1:
            self._verify_sample(array, positions)
        return positions

    def _verify_sample(self, array: zarr.Array, positions: np.ndarray) -> None:
        """Re-read some requested ids from this file and check they match.

        The sample is confined to one chunk so only that chunk is fetched, and
        which chunk rotates per call, so consecutive files cover different
        parts of the coordinate rather than all re-checking the same one.
        """
        if positions.size == 0:
            return
        chunk_len = array.chunks[0]
        touched = np.unique(positions // chunk_len)
        target = touched[next(self._sample_counter) % touched.size]
        sampled = np.flatnonzero(positions // chunk_len == target)
        sampled = sampled[:FEATURE_ID_SAMPLE_SIZE]

        found = array.get_orthogonal_selection((positions[sampled],))
        expected = self.location_ids[sampled]
        if not np.array_equal(found, expected):
            raise ValueError(
                "This file's feature_id does not match the one the requested"
                " locations were resolved against, though it is the same"
                " shape. Refusing to return values that would be labelled"
                f" with the wrong ids. Expected {expected[:5].tolist()},"
                f" found {found[:5].tolist()}."
            )


def _decode(name: str, array: zarr.Array, values: np.ndarray) -> np.ndarray:
    """Apply CF decoding to values already selected out of ``array``.

    Uses xarray's own decoder, so scale_factor/add_offset/_FillValue and time
    units are handled exactly as before -- just on the selection rather than
    on the whole array, which is where the masked float64 intermediates came
    from.
    """
    dims = tuple(f"dim_{i}" for i in range(values.ndim))
    variable = xr.Variable(dims, values, attrs=dict(array.attrs))
    return xr.conventions.decode_cf_variable(name, variable).values


def _array_dims(array: zarr.Array) -> List[Optional[str]]:
    """Dimension names of a zarr array, or Nones if it doesn't name them.

    ManifestStore presents kerchunk's v2 metadata as v3, which moves the names
    out of the ``_ARRAY_DIMENSIONS`` attribute and into ``dimension_names``.
    """
    names = getattr(array.metadata, "dimension_names", None)
    if not names:
        names = array.attrs.get("_ARRAY_DIMENSIONS")
    if not names:
        return [None] * array.ndim
    return list(names)


def _select_on_axis(
    array: zarr.Array, positions: np.ndarray, axis: int
) -> np.ndarray:
    """Read ``array`` at ``positions`` along ``axis``.

    zarr fetches only the chunks the selection touches, and returns them in
    the order asked for, duplicates included -- matching numpy fancy indexing.
    """
    selection = tuple(
        positions if i == axis else slice(None) for i in range(array.ndim)
    )
    return array.get_orthogonal_selection(selection)


def _read_point_values(
    content: bytes,
    registry: ObjectStoreRegistry,
    variable_name: str,
    positions_cache: "_FeatureIdPositions",
) -> Tuple[np.ndarray, np.ndarray, str]:
    """Read one file's values for the requested locations.

    Goes to the zarr store directly rather than through
    ``to_virtual_dataset``: teehr wants a handful of values, not a virtual
    dataset, and building one reads the full feature_id coordinate and (via
    an upstream bug in VirtualiZarr's oversized-chunk warning) materializes
    the whole data array before it can be subset.

    Returns the decoded values, the file's time, and the NWM units.
    """
    group = zarr.open_group(_manifest_store_from_refs(content, registry), mode="r")

    array = group[variable_name]
    feature_id = group["feature_id"]
    dims = _array_dims(array)
    if "feature_id" not in dims:
        raise ValueError(
            f"'{variable_name}' has dimensions {dims}, with no feature_id to"
            " select locations along."
        )
    axis = dims.index("feature_id")
    # zarr indexes by position, so nothing else checks that the two agree.
    if array.shape[axis] != feature_id.shape[0]:
        raise ValueError(
            f"'{variable_name}' spans {array.shape[axis]} features but"
            f" feature_id has {feature_id.shape[0]}; the file disagrees"
            " with itself."
        )

    positions = positions_cache.resolve(group)
    values = _decode(variable_name, array, _select_on_axis(array, positions, axis))
    time = _decode("time", group["time"], group["time"][:])
    return values, time, array.attrs.get("units", "")


async def _open_ref_virtualizarr(
    url: str,
    registry: ObjectStoreRegistry,
    ignore_missing_file: bool,
    variable_name: str,
    positions_cache: "_FeatureIdPositions",
    executor: Optional[Executor] = None,
) -> Optional[Tuple[np.ndarray, np.ndarray, str]]:
    """Download one kerchunk reference and read the requested locations from it.

    Only the download happens on this coroutine; the read is blocking, so it
    runs in ``executor`` and leaves the loop free for other downloads.
    """
    try:
        content = await _download_kerchunk_refs(url, registry)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not download reference file: {e}")
        return None

    # Not guarded by ignore_missing_file: a location_id that isn't in the file
    # is a bad request, not a missing file, and must not be skipped silently.
    return await run_in_executor(
        lambda: _read_point_values(
            content, registry, variable_name, positions_cache
        ),
        executor,
    )


async def _open_kerchunk_dataset_async(
    url: str,
    registry: ObjectStoreRegistry,
    ignore_missing_file: bool,
    loadable_variables: List[str],
) -> Optional[xr.Dataset]:
    """Open a single kerchunk reference via VirtualiZarr, materializing ``loadable_variables``.

    Unlike :func:`_open_ref_virtualizarr` (point fetching, which subsets to
    ``location_ids``), no selection is applied here -- callers needing a subset
    (e.g. grid fetching's row/col slicing) do so themselves afterward.
    """
    try:
        content = await _download_kerchunk_refs(url, registry)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not download reference file: {e}")
        return None

    def _materialize() -> xr.Dataset:
        manifest_store = _manifest_store_from_refs(content, registry)
        ds = manifest_store.to_virtual_dataset(
            loadable_variables=loadable_variables,
            decode_times=True,
        )
        return ds[[v for v in loadable_variables if v in ds.variables]]

    try:
        return await asyncio.to_thread(_materialize)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not open reference dataset: {e}")
        return None


def open_kerchunk_dataset(
    url: str,
    loadable_variables: List[str],
    ignore_missing_file: bool = True,
    registry: Optional[ObjectStoreRegistry] = None,
) -> Optional[xr.Dataset]:
    """Open a single kerchunk reference via VirtualiZarr, materializing ``loadable_variables``.

    Used by grid fetching (see :func:`combine_and_open_kerchunk_refs` for the
    point-fetching, ``location_ids``-subsetting analogue), which reads the
    whole array for the requested variable(s) and subsets by row/col
    position afterward rather than by coordinate label.

    Parameters
    ----------
    url : str
        Path (local or remote) to a kerchunk reference JSON file.
    loadable_variables : List[str]
        Names of the variables/coordinates to materialize (e.g.
        ``[variable_name, "time"]``). Any not present in the file are
        silently skipped.
    ignore_missing_file : bool, optional
        Whether to ignore missing files, by default True.
    registry : Optional[ObjectStoreRegistry], optional
        A pre-built registry (see :func:`build_kerchunk_registry`) covering
        ``url``. Callers processing many files in one run should build one
        registry up front and pass it to every call. Built fresh from
        ``url`` alone if omitted.

    Returns
    -------
    Optional[xr.Dataset]
        The dataset with ``loadable_variables`` materialized, or None if the
        file was missing and ``ignore_missing_file`` is True.
    """
    if registry is None:
        registry = build_kerchunk_registry([url])
    return run_sync(
        _open_kerchunk_dataset_async(url, registry, ignore_missing_file, loadable_variables)
    )


def open_kerchunk_grid_window(
    url: str,
    variable_name: str,
    row_min: int,
    row_max: int,
    col_min: int,
    col_max: int,
    ignore_missing_file: bool = True,
    registry: Optional[ObjectStoreRegistry] = None,
    x_dim: str = "x",
    y_dim: str = "y",
) -> Optional[Tuple[np.ndarray, np.ndarray, str]]:
    """Read one gridded variable's bounding-box window from a kerchunk reference.

    Slices before reading, so only the chunks the window covers are fetched --
    NWM forcing grids are 5x5 tiles, so a small zone touches one of 25. The
    row/col bounds are inclusive, matching ``get_weights_row_col_stats``.

    Goes to the zarr store rather than ``open_kerchunk_dataset`` because
    building an xarray Dataset materializes the whole grid before it can be
    sliced. ``x``/``y`` are not read at all: only positions are needed here.

    Parameters
    ----------
    url : str
        Path (local or remote) to a kerchunk reference JSON file.
    variable_name : str
        Gridded variable to read.
    row_min, row_max, col_min, col_max : int
        Inclusive bounding box in grid positions.
    ignore_missing_file : bool, optional
        Whether to return None rather than raise on a missing file.
    registry : Optional[ObjectStoreRegistry], optional
        Pre-built registry covering ``url``; built fresh if omitted.
    x_dim, y_dim : str
        Dimension names, for grids that don't use NWM's usual "x"/"y".

    Returns
    -------
    Optional[Tuple[np.ndarray, np.ndarray, str]]
        ``(window, time_values, units)``, where ``window`` keeps the source
        variable's dimensions with y/x sliced. None if the file was missing
        and ``ignore_missing_file`` is True.
    """
    if registry is None:
        registry = build_kerchunk_registry([url])
    return run_sync(
        _open_kerchunk_grid_window_async(
            url, variable_name, row_min, row_max, col_min, col_max,
            ignore_missing_file, registry, x_dim, y_dim,
        )
    )


async def _open_kerchunk_grid_window_async(
    url: str,
    variable_name: str,
    row_min: int,
    row_max: int,
    col_min: int,
    col_max: int,
    ignore_missing_file: bool,
    registry: ObjectStoreRegistry,
    x_dim: str,
    y_dim: str,
) -> Optional[Tuple[np.ndarray, np.ndarray, str]]:
    """Async implementation backing :func:`open_kerchunk_grid_window`."""
    try:
        content = await _download_kerchunk_refs(url, registry)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not download reference file: {e}")
        return None

    def _read() -> Tuple[np.ndarray, np.ndarray, str]:
        group = zarr.open_group(
            _manifest_store_from_refs(content, registry), mode="r"
        )
        array = group[variable_name]
        dims = _array_dims(array)
        if x_dim not in dims or y_dim not in dims:
            raise ValueError(
                f"'{variable_name}' has dimensions {dims}, which do not"
                f" include '{y_dim}' and '{x_dim}'."
            )
        windows = {y_dim: slice(row_min, row_max + 1),
                   x_dim: slice(col_min, col_max + 1)}
        selection = tuple(windows.get(dim, slice(None)) for dim in dims)
        window = _decode(variable_name, array, array[selection])
        time = _decode("time", group["time"], group["time"][:])
        return window, time, array.attrs.get("units", "")

    try:
        return await asyncio.to_thread(_read)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not read grid window: {e}")
        return None


async def _combine_and_open_kerchunk_refs_async(
    json_paths: List[str],
    variable_name: str,
    location_ids: np.ndarray,
    ignore_missing_file: bool = True,
    concat_dims: Optional[List[str]] = ["time"],
    registry: Optional[ObjectStoreRegistry] = None,
    max_concurrent_files: Optional[int] = None,
    cpu_workers: Optional[int] = None,
) -> Tuple[xr.Dataset, List[bool]]:
    """Async implementation backing :func:`combine_and_open_kerchunk_refs`."""
    # Debug, not info: this runs once per chunk, and the caller is better
    # placed to report progress across the whole run.
    logger.debug(
        f"Combining and opening {len(json_paths)} virtualized reference files."
    )
    if not json_paths:
        raise FileNotFoundError("No NWM reference files were provided.")

    if registry is None:
        registry = build_kerchunk_registry(json_paths)
    urls = [_json_path_to_url(path) for path in json_paths]

    budget = resolve_budget(
        io=max_concurrent_files,
        cpu=cpu_workers,
        memory_per_item=POINT_READ_MEMORY,
    )

    # Must happen before the pool exists; see the function's docstring.
    _warm_zarr_version_lookup()

    # Shared across the chunk's files so feature_id is resolved once, not per
    # file; see _FeatureIdPositions for what each file is still checked on.
    positions_cache = _FeatureIdPositions(location_ids)

    # Sized from io, not cpu: the work inside is mostly waiting on the object
    # store with the GIL released, so bounding it by core count caps files in
    # flight far below what io_concurrency asks for. Matters on a pod with
    # fewer cores than a chunk has files -- measured 6.5s vs 2.9s for 18 files
    # at 2 vs 18 threads.
    with thread_pool(budget.io, len(urls)) as executor:
        results = await gather_bounded(
            lambda url: _open_ref_virtualizarr(
                url, registry, ignore_missing_file, variable_name,
                positions_cache, executor,
            ),
            urls,
            limit=budget.io,
        )
    read_mask = [result is not None for result in results]
    results = [result for result in results if result is not None]

    if not results:
        raise FileNotFoundError(
            "No NWM reference files could be read for the specified configuration."
        )

    # Stacked rather than xr.concat'd: every file contributes the same
    # locations in the same order, so there is nothing to align.
    values, times, units = zip(*results)
    ds = xr.Dataset(
        {variable_name: ((concat_dims[0], "feature_id"), np.stack(values))},
        coords={
            concat_dims[0]: np.concatenate(times),
            "feature_id": positions_cache.location_ids,
        },
    )
    ds[variable_name].attrs["units"] = units[0]
    return ds, read_mask


def combine_and_open_kerchunk_refs(
    json_paths: List[str],
    variable_name: str,
    location_ids: np.ndarray,
    ignore_missing_file: bool = True,
    concat_dims: Optional[List[str]] = ["time"],
    registry: Optional[ObjectStoreRegistry] = None,
    max_concurrent_files: Optional[int] = None,
    cpu_workers: Optional[int] = None,
) -> Tuple[xr.Dataset, List[bool]]:
    """Combine multiple kerchunk reference files into a single xarray Dataset.

    Each file is read through zarr directly, selecting ``location_ids`` before
    any data is fetched, so only the chunks holding them are transferred and
    the full per-file array is never materialized. Results are stacked along
    ``concat_dims[0]``.

    Uses VirtualiZarr's ManifestStore + an obstore-backed ObjectStoreRegistry
    rather than fsspec/gcsfs/s3fs, avoiding the async filesystem lifecycle
    issues those libraries can hit under zarr v3. Concurrency across files is
    handled with asyncio (see :func:`_combine_and_open_kerchunk_refs_async`);
    this function is a synchronous wrapper so existing callers (and
    Jupyter/script usage) don't need to change.

    Parameters
    ----------
    json_paths : List[str]
        List of paths (local or remote, may be mixed) to kerchunk reference
        JSON files.
    variable_name : str
        Name of the single data variable to load from each file. Other data
        variables are left virtual and never materialized.
    location_ids : np.ndarray
        NWM feature_ids to select from each file. Resolved to positions once
        per run and re-checked per file; see :class:`_FeatureIdPositions`.
    ignore_missing_file : bool, optional
        Whether to ignore missing files, by default True.
    concat_dims : Optional[List[str]], optional
        Dimensions to concatenate along, by default ["time"].
    registry : Optional[ObjectStoreRegistry], optional
        A pre-built registry (see :func:`build_kerchunk_registry`) covering
        ``json_paths``. Callers processing many chunks in one run should
        build one registry up front and pass it to every call, so obstore's
        stores/connection pools are reused across the whole run instead of
        being rebuilt per chunk. Built fresh from ``json_paths`` if omitted.
    max_concurrent_files : Optional[int], optional
        How many of ``json_paths`` to read at once. Defaults to the
        process-wide budget; divide it among callers running at the same time.
    cpu_workers : Optional[int]
        How many of those reads may be parsed at once. Defaults the same way.

    Returns
    -------
    Tuple[xr.Dataset, List[bool]]
        ``(dataset, read_mask)`` where ``read_mask[i]`` is ``True`` if
        ``json_paths[i]`` was read successfully.  Callers should use
        ``read_mask`` to keep any associated DataFrame in sync with the
        number of timesteps in the returned dataset.
    """
    return run_sync(
        _combine_and_open_kerchunk_refs_async(
            json_paths,
            variable_name,
            location_ids,
            ignore_missing_file,
            concat_dims,
            registry,
            max_concurrent_files,
            cpu_workers,
        )
    )


@lru_cache(maxsize=1)
def _build_gcs_source_registry() -> ObjectStoreRegistry:
    """Return the registry for the source NWM GCS bucket.

    Cached, so threads share one and each worker process builds its own on
    first use rather than one per file.
    """
    return ObjectStoreRegistry({
        f"gcs://{NWM_BUCKET}/": from_url(
            f"gs://{NWM_BUCKET}/",
            skip_signature=True,
            retry_config=REMOTE_RETRY_CONFIG,
        ),
    })


def _fix_scalar_chunk_keys(refs: Dict) -> Dict:
    """Rewrite VirtualiZarr's chunk-key convention for 0-d (scalar) arrays.

    Per the Zarr v2 spec, a 0-d array's chunk key is the empty string, and
    VirtualiZarr's kerchunk writer (``to_kerchunk``) follows that literally
    (e.g. a scalar ``crs`` grid-mapping variable gets the ref key ``"crs/"``).
    But VirtualiZarr's own kerchunk-JSON reader/translator
    (``manifestgroup_from_kerchunk_refs``, used by
    ``_manifest_store_from_refs``) can't parse that back
    (``ValueError: Invalid format for chunk key: ''``). Classic kerchunk
    (and community pre-built NWM references) instead use ``"0"`` for a
    scalar array's chunk key, which VirtualiZarr's reader handles fine, so
    rewrite newly-written refs to that convention here.
    """
    refs["refs"] = {
        (key + "0" if key.endswith("/") else key): value
        for key, value in refs["refs"].items()
    }
    return refs


def _fix_fill_values(refs: Dict) -> Dict:
    """Align each array's zarr ``fill_value`` with its CF fill-value attribute.

    VirtualiZarr's HDF reader carries over the raw on-disk HDF5 storage fill
    value (e.g. ``0``) into the zarr array's ``fill_value``, while the CF
    ``_FillValue``/``missing_value`` attribute (e.g. ``-999900``) says what
    actually represents missing data. When the two disagree, xarray's CF
    decoding treats the variable as ambiguous and masks *every* value to NaN
    (``SerializationWarning: variable 'x' has multiple fill values ...``) --
    reproducible via ``xr.open_dataset(..., engine="kerchunk")`` (classic
    kerchunk's xarray backend, formerly used for grid fetching before it
    moved to VirtualiZarr too). Classic kerchunk avoids this by setting the
    zarr ``fill_value`` to match the CF attribute directly, so do the same
    here. Note ``.zarray``/``.zattrs`` values in a kerchunk refs dict are
    JSON-encoded strings, not nested dicts.

    For some float-dtype variables (e.g. ``RAINRATE``), VirtualiZarr encodes
    ``_FillValue`` as a base64 string wrapping the raw little-endian double
    bytes of the value (rather than a plain JSON number), since not every
    float value round-trips cleanly through JSON -- decode that back to a
    number before using it, and skip entries that aren't a plain number or a
    decodable base64 double, so we never write a value zarr can't parse.
    """
    for key in list(refs["refs"].keys()):
        if not key.endswith("/.zattrs"):
            continue
        zarray_key = key[: -len(".zattrs")] + ".zarray"
        if zarray_key not in refs["refs"]:
            continue

        zattrs = ujson.loads(refs["refs"][key])
        cf_fill_value = zattrs.get("_FillValue", zattrs.get("missing_value"))
        if cf_fill_value is None:
            continue

        if isinstance(cf_fill_value, str) and cf_fill_value not in ("NaN", "Infinity", "-Infinity"):
            try:
                decoded_bytes = base64.b64decode(cf_fill_value, validate=True)
                if len(decoded_bytes) != 8:
                    continue
                cf_fill_value = struct.unpack("<d", decoded_bytes)[0]
            except (ValueError, struct.error):
                continue

        zarray = ujson.loads(refs["refs"][zarray_key])
        if zarray.get("dtype", "").lstrip("<>=|")[0] in ("i", "u"):
            cf_fill_value = int(cf_fill_value)
        if zarray.get("fill_value") != cf_fill_value:
            zarray["fill_value"] = cf_fill_value
            refs["refs"][zarray_key] = ujson.dumps(zarray)

    return refs


def gen_json_virtualizarr(
    remote_path: str,
    json_dir: Union[str, Path],
    ignore_missing_file: bool,
    registry: Optional[ObjectStoreRegistry] = None,
) -> Optional[str]:
    """Create a single kerchunk reference JSON file using VirtualiZarr's HDFParser.

    Reads NWM NetCDF metadata directly via obstore (no fsspec/gcsfs).

    Parameters
    ----------
    remote_path : str
        Path to the file in the remote location (ie, GCS bucket).
    json_dir : str
        Directory for saving zarr reference json files.
    ignore_missing_file : bool
        Whether to skip (return None) or raise on missing/corrupt files.
    registry : Optional[ObjectStoreRegistry]
        Registry covering the source bucket. Built fresh if not provided, so
        callers processing many files in parallel should build one once and
        pass it in to avoid re-registering a store per file.

    Returns
    -------
    Optional[str]
        Path to the local zarr reference json file, or None if the file was
        missing/corrupt and ``ignore_missing_file`` is True.
    """
    if registry is None:
        registry = _build_gcs_source_registry()

    p = remote_path.split("/")
    date = p[3]
    fname = p[5]
    outf = str(Path(json_dir, f"{date}.{fname}.json"))

    try:
        manifest_store = HDFParser()(url=remote_path, registry=registry)
        # Materialize only the coordinates small enough to be worth embedding.
        # Left to its default, to_virtual_dataset loads every *indexed*
        # coordinate, and a loaded variable has no byte range left to point at,
        # so it must be inlined here as base64. For NWM point output that means
        # feature_id: a 103KB compressed array inflated to 29.7MB, making the
        # reference 2.4x the size of the NetCDF it stands in for. feature_id is
        # read from the source file's byte range when a chunk is read, which
        # costs little since those chunks are fetched alongside the data.
        #
        # The grid coordinates stay on the list: x and y run a few thousand
        # values, cost ~90KB inlined, and keeping them here saves the gridded
        # read path a round trip per file. Names absent from a given file are
        # ignored, so one list covers point and gridded output.
        vds = manifest_store.to_virtual_dataset(
            loadable_variables=INLINE_COORDINATES
        )
        refs = vds.virtualize.to_kerchunk(format="dict")
        refs = _fix_scalar_chunk_keys(refs)
        refs = _fix_fill_values(refs)
        with open(outf, "w") as f:
            ujson.dump(refs, f)
    except Exception as err:
        if not ignore_missing_file:
            raise Exception(f"Corrupt or missing file: {remote_path}") from err
        logger.warning(f"A missing or corrupt file was encountered: {remote_path}")
        return None

    return outf


def read_nwm_global_attrs(
    remote_path: str,
    registry: Optional[ObjectStoreRegistry] = None,
) -> Dict:
    """Read the global (root group) attributes of a remote NWM NetCDF file.

    Fetches only the file's HDF5 metadata, through the same obstore-backed I/O
    as :func:`gen_json_virtualizarr`, so it is cheap enough to run across a
    file list before committing to a fetch.

    Avoids ``ManifestStore.to_virtual_dataset``, which loads every indexed
    coordinate -- for point output that means inlining ``feature_id`` at the
    cost documented in :func:`gen_json_virtualizarr`. Reading the zarr group's
    attributes is free once the file is parsed.

    Parameters
    ----------
    remote_path : str
        Path to the file in the remote location (ie, GCS bucket), as produced
        by :func:`build_remote_nwm_filelist`.
    registry : Optional[ObjectStoreRegistry]
        Registry covering the source bucket. Defaults to the process-wide
        cached NWM source registry; pass one only to read a different bucket.

    Returns
    -------
    Dict
        The file's global attributes, e.g. ``NWM_version_number``,
        ``code_version``, ``model_configuration``,
        ``model_output_valid_time``. Raises if the file is missing or corrupt.

    Examples
    --------
    >>> attrs = read_nwm_global_attrs(gcs_component_paths[0])
    >>> attrs["NWM_version_number"]
    'v3.0'
    """
    if registry is None:
        registry = _build_gcs_source_registry()

    manifest_store = HDFParser()(url=remote_path, registry=registry)
    return zarr.open_group(manifest_store, mode="r").attrs.asdict()


def _normalize_nwm_version_attr(value: str) -> str:
    """Reduce a model-version attribute to bare digits, e.g. ``"3.0"``.

    Spelling varies by era (``"NWM 1.2"`` vs ``"v3.1"``), so both are reduced
    to the form used in :data:`NWM_VERSION_ATTR_VALUES`.
    """
    return value.strip().removeprefix("NWM").strip().lstrip("vV")


def read_nwm_file_version(
    remote_path: str,
    registry: Optional[ObjectStoreRegistry] = None,
) -> Optional[str]:
    """Read the model version a remote NWM file reports, e.g. ``"3.0"``.

    Takes the file's global attributes (see :func:`read_nwm_global_attrs`) and
    returns whichever of :data:`NWM_VERSION_ATTRS` is present, normalized to
    bare digits so eras with different conventions compare cleanly.

    Parameters
    ----------
    remote_path : str
        Path to the file in the remote location (ie, GCS bucket).
    registry : Optional[ObjectStoreRegistry]
        Registry covering the source bucket; see
        :func:`read_nwm_global_attrs`.

    Returns
    -------
    Optional[str]
        The normalized version, or None if the file carries no version
        attribute at all -- nwm12-era *forcing* files record only their
        initialization and valid times -- so absence is normal, not an error.
    """
    attrs = read_nwm_global_attrs(remote_path, registry=registry)
    for attr in NWM_VERSION_ATTRS:
        if attrs.get(attr):
            return _normalize_nwm_version_attr(attrs[attr])
    return None


def _parse_nwm_cycle(remote_path: str) -> Optional[datetime]:
    """Parse the cycle (day plus z-hour) a remote NWM path refers to.

    Parsed as in :func:`parse_nwm_gcs_paths`, but for one path and without
    needing the configuration name. None if either part is absent.
    """
    day_match = re.search(DAY_PATTERN, remote_path)
    z_match = re.search(r"t([0-9]+)z", Path(remote_path).name)
    if day_match is None or z_match is None:
        return None
    day = day_match.group().split(".")[1]
    return (
        datetime.strptime(day, "%Y%m%d")
        + timedelta(hours=int(z_match.group(1)))
    )


def nwm_version_at(cycle: datetime) -> Optional[str]:
    """Look up the NWM version in force at ``cycle``.

    Walks :data:`NWM_VERSION_BOUNDARIES`, which holds each version's first
    cycle at z-hour resolution.

    Parameters
    ----------
    cycle : datetime
        A cycle time (day plus z-hour).

    Returns
    -------
    Optional[str]
        The normalized version, or None if ``cycle`` precedes the earliest
        operational data teehr reads.
    """
    in_force = None
    for boundary, boundary_version in NWM_VERSION_BOUNDARIES:
        if cycle < boundary:
            break
        in_force = boundary_version
    return in_force


def _outgoing_version_within_grace(cycle: datetime) -> Optional[str]:
    """Find the previous version still tolerated at ``cycle``.

    Returns the version in force immediately *before* the most recent
    boundary, when ``cycle`` falls within
    :data:`NWM_VERSION_SWITCHOVER_GRACE` of it, else None.
    """
    latest = None
    for index, (boundary, _) in enumerate(NWM_VERSION_BOUNDARIES):
        if cycle < boundary:
            break
        latest = index
    if not latest:  # None, or the first entry, which has no predecessor
        return None

    boundary = NWM_VERSION_BOUNDARIES[latest][0]
    if cycle - boundary >= NWM_VERSION_SWITCHOVER_GRACE:
        return None
    return NWM_VERSION_BOUNDARIES[latest - 1][1]


def validate_nwm_version_against_files(
    remote_paths: List[str],
    nwm_version: str,
    registry: Optional[ObjectStoreRegistry] = None,
) -> None:
    """Raise if the source files disagree with the requested NWM version.

    Catches a date range the requested ``nwm_version`` did not produce, which
    otherwise succeeds and yields timeseries labelled with the wrong version,
    by comparing each file's own attributes against
    :data:`NWM_VERSION_ATTR_VALUES`.

    Reads only the first and last of ``remote_paths``: that covers both a
    version wrong for the whole range and a range straddling a boundary, where
    only one end disagrees. Checking every file would double the run's
    metadata reads, since reference building parses each file anyway.

    A file reporting the previous version within
    :data:`NWM_VERSION_SWITCHOVER_GRACE` of a boundary warns and is accepted,
    since NOAA reruns some cycles on the outgoing system mid-switch. Outside
    that window a mismatch raises.

    Parameters
    ----------
    remote_paths : List[str]
        Remote filepaths for the fetch, as produced by
        :func:`build_remote_nwm_filelist`. An empty list is a no-op.
    nwm_version : str
        The requested version, a
        :class:`SupportedNWMOperationalVersionsEnum` value. ``nwm21`` and
        ``nwm22`` are treated as one version, so a file stamped either
        satisfies a request for either.
    registry : Optional[ObjectStoreRegistry]
        Registry covering the source bucket; see
        :func:`read_nwm_global_attrs`.

    Raises
    ------
    ValueError
        If a checked file reports a version outside the set accepted for
        ``nwm_version``, or if ``nwm_version`` has no entry in
        :data:`NWM_VERSION_ATTR_VALUES`.
    """
    if nwm_version not in NWM_VERSION_ATTR_VALUES:
        raise ValueError(
            f"No model version attribute values are known for"
            f" '{nwm_version}'; add an entry to NWM_VERSION_ATTR_VALUES."
        )
    if not remote_paths:
        return

    expected = NWM_VERSION_ATTR_VALUES[nwm_version]
    # dict.fromkeys so a single-file list is read once, not twice.
    to_check = dict.fromkeys([remote_paths[0], remote_paths[-1]])

    mismatches = []
    for path in to_check:
        found = read_nwm_file_version(path, registry=registry)
        if found is None:
            # Normal for some eras (see read_nwm_file_version); nothing to
            # compare against, so this file cannot confirm or deny.
            logger.debug(f"No model version attribute to check in {path}.")
            continue
        if found in expected:
            continue

        # An outgoing-version file just after a switchover is an archive
        # artifact, not the wrong request, so warn rather than fail the fetch.
        cycle = _parse_nwm_cycle(path)
        outgoing = (
            _outgoing_version_within_grace(cycle)
            if cycle is not None else None
        )
        if outgoing is not None and found == outgoing:
            logger.warning(
                f"{path} reports NWM version {found}, but its cycle is within"
                f" {NWM_VERSION_SWITCHOVER_GRACE} of the"
                f" v{nwm_version_at(cycle)} switchover, where NOAA reruns some"
                " cycles on the outgoing system. Treating it as"
                f" v{nwm_version_at(cycle)} rather than a version mismatch."
            )
            continue

        mismatches.append((path, found))

    if mismatches:
        detail = "; ".join(f"{path} reports {found}" for path, found in mismatches)
        raise ValueError(
            f"NWM version mismatch: requested '{nwm_version}' which expects"
            f" {'/'.join(sorted(expected))}, but {detail}. Check the start and"
            " end dates against the requested version."
        )
    logger.debug(
        f"Checked {len(to_check)} file(s) against requested NWM version"
        f" '{nwm_version}'."
    )


async def _build_zarr_references_virtualizarr_async(
    remote_paths: List[str],
    json_dir: Union[str, Path],
    ignore_missing_file: bool,
    cpu_workers: Optional[int] = None,
) -> list[str]:
    """Async implementation backing :func:`build_zarr_references_virtualizarr`."""
    logger.debug("Building zarr references via VirtualiZarr.")

    json_dir_path = Path(json_dir)
    if not json_dir_path.exists():
        json_dir_path.mkdir(parents=True)

    existing_jsons = []
    missing_paths = []
    for path in remote_paths:
        p = path.split("/")
        date = p[3]
        fname = p[5]
        local_path = Path(json_dir, f"{date}.{fname}.json")
        if local_path.exists():
            existing_jsons.append(str(local_path))
        else:
            missing_paths.append(path)
    if len(missing_paths) == 0:
        return sorted(existing_jsons)

    budget = resolve_budget(
        cpu=cpu_workers, memory_per_process=REFERENCE_WORKER_MEMORY
    )
    processes = (
        budget.processes
        if use_process_pool(
            len(missing_paths), budget.processes, REFERENCE_BUILD_MIN_ITEMS
        )
        else 0
    )
    logger.info(
        f"Building {len(missing_paths)} references in"
        f" {processes or budget.cpu} {'processes' if processes else 'threads'}."
    )

    # Reference building opens each file with xarray too, so warm the lookup
    # here as well. As an initializer it also runs in each worker process,
    # which starts with a cold cache of its own.
    _warm_zarr_version_lookup()

    json_paths = await map_blocking(
        gen_json_virtualizarr,
        missing_paths,
        workers=budget.cpu,
        args=(str(json_dir), ignore_missing_file),
        processes=processes,
        initializer=_warm_zarr_version_lookup,
    )
    json_paths = list(json_paths)
    json_paths.extend(existing_jsons)

    if not any(json_paths):
        raise FileNotFoundError(
            "No NWM files for specified input configuration were found in GCS!"
        )

    json_paths = [path for path in json_paths if path is not None]

    return sorted(json_paths)


def build_zarr_references_virtualizarr(
    remote_paths: List[str],
    json_dir: Union[str, Path],
    ignore_missing_file: bool,
    cpu_workers: Optional[int] = None,
) -> list[str]:
    """Build the single-file zarr JSON reference files using VirtualiZarr.

    Avoids fsspec/gcsfs entirely, so the whole reference-building step goes
    through the same obstore-backed I/O as ``combine_and_open_kerchunk_refs``,
    and removes any dependency on pre-built S3 kerchunk references.

    Files already cached in ``json_dir`` are skipped. The rest are built in
    parallel -- in worker processes for big batches, threads otherwise; see
    :mod:`teehr.utils.concurrency`.

    Parameters
    ----------
    remote_paths : List[str]
        List of remote filepaths.
    json_dir : str or Path
        Local directory for caching json files.
    ignore_missing_file : bool
        Whether to skip or raise on missing/corrupt files.
    cpu_workers : Optional[int]
        References built at once. Compute-bound (h5py parses each file), so
        this takes the cpu budget rather than the io one.

    Returns
    -------
    list[str]
        List of paths to the zarr reference json files.
    """
    return run_sync(
        _build_zarr_references_virtualizarr_async(
            remote_paths, json_dir, ignore_missing_file, cpu_workers
        )
    )


def construct_assim_paths(
    gcs_dir: str,
    configuration: str,
    output_type: str,
    dates: pd.DatetimeIndex,
    t_minus: Iterable[int],
    configuration_name_in_filepath: str,
    cycle_z_hours: Iterable[int],
    domain: str,
    file_extension: str = "nc"
) -> list[str]:
    """Construct paths to NWM point assimilation data.

    This function prioritizes value time over reference time so that only
    files with value times falling within the specified date range are included
    in the resulting file list.

    Parameters
    ----------
    gcs_dir : str
        Path to the NWM data on GCS.
    configuration : str
        Configuration type.
    output_type : str
        Output component of the configuration.
    dates : pd.DatetimeIndex
        Range of days to fetch data.
    t_minus : Iterable[int]
        Collection of lookback hours to include when fetching assimilation
        data.
    configuration_name_in_filepath : str
        Name of the assimilation configuration as represented in the GCS file.
        Defined in const_nwm.py.
    cycle_z_hours : Iterable[int]
        The z-hour of the assimilation configuration per day.
        Defined in const_nwm.py.
    domain : str
        Geographic region covered by the assimilation configuration.
        Defined in const_nwm.py.
    file_extension : str
        File extension ("nc" or "nc.json" for remote kerchunk).

    Returns
    -------
    list[str]
        List of remote filepaths.
    """
    logger.debug("Constructing assimilation paths.")

    component_paths = []

    for dt in dates:
        dt_str = dt.strftime("%Y%m%d")

        # Add the values starting from day 1,
        # skipping value times in the previous day
        if "hawaii" in configuration:
            for cycle_hr in cycle_z_hours:
                for tm in t_minus:
                    for tm2 in [0, 15, 30, 45]:
                        if (tm * 100 + tm2) > cycle_hr * 100:
                            continue
                        file_path = f"{gcs_dir}/nwm.{dt_str}/{configuration}/nwm.t{cycle_hr:02d}z.{configuration_name_in_filepath}.{output_type}.tm{tm:02d}{tm2:02d}.{domain}.{file_extension}"  # noqa
                        component_paths.append(file_path)
        else:
            for cycle_hr in cycle_z_hours:
                for tm in t_minus:
                    if tm > cycle_hr:
                        continue
                    file_path = f"{gcs_dir}/nwm.{dt_str}/{configuration}/nwm.t{cycle_hr:02d}z.{configuration_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.{file_extension}"  # noqa
                    component_paths.append(file_path)

        # Now add the values from the day following the end day,
        # whose value times fall within the end day
        if "extend" in configuration:
            for tm in t_minus:
                dt_add = dt + pd.Timedelta(cycle_hr + 24, unit="hours")
                hr_add = dt_add.hour
                if tm > hr_add:
                    dt_add_str = dt_add.strftime("%Y%m%d")
                    file_path = f"{gcs_dir}/nwm.{dt_add_str}/{configuration}/nwm.t{hr_add:02d}z.{configuration_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.{file_extension}"  # noqa
                    component_paths.append(file_path)

        elif "hawaii" in configuration:
            for cycle_hr2 in cycle_z_hours:
                for tm in t_minus:
                    for tm2 in [0, 15, 30, 45]:
                        if cycle_hr2 > 0:
                            dt_add = dt + pd.Timedelta(
                                cycle_hr + cycle_hr2, unit="hours"
                            )
                            hr_add = dt_add.hour
                            if (tm * 100 + tm2) > hr_add * 100:
                                dt_add_str = dt_add.strftime("%Y%m%d")
                                file_path = f"{gcs_dir}/nwm.{dt_add_str}/{configuration}/nwm.t{hr_add:02d}z.{configuration_name_in_filepath}.{output_type}.tm{tm:02d}{tm2:02d}.{domain}.{file_extension}"  # noqa
                                component_paths.append(file_path)
        else:
            for cycle_hr2 in cycle_z_hours:
                for tm in t_minus:
                    if cycle_hr2 > 0:
                        dt_add = dt + pd.Timedelta(
                            cycle_hr + cycle_hr2, unit="hours"
                        )
                        hr_add = dt_add.hour
                        if tm > hr_add:
                            dt_add_str = dt_add.strftime("%Y%m%d")
                            file_path = f"{gcs_dir}/nwm.{dt_add_str}/{configuration}/nwm.t{hr_add:02d}z.{configuration_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.{file_extension}"  # noqa
                            component_paths.append(file_path)

    return sorted(component_paths)


def get_end_date_from_ingest_days(
    start_date: Union[datetime, pd.Timestamp],
    ingest_days: int
) -> datetime:
    """Get the end date from the start date and ingest days.

    Parameters
    ----------
    start_date : Union[datetime, pd.Timestamp]
        The start date.
    ingest_days : int
        The number of days to ingest.

    Returns
    -------
    datetime
        The end date.
    """
    if ingest_days <= 0:
        raise ValueError("ingest_days must be greater than 0")
    warn(
        "'ingest_days' is deprecated and "
        "will be removed in future versions",
        DeprecationWarning,
        stacklevel=2
    )
    end_date = start_date + timedelta(days=ingest_days)
    return end_date


def build_remote_nwm_filelist(
    configuration: str,
    output_type: str,
    start_dt: Union[datetime, pd.Timestamp],
    end_dt: Union[datetime, pd.Timestamp],
    analysis_config_dict: Dict,
    t_minus_hours: Optional[Iterable[int]],
    ignore_missing_file: Optional[bool],
    prioritize_analysis_value_time: Optional[bool],
    drop_overlapping_assimilation_values: Optional[bool],
    ingest_days: Optional[int] = None
) -> List[str]:
    """Assemble a list of remote NWM files based on user parameters.

    Parameters
    ----------
    configuration : str
        Configuration type.
    output_type : str
        Output component of the configuration.
    start_dt : Timestamp or datetime
        Date to begin data ingest.
    end_dt : Timestamp or datetime
        Date to end data ingest.
    t_minus_hours : Optional[Iterable[int]]
        Collection of lookback hours to include when fetching
        assimilation data. If None (default), all available
        t-minus hours are included.
    ignore_missing_file : Optional[bool]
        Flag specifying whether or not to fail if a missing
        NWM file is encountered
        True = skip and continue
        False = fail.
    prioritize_analysis_value_time : Optional[bool]
        A boolean flag that determines the method of fetching analysis
        assimilation data. When True, assimilation data is limited to
        the start and end dates according to value_time. When False,
        the data is fetched based on reference_time (value_time may fall
        before the start date)
    drop_overlapping_assimilation_values : Optional[bool]
        A boolean flag that determines whether or not to remove
        overlapping assimilation values. If True, only values corresponding
        to the most recent reference_time are kept. If False, all values
        are kept, even if they overlap in value_time.
    ingest_days : int
        The number of days to ingest.

    Returns
    -------
    list
        List of remote filepaths (strings).
    """
    logger.debug("Building remote NWM file list from GCS.")

    gcs_dir = f"gcs://{NWM_BUCKET}"
    if ingest_days is None:
        dates = pd.date_range(start=start_dt.date(), end=end_dt.date(), freq="1d")
    else:
        dates = pd.date_range(start=start_dt.date(), end=end_dt.date(), freq="1d", inclusive="left")

    if "assim" in configuration and prioritize_analysis_value_time:
        cycle_z_hours = analysis_config_dict[configuration]["cycle_z_hours"]
        domain = analysis_config_dict[configuration]["domain"]
        configuration_name_in_filepath = analysis_config_dict[configuration][
            "configuration_name_in_filepath"
        ]
        max_lookback = analysis_config_dict[configuration]["num_lookback_hrs"]

        if t_minus_hours is None:
            t_minus_hours = np.arange(
                0, max_lookback, 1
            ).tolist()

        if max(t_minus_hours) > max_lookback - 1:
            raise ValueError(
                f"The maximum specified t-minus hour exceeds the lookback "
                f"period for this configuration: {configuration}; max t-minus: "  # noqa
                f"{max(t_minus_hours)} hrs; "
                f"look-back period: {max_lookback} hrs"
            )

        component_paths = construct_assim_paths(
            gcs_dir,
            configuration,
            output_type,
            dates,
            t_minus_hours,
            configuration_name_in_filepath,
            cycle_z_hours,
            domain,
        )

        if drop_overlapping_assimilation_values is True:
            logger.debug(
                "Removing overlapping assimilation value times."
            )
            parsed_df = parse_nwm_gcs_paths(
                component_paths=component_paths,
                nwm_configuration=configuration,
            )
            dropped_df = remove_overlapping_assim_validtimes(
                parsed_df=parsed_df,
            )
            component_paths = dropped_df["filepath"].tolist()
    else:
        store = from_url(
            f"gs://{NWM_BUCKET}/",
            skip_signature=True,
            retry_config=REMOTE_RETRY_CONFIG,
        )
        prefixes = [
            f"nwm.{dt.strftime('%Y%m%d')}/{configuration}/" for dt in dates
        ]

        # One listing per day, run concurrently: each is ~0.85s of waiting, so
        # a year of them in sequence is minutes before any data is touched.
        async def _list_day(prefix: str) -> List[str]:
            pattern = f"{prefix}nwm.*.{output_type}*"
            keys = await _list_prefix(store, prefix)
            result = [key for key in keys if fnmatch.fnmatch(key, pattern)]
            if len(result) == 0 and not ignore_missing_file:
                raise FileNotFoundError(
                    f"No NWM files found in {gcs_dir}/{pattern}"
                )
            return result

        per_day = run_sync(
            gather_bounded(
                _list_day, prefixes, limit=resolve_budget().io
            )
        )
        # Sorted at the end, so listing order does not affect the result.
        component_paths = sorted(
            f"gcs://{NWM_BUCKET}/{key}" for keys in per_day for key in keys
        )

        if "assim" in configuration:
            parsed_df = parse_nwm_gcs_paths(
                component_paths=component_paths,
                nwm_configuration=configuration,
            )
            if drop_overlapping_assimilation_values is True:
                parsed_df = remove_overlapping_assim_validtimes(
                    parsed_df=parsed_df,
                )
            if t_minus_hours is not None:
                parsed_df = parsed_df[
                    parsed_df["tm_hour"].astype(int).isin(t_minus_hours)
                ]
            component_paths = parsed_df["filepath"].tolist()

    return component_paths


def get_period_start_end_times(
    period: pd.Period,
    start_date: datetime,
    end_date: datetime
) -> Dict[str, datetime]:
    """Get the start and end times for a period.

    Adjusts for the start and end dates of the total data ingest.

    Parameters
    ----------
    period : pd.Period
        The current period.
    start_date : datetime
        The start date of the data ingest.
    end_date : datetime
        Then end date of the data ingest.

    Returns
    -------
    Dict[str, datetime]
        The start and end times for the period.
    """
    logger.debug("Getting period start and end times.")

    start_dt = period.start_time
    end_dt = period.end_time

    if start_date > period.start_time:
        start_dt = start_date

    if (end_date < period.end_time) & (period.freq.name != "D"):
        end_dt = end_date

    return {"start_dt": start_dt, "end_dt": end_dt}


def create_periods_based_on_chunksize(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    chunk_by: Union[NWMChunkByEnum, None]
) -> List[pd.Period]:
    """Create a list of periods of a given frequency, start, and end time.

    Parameters
    ----------
    start_date : datetime, str
        The start date.
    end_date : datetime, str
        The end date.
    chunk_by : Union[NWMChunkByEnum, None]
        The chunk size frequency.

    Returns
    -------
    List[pd.Period]
        A pandas period range.
    """
    logger.debug("Creating periods based on chunk_by.")

    if chunk_by is None:
        periods = [None]

    if chunk_by == "day":
        periods = pd.period_range(start=start_date, end=end_date, freq="D")

    if chunk_by == "week":
        periods = pd.period_range(start=start_date, end=end_date, freq="W")

    if chunk_by == "month":
        periods = pd.period_range(start=start_date, end=end_date, freq="M")

    if chunk_by == "year":
        periods = pd.period_range(start=start_date, end=end_date, freq="Y")

    if chunk_by == "location_id":
        raise ValueError(
            "A period range cannot be created based on location_id."
        )

    return periods


def split_dataframe(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    """Split a dataframe into chunks of a specified size.

    Parameters
    ----------
    df : pd.DataFrame
        The input dataframe.
    chunk_size : int
        The size of the chunks.

    Returns
    -------
    List[pd.DataFrame]
        A list of dataframes.
    """
    chunks = []
    for i in range(0, df.shape[0], chunk_size):
        chunks.append(df.iloc[i:i + chunk_size])
    return chunks


def convert_value_from_kelvin_to_celsius(df: pd.DataFrame) -> pd.DataFrame:
    """Convert temperature values from Kelvin to Celsius for a specific variable.

    Parameters
    ----------
    df : pd.DataFrame
        The input dataframe.
    variable_name : str
        The name of the variable to convert.

    Returns
    -------
    pd.DataFrame
        The dataframe with converted temperature values.
    """
    df["value"] = df["value"] - 273.15
    df.loc[:, UNIT_NAME] = "C"
    return df


def log_temperature_conversion_message(
    variable_name: str,
    convert_k_to_c: bool
):
    """Log the conversion of temperature values from Kelvin to Celsius."""
    if variable_name == "T2D" and convert_k_to_c:
        logger.info(
            f"Temperature values for {variable_name} will be converted from Kelvin to Celsius."
        )
    elif variable_name == "T2D" and not convert_k_to_c:
        logger.warning(
            f"Temperature values for {variable_name} will be kept in Kelvin."
            " If you would like to convert to Celsius, set 'convert_k_to_c=True'."
        )
    elif variable_name != "T2D" and convert_k_to_c:
        logger.warning(
            "Temperature conversion from Kelvin to Celsius is only applicable for the variable 'T2D'."
            f" The variable you are fetching is {variable_name}, so no conversion will be applied."
            " Set 'convert_k_to_c=False' to suppress this warning."
        )
