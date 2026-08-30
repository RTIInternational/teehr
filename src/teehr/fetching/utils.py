"""Module defining common utilities for fetching and processing NWM data."""
from pathlib import Path
from typing import Union, Optional, Iterable, List, Dict, Tuple
from datetime import datetime
from datetime import timedelta
import asyncio
import base64
import logging
import os
import re
import json
import struct
from warnings import warn

import fsspec
import ujson  # fast json
from obstore.store import from_url
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.manifests import ManifestStore
from virtualizarr.manifests.manifest import validate_and_normalize_path_to_uri
from virtualizarr.parsers import HDFParser
from virtualizarr.parsers.kerchunk.translator import manifestgroup_from_kerchunk_refs
import pandas as pd
import numpy as np
import xarray as xr
import pyarrow as pa
import pandera

from teehr.evaluation.write import Write as writer
from teehr.utils.utils import run_sync
from teehr.fetching.models.utils import (
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.fetching.models.utils import (
    SupportedNWMOperationalVersionsEnum,
    NWMChunkByEnum
)
from teehr.fetching.const import (
    NWM_BUCKET,
    NWM_HAWAII_VARIABLE_MAPPER,
    NWM_S3_JSON_PATH,
    NWM30_START_DATE,
    NWM21_START_DATE,
    NWM20_START_DATE,
    NWM12_START_DATE,
    NWM_VARIABLE_MAPPER,
    NWM_CONFIGURATION_DESCRIPTIONS,
    UNIT_NAME,
    VARIABLE_NAME
)
import teehr.models.pandera_dataframe_schemas as schemas

TZ_PATTERN = re.compile(r't[0-9]+z')
DAY_PATTERN = re.compile(r'nwm.[0-9]+')

# Concurrency for I/O-bound work (remote existence checks, VirtualiZarr/obstore
# reads), which mostly waits on network latency rather than the GIL.
IO_MAX_WORKERS = int(os.environ.get("TEEHR_IO_MAX_WORKERS", 48))
# Concurrency for CPU-bound work (parsing HDF5 metadata via VirtualiZarr's
# HDFParser), which holds the GIL and scales with available cores, not I/O latency.
CPU_MAX_WORKERS = int(os.environ.get("TEEHR_CPU_MAX_WORKERS", os.cpu_count() or 4))


logger = logging.getLogger(__name__)


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
    if nwm_config_name in NWM_CONFIGURATION_DESCRIPTIONS:
        ev_config_desc = NWM_CONFIGURATION_DESCRIPTIONS[nwm_config_name]
    else:
        ev_config_desc = "NWM operational forecasts"  # default description
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
    """Make sure start/end dates work with specified NWM version."""
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

    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        if start_date < NWM30_START_DATE:
            raise ValueError(v3_err_msg)
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
    ignore_missing_file: bool
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
            ignore_missing_file
        )

    elif kerchunk_method == SupportedKerchunkMethod.remote:
        # Use whatever pre-builts exist, skipping the rest
        s3_path_list = [f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json" for gcs_path in gcs_component_paths]
        file_check_output = check_if_files_exist(s3_path_list)
        json_paths = [path for path, exists in file_check_output.items() if exists]
        missing_files = [path for path, exists in file_check_output.items() if not exists]
        logger.info(
            f"Mode: {kerchunk_method}. Found {len(json_paths)} pre-built jsons in s3,"
            f" skipping {len(missing_files)} missing files."
        )

    elif kerchunk_method == SupportedKerchunkMethod.auto:
        # Use whatever pre-builts exist, and create the missing
        s3_path_list = [f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json" for gcs_path in gcs_component_paths]
        file_check_output = check_if_files_exist(s3_path_list)
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
                    ignore_missing_file
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


def write_timeseries_parquet_file(
    filepath: Path,
    overwrite_output: bool,
    data: Union[pa.Table, pd.DataFrame],
    timeseries_type: TimeseriesTypeEnum
):
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
    """
    logger.debug(f"Writing parquet file: {filepath}")

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
        return

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
        return

    if not filepath.is_file():
        writer.to_cache(
            source_data=validated_df,
            cache_filepath=filepath,
            write_schema=write_schema
        )
    elif filepath.is_file() and overwrite_output:
        logger.info(f"Overwriting {filepath.name}")
        writer.to_cache(
            source_data=validated_df,
            cache_filepath=filepath,
            write_schema=write_schema
        )
    elif filepath.is_file() and not overwrite_output:
        logger.info(
            f"{filepath.name} already exists and overwrite_output=False;"
            " skipping"
        )


def list_to_np(lst):
    """Convert list to a tuple."""
    return tuple([np.array(a) for a in lst])


async def _check_if_files_exist_async(file_path_list: List[str]) -> Dict[str, bool]:
    """Async implementation backing :func:`check_if_files_exist`."""
    stores: Dict[str, object] = {}

    def _resolve(path: str) -> Tuple[object, str]:
        # e.g. "s3://bucket/some/key.json" -> store for "s3://bucket/", "some/key.json"
        scheme, _, rest = path.partition("://")
        bucket, _, key = rest.partition("/")
        prefix = f"{scheme}://{bucket}/"
        if prefix not in stores:
            stores[prefix] = from_url(prefix, skip_signature=True)
        return stores[prefix], key

    semaphore = asyncio.Semaphore(min(IO_MAX_WORKERS, len(file_path_list)))

    async def _check(path: str) -> tuple:
        store, key = _resolve(path)
        async with semaphore:
            try:
                await store.head_async(key)
                return path, True
            except FileNotFoundError:
                return path, False

    results = await asyncio.gather(*[_check(path) for path in file_path_list])
    return dict(results)


def check_if_files_exist(file_path_list: List[str]) -> Dict[str, bool]:
    """Check for existence of S3 files."""
    return run_sync(_check_if_files_exist_async(file_path_list))


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
    gcs_store = from_url(f"gs://{NWM_BUCKET}/", skip_signature=True)
    s3_bucket = NWM_S3_JSON_PATH.split("://")[1]
    stores = {
        f"gcs://{NWM_BUCKET}/": gcs_store,
        f"gs://{NWM_BUCKET}/": gcs_store,
        # some kerchunk references encode chunk locations via GCS's public HTTPS
        # frontend rather than the "gcs://" scheme.
        "https://storage.googleapis.com/": from_url("https://storage.googleapis.com/"),
        f"s3://{s3_bucket}/": from_url(f"s3://{s3_bucket}/", skip_signature=True),
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

        # FillValueCoder.decode only supports numeric dtypes for a
        # _FillValue attribute (raises for e.g. "|S1" byte-string arrays
        # like NWM's "crs" grid-mapping variable, which never has -- and
        # shouldn't get -- CF fill-value semantics).
        if dtype_kind in ("f", "c", "b", "i", "u") \
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


async def _open_kerchunk_manifest_store(
    url: str,
    registry: ObjectStoreRegistry,
):
    """Parse a single kerchunk reference JSON into a VirtualiZarr ManifestStore."""
    filepath = validate_and_normalize_path_to_uri(url, fs_root=Path.cwd().as_uri())
    store, path_after_prefix = registry.resolve(filepath)
    resp = await store.get_async(path_after_prefix)
    content = memoryview(await resp.buffer_async()).tobytes()
    refs = ujson.loads(content)
    refs = _resolve_kerchunk_templates(refs)
    refs = _fix_kerchunk_fill_values(refs)
    manifestgroup = manifestgroup_from_kerchunk_refs(refs)
    return ManifestStore(group=manifestgroup, registry=registry)


async def _open_ref_virtualizarr(
    url: str,
    registry: ObjectStoreRegistry,
    ignore_missing_file: bool,
    variable_name: str,
    location_ids: np.ndarray,
) -> Optional[xr.Dataset]:
    """Open a single kerchunk reference via VirtualiZarr and subset it to ``location_ids``.

    Only ``variable_name`` (plus the coordinate variables needed to index it) is
    materialized; all other data variables in the file are left virtual and dropped.
    ``location_ids`` selection happens immediately after materialization, so a full
    per-file array (e.g. every NWM feature_id) is never held in memory once this
    function returns—only the handful of requested locations are.

    The reference JSON itself is fetched with obstore's async API directly on
    this coroutine; materializing the manifest store (which involves its own,
    separate synchronous I/O inside VirtualiZarr/zarr) is offloaded to a
    thread via ``asyncio.to_thread`` so many files can be in flight at once
    without blocking the event loop.
    """
    try:
        manifest_store = await _open_kerchunk_manifest_store(url, registry)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not open reference dataset: {e}")
        return None

    def _materialize() -> xr.Dataset:
        # Cheap pass: only default (dimension) coordinate variables get materialized here.
        probe_ds = manifest_store.to_virtual_dataset()
        loadable_variables = [variable_name, *probe_ds.coords.keys()]
        ds = manifest_store.to_virtual_dataset(
            loadable_variables=loadable_variables,
            decode_times=True,
        )
        keep_vars = [variable_name]
        if "time" in ds.coords:
            keep_vars.append("time")
        ds = ds[keep_vars]
        try:
            return ds.sel(feature_id=location_ids)
        except KeyError as e:
            missing = np.setdiff1d(location_ids, ds.feature_id.values.astype(int))
            raise ValueError(
                f"{missing.size} of {len(location_ids)} location_ids not found in "
                f"the NWM output: {missing[:10].tolist()}"
            ) from e

    return await asyncio.to_thread(_materialize)


async def _open_kerchunk_dataset_async(
    url: str,
    registry: ObjectStoreRegistry,
    ignore_missing_file: bool,
    loadable_variables: List[str],
) -> Optional[xr.Dataset]:
    """Open a single kerchunk reference via VirtualiZarr, materializing ``loadable_variables``.

    Unlike :func:`_open_ref_virtualizarr` (point fetching's ``location_ids``
    label-based subsetting via ``.sel``), no selection is applied here --
    callers needing a subset (e.g. grid fetching's row/col slicing via
    ``.isel``, which only needs positions, not coordinate values) do so
    themselves afterward.
    """
    try:
        manifest_store = await _open_kerchunk_manifest_store(url, registry)
    except Exception as e:
        if not ignore_missing_file:
            raise
        logger.warning(f"Could not open reference dataset: {e}")
        return None

    def _materialize() -> xr.Dataset:
        ds = manifest_store.to_virtual_dataset(
            loadable_variables=loadable_variables,
            decode_times=True,
        )
        return ds[[v for v in loadable_variables if v in ds.variables]]

    return await asyncio.to_thread(_materialize)


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


async def _combine_and_open_kerchunk_refs_async(
    json_paths: List[str],
    variable_name: str,
    location_ids: np.ndarray,
    ignore_missing_file: bool = True,
    concat_dims: Optional[List[str]] = ["time"],
    registry: Optional[ObjectStoreRegistry] = None,
) -> Tuple[xr.Dataset, List[bool]]:
    """Async implementation backing :func:`combine_and_open_kerchunk_refs`."""
    logger.info("Combining and opening kerchunk reference files.")
    if not json_paths:
        raise FileNotFoundError("No NWM reference files were provided.")

    if registry is None:
        registry = build_kerchunk_registry(json_paths)
    urls = [_json_path_to_url(path) for path in json_paths]

    semaphore = asyncio.Semaphore(min(IO_MAX_WORKERS, len(urls)))

    async def _bounded_open(url: str) -> Optional[xr.Dataset]:
        async with semaphore:
            return await _open_ref_virtualizarr(
                url, registry, ignore_missing_file, variable_name, location_ids
            )

    datasets = list(await asyncio.gather(*[_bounded_open(url) for url in urls]))
    read_mask = [ds is not None for ds in datasets]
    datasets = [ds for ds in datasets if ds is not None]

    if not datasets:
        raise FileNotFoundError(
            "No NWM reference files could be read for the specified configuration."
        )

    ds = xr.concat(datasets, dim=concat_dims[0], data_vars="all")
    return ds, read_mask


def combine_and_open_kerchunk_refs(
    json_paths: List[str],
    variable_name: str,
    location_ids: np.ndarray,
    ignore_missing_file: bool = True,
    concat_dims: Optional[List[str]] = ["time"],
    storage_options: Optional[Dict] = {},
    registry: Optional[ObjectStoreRegistry] = None,
) -> Tuple[xr.Dataset, List[bool]]:
    """Combine multiple kerchunk reference files into a single xarray Dataset.

    Uses VirtualiZarr + an obstore-backed ObjectStoreRegistry rather than
    fsspec/gcsfs/s3fs, avoiding the async filesystem lifecycle issues those
    libraries can hit under zarr v3. Concurrency across files is handled with
    asyncio (see :func:`_combine_and_open_kerchunk_refs_async`); this function
    is a synchronous wrapper around that coroutine so existing callers (and
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
        NWM feature_ids to subset each file to immediately after loading
        ``variable_name``, before results from all files are gathered.
    ignore_missing_file : bool, optional
        Whether to ignore missing files, by default True.
    concat_dims : Optional[List[str]], optional
        Dimensions to concatenate along, by default ["time"].
    storage_options : Optional[Dict], optional
        Unused; retained for backward compatibility with existing callers.
        Anonymous remote access is configured directly on the registry's
        stores instead.
    registry : Optional[ObjectStoreRegistry], optional
        A pre-built registry (see :func:`build_kerchunk_registry`) covering
        ``json_paths``. Callers processing many chunks in one run should
        build one registry up front and pass it to every call, so obstore's
        stores/connection pools are reused across the whole run instead of
        being rebuilt per chunk. Built fresh from ``json_paths`` if omitted.

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
        )
    )


def _build_gcs_source_registry() -> ObjectStoreRegistry:
    """Build an ObjectStoreRegistry covering only the source NWM GCS bucket."""
    return ObjectStoreRegistry({
        f"gcs://{NWM_BUCKET}/": from_url(f"gs://{NWM_BUCKET}/", skip_signature=True),
    })


def _fix_scalar_chunk_keys(refs: Dict) -> Dict:
    """Rewrite VirtualiZarr's chunk-key convention for 0-d (scalar) arrays.

    Per the Zarr v2 spec, a 0-d array's chunk key is the empty string, and
    VirtualiZarr's kerchunk writer (``to_kerchunk``) follows that literally
    (e.g. a scalar ``crs`` grid-mapping variable gets the ref key ``"crs/"``).
    But VirtualiZarr's own kerchunk-JSON reader/translator
    (``manifestgroup_from_kerchunk_refs``, used by
    ``_open_kerchunk_manifest_store``) can't parse that back
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
        vds = manifest_store.to_virtual_dataset()
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


async def _build_zarr_references_virtualizarr_async(
    remote_paths: List[str],
    json_dir: Union[str, Path],
    ignore_missing_file: bool,
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

    registry = _build_gcs_source_registry()
    semaphore = asyncio.Semaphore(min(CPU_MAX_WORKERS, len(missing_paths)))

    async def _build_one(path: str) -> Optional[str]:
        async with semaphore:
            return await asyncio.to_thread(
                gen_json_virtualizarr, path, json_dir, ignore_missing_file, registry
            )

    json_paths = list(await asyncio.gather(*[_build_one(path) for path in missing_paths]))
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
) -> list[str]:
    """Build the single-file zarr JSON reference files using VirtualiZarr.

    Avoids fsspec/gcsfs entirely, so the whole reference-building step goes
    through the same obstore-backed I/O as ``combine_and_open_kerchunk_refs``,
    and removes any dependency on pre-built S3 kerchunk references.

    Parameters
    ----------
    remote_paths : List[str]
        List of remote filepaths.
    json_dir : str or Path
        Local directory for caching json files.

    Returns
    -------
    list[str]
        List of paths to the zarr reference json files.
    """
    return run_sync(
        _build_zarr_references_virtualizarr_async(
            remote_paths, json_dir, ignore_missing_file
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
    fs = fsspec.filesystem("gcs", token="anon", skip_instance_cache=True)
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
        component_paths = []
        for dt in dates:
            dt_str = dt.strftime("%Y%m%d")
            file_path = (
                f"{gcs_dir}/nwm.{dt_str}/{configuration}/nwm.*.{output_type}*"
            )
            result = fs.glob(file_path)
            if (len(result) == 0) & (not ignore_missing_file):
                raise FileNotFoundError(f"No NWM files found in {file_path}")
            component_paths.extend(result)
        component_paths = sorted([f"gcs://{path}" for path in component_paths])

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
