"""Module defining common utilities for fetching and processing NWM data."""
from pathlib import Path
from typing import Union, Optional, Iterable, List, Dict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timedelta
import logging
import re
import json
from warnings import warn
import asyncio

from kerchunk.combine import MultiZarrToZarr
import dask
import fsspec
import ujson  # fast json
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd
import numpy as np
import xarray as xr
import geopandas as gpd
import pyarrow as pa
import pandera

from teehr.evaluation.write import Write as writer
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
    UNIT_NAME
)
import teehr.models.pandera_dataframe_schemas as schemas

TZ_PATTERN = re.compile(r't[0-9]+z')
DAY_PATTERN = re.compile(r'nwm.[0-9]+')


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


def parse_nwm_forecast_gcs_paths(
    gcs_paths: List[str],
) -> pd.DataFrame:
    """Parse day and z_hour from NWM file paths.

    Works with any NWM file path type: GCS HDF5 (.nc), S3 or local kerchunk
    JSON (.json), or VirtualiZarr parquet reference (.parq). Uses DAY_PATTERN
    and TZ_PATTERN to extract day and z_hour; returns z_hour in 't00z' format
    matching parse_nwm_json_paths.

    Parameters
    ----------
    gcs_paths : List[str]
        List of NWM file paths (GCS .nc, S3 .json, local .json, or local .parq).

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: day (YYYYMMDD str), z_hour ('t00z' str),
        filepath (str).
    """
    logger.debug("Parsing day and z-hour from NWM file paths.")
    days = []
    z_hours = []
    for path in gcs_paths:
        filename = Path(path).name
        res = re.search(DAY_PATTERN, path).group()
        days.append(res.split(".")[1])
        z_hours.append(re.search(TZ_PATTERN, filename).group())
    return pd.DataFrame({"day": days, "z_hour": z_hours, "filepath": gcs_paths})


def format_nwm_configuration_metadata(
    nwm_config_name: str,
    nwm_version: str
) -> Dict[str, str]:
    """Format the NWM configuration name and member for the Evaluation.

    Returns a dictionary with the formatted configuration name and member,
    which is parsed from the NWM configuration name if it's an ensemble
    (ie., medium range or long range streamflow).
    """
    logger.info(
        f"Formatting configuration name for {nwm_config_name}."
    )
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
    logger.debug(f"Generating json paths. kerchunk_method: {kerchunk_method}")

    if kerchunk_method == SupportedKerchunkMethod.local:
        # Create them manually first
        json_paths = build_zarr_references(gcs_component_paths,
                                           json_dir,
                                           ignore_missing_file)

    elif kerchunk_method == SupportedKerchunkMethod.remote:
        # Use whatever pre-builts exist, skipping the rest
        fs = fsspec.filesystem("s3", anon=True, asynchronous=True)
        s3_path_list = [f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json" for gcs_path in gcs_component_paths]
        file_check_output = asyncio.run(check_if_files_exist(fs, s3_path_list))
        json_paths = [path for path, exists in file_check_output.items() if exists]
        missing_files = [path for path, exists in file_check_output.items() if not exists]
        logger.info(
            f"Mode: {kerchunk_method}. Found {len(json_paths)} pre-built jsons in s3,"
            f" skipping {len(missing_files)} missing files."
        )

    elif kerchunk_method == SupportedKerchunkMethod.auto:
        # Use whatever pre-builts exist, and create the missing
        fs = fsspec.filesystem("s3", anon=True, asynchronous=True)
        s3_path_list = [f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json" for gcs_path in gcs_component_paths]
        file_check_output = asyncio.run(check_if_files_exist(fs, s3_path_list))
        json_paths = [path for path, exists in file_check_output.items() if exists]
        missing_files = [path for path, exists in file_check_output.items() if not exists]
        logger.info(
            f"Mode: {kerchunk_method}. Found {len(json_paths)} pre-built jsons in s3,"
            f" building references for {len(missing_files)} files."
        )

        if len(missing_files) > 0:
            # Set back to gcs paths
            missing_files = [path.replace(NWM_S3_JSON_PATH, "gcs:/") for path in missing_files]
            json_paths.extend(
                build_zarr_references(missing_files,
                                      json_dir,
                                      ignore_missing_file)
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


def parquet_to_gdf(parquet_filepath: str) -> gpd.GeoDataFrame:
    """Read parquet as GeoDataFrame."""
    gdf = gpd.read_parquet(parquet_filepath)
    return gdf


def np_to_list(t):
    """Convert numpy array to list."""
    return [a.tolist() for a in t]


def get_dataset(
    filepath: str, ignore_missing_file: bool, **kwargs
) -> xr.Dataset:
    """Get an xarray dataset from a filepath."""
    logger.debug(f"Getting xarray dataset from: {filepath}")
    try:
        if filepath.startswith("s3://"):
            s3 = fsspec.filesystem("s3", anon=True)
            with s3.open(filepath, "rb") as f:
                reference = ujson.load(f)
            return xr.open_dataset(reference, engine="kerchunk", storage_options=kwargs)
        return xr.open_dataset(filepath, engine="kerchunk", storage_options=kwargs)
    except FileNotFoundError as e:
        if not ignore_missing_file:
            raise e
        return None
    except ValueError:
        raise ValueError(f"There was a problem reading {filepath}")


def list_to_np(lst):
    """Convert list to a tuple."""
    return tuple([np.array(a) for a in lst])


async def check_if_files_exist(fs: fsspec.filesystem, file_path_list: List[str]) -> Dict[str, bool]:
    """Check for existence of files asynchronously."""
    # Prepare concurrent tasks using the internal async method _exists
    tasks = [fs._exists(path) for path in file_path_list]
    # Execute all network requests in parallel
    results = await asyncio.gather(*tasks)
    # Map each path to its True/False result
    return dict(zip(file_path_list, results))


@dask.delayed
def gen_json(
    remote_path: str,
    fs: fsspec.filesystem,
    json_dir: Union[str, Path],
    ignore_missing_file: bool,
) -> str:
    """Create a single kerchunk reference JSON file.

    Parameters
    ----------
    remote_path : str
        Path to the file in the remote location (ie, GCS bucket).
    fs : fsspec.filesystem
        Fsspec filesystem mapped to GCS.
    json_dir : str
        Directory for saving zarr reference json files.

    Returns
    -------
    str
        Path to the local zarr reference json file.
    """
    so = dict(
        mode="rb",
        anon=True,
        default_fill_cache=False,
        default_cache_type="first",  # noqa
    )
    try:
        with fs.open(remote_path, **so) as infile:
            p = remote_path.split("/")
            date = p[3]
            fname = p[5]
            outf = str(Path(json_dir, f"{date}.{fname}.json"))
            try:
                h5chunks = SingleHdf5ToZarr(infile,
                                            remote_path,
                                            inline_threshold=300)
            except OSError as err:
                if not ignore_missing_file:
                    raise Exception(f"Corrupt file: {remote_path}") from err
                else:
                    logger.warning(
                        "A potentially corrupt file was encountered:"
                        f"{remote_path}"
                    )
                    return None
            with open(outf, "wb") as f:
                f.write(ujson.dumps(h5chunks.translate()).encode())
    except FileNotFoundError as e:
        if not ignore_missing_file:
            raise e
        else:
            logger.warning(f"A missing file was encountered: {remote_path}")
            return None
    return outf


def combine_and_open_kerchunk_refs(
    json_paths: List[str],
    target_protocol: str = "gcs",
    target_options: Optional[Dict] = None,
    concat_dims: Optional[List[str]] = None,
    storage_options: Optional[Dict] = None,
) -> xr.Dataset:
    """Combine multiple kerchunk reference files into a single xarray Dataset.

    Reads all reference files in parallel (handles local, S3, and GCS paths),
    merges them with ``MultiZarrToZarr``, then opens the combined zarr store
    as a single xarray Dataset.  zarr v3 fetches all required chunks
    asynchronously from the merged store.

    Parameters
    ----------
    json_paths : List[str]
        Paths to kerchunk JSON reference files. Mixed local/S3/GCS paths are
        supported within the same call.
    target_protocol : str
        fsspec protocol for the actual data chunks referenced in the JSON
        files. Default ``"gcs"``.
    target_options : Optional[Dict]
        Auth/storage options for the data chunks. Defaults to
        ``{"anon": True}``.
    concat_dims : Optional[List[str]]
        Dimension(s) to concatenate along. Default ``["time"]``.
    storage_options : Optional[Dict]
        Passed to ``xr.open_dataset`` for the kerchunk engine, e.g.
        ``{"target_options": {"anon": True}}``.

    Returns
    -------
    xr.Dataset
        A materialised xarray Dataset backed by a single zarr store.
    """
    def _read_ref(path: str) -> dict:
        if path.startswith("s3://"):
            with fsspec.open(path, "rb", anon=True) as f:
                return ujson.load(f)
        if path.startswith(("gcs://", "gs://")):
            with fsspec.open(path, "rb", token="anon") as f:
                return ujson.load(f)
        return ujson.loads(Path(path).read_bytes())

    with ThreadPoolExecutor(max_workers=min(64, len(json_paths))) as executor:
        refs = list(executor.map(_read_ref, json_paths))

    mzz = MultiZarrToZarr(
        refs,
        remote_protocol=target_protocol,
        remote_options=target_options or {"anon": True},
        concat_dims=concat_dims or ["time"],
    )
    combined = mzz.translate()
    return xr.open_dataset(
        combined,
        engine="kerchunk",
        storage_options=storage_options or {},
    )


def resolve_nwm_file_paths(
    gcs_paths: List[str],
    kerchunk_method: str,
    json_dir: Path,
    ignore_missing_file: bool,
) -> List[Optional[str]]:
    """Resolve the best available virtual reference path for each GCS file.

    For each GCS file path, returns the highest-priority available reference
    according to ``kerchunk_method``:

    - ``"local"``: check ``json_dir`` for ``.parq`` then ``.json``; fall back
      to the original GCS ``.nc`` path (VirtualiZarr will scan the HDF5 header
      and cache the result to ``json_dir``).
    - ``"remote"``: check S3 for a pre-built kerchunk JSON using a single async
      batch call; return ``None`` for files with no S3 JSON (they are skipped).
    - ``"auto"``: check S3 first, then ``json_dir`` for ``.parq``/``.json``,
      then fall back to the GCS ``.nc`` path.

    Parameters
    ----------
    gcs_paths : List[str]
        GCS paths to NWM netcdf files.
    kerchunk_method : str
        One of ``"local"``, ``"remote"``, or ``"auto"``.
    json_dir : Path
        Cache directory for local ``.json`` and ``.parq`` reference files.
    ignore_missing_file : bool
        Passed through for logging context; controls whether callers raise on
        empty results.

    Returns
    -------
    List[Optional[str]]
        Same length as ``gcs_paths``. Each entry is the best available
        reference path, or ``None`` if the file should be skipped
        (``"remote"`` mode with no S3 JSON).
    """
    logger.debug(f"Resolving file paths. kerchunk_method: {kerchunk_method}")

    json_dir_path = Path(json_dir)
    if not json_dir_path.exists():
        json_dir_path.mkdir(parents=True)

    def _local_cache_path(gcs_path: str) -> Optional[str]:
        p = gcs_path.split("/")
        date = p[3]
        fname = p[5]
        parq = Path(json_dir, f"{date}.{fname}.parq")
        if parq.exists():
            return str(parq)
        jsn = Path(json_dir, f"{date}.{fname}.json")
        if jsn.exists():
            return str(jsn)
        return None

    resolved: List[Optional[str]] = [None] * len(gcs_paths)

    if kerchunk_method == SupportedKerchunkMethod.local:
        for i, gcs_path in enumerate(gcs_paths):
            cached = _local_cache_path(gcs_path)
            resolved[i] = cached if cached is not None else gcs_path

    elif kerchunk_method == SupportedKerchunkMethod.remote:
        fs = fsspec.filesystem("s3", anon=True, asynchronous=True)
        s3_paths = [
            f"{NWM_S3_JSON_PATH}/{p.split('://')[1]}.json" for p in gcs_paths
        ]
        file_check = asyncio.run(check_if_files_exist(fs, s3_paths))
        found = sum(1 for v in file_check.values() if v)
        logger.info(
            f"Mode: {kerchunk_method}. Found {found} pre-built jsons in s3,"
            f" skipping {len(gcs_paths) - found} missing files."
        )
        for i, s3_path in enumerate(s3_paths):
            resolved[i] = s3_path if file_check.get(s3_path) else None

    elif kerchunk_method == SupportedKerchunkMethod.auto:
        fs = fsspec.filesystem("s3", anon=True, asynchronous=True)
        s3_paths = [
            f"{NWM_S3_JSON_PATH}/{p.split('://')[1]}.json" for p in gcs_paths
        ]
        file_check = asyncio.run(check_if_files_exist(fs, s3_paths))
        found = sum(1 for v in file_check.values() if v)
        logger.info(
            f"Mode: {kerchunk_method}. Found {found} pre-built jsons in s3,"
            f" checking local cache and GCS for {len(gcs_paths) - found} remaining files."
        )
        for i, (gcs_path, s3_path) in enumerate(zip(gcs_paths, s3_paths)):
            if file_check.get(s3_path):
                resolved[i] = s3_path
            else:
                cached = _local_cache_path(gcs_path)
                resolved[i] = cached if cached is not None else gcs_path

    return resolved


def build_zarr_references(
    remote_paths: List[str],
    json_dir: Union[str, Path],
    ignore_missing_file: bool,
) -> list[str]:
    """Build the single file zarr json reference files using kerchunk.

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
    logger.debug("Building zarr references.")

    json_dir_path = Path(json_dir)
    if not json_dir_path.exists():
        json_dir_path.mkdir(parents=True)

    fs = fsspec.filesystem("gcs", token="anon")

    # Check to see if the jsons already exist locally
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

    results = []
    for path in missing_paths:
        results.append(gen_json(path, fs, json_dir, ignore_missing_file))
    json_paths = dask.compute(results)[0]
    json_paths.extend(existing_jsons)

    if not any(json_paths):
        raise FileNotFoundError(
            "No NWM files for specified input configuration were found in GCS!"
        )

    json_paths = [path for path in json_paths if path is not None]

    return sorted(json_paths)


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
    fs = fsspec.filesystem("gcs", token="anon")
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
