"""Module defining common utilities for fetching and processing NWM data."""
from pathlib import Path
from typing import Union, Optional, Iterable, List, Dict
from datetime import datetime
from datetime import timedelta
import logging
import re
import json
from warnings import warn

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

from teehr.models.fetching.utils import (
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.models.fetching.utils import (
    SupportedNWMOperationalVersionsEnum,
    NWMChunkByEnum
)
from teehr.fetching.const import (
    NWM_BUCKET,
    NWM_S3_JSON_PATH,
    NWM30_START_DATE,
    NWM21_START_DATE,
    NWM20_START_DATE,
    NWM12_START_DATE,
    NWM_VARIABLE_MAPPER,
    VARIABLE_NAME,
    NWM_CONFIGURATION_DESCRIPTIONS
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
        if nwm_configuration == "analysis_assim_hawaii":
            value_time = reference_time - timedelta(minutes=int(tm_hour))
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
        if ev_member is not None:
            ev_config_desc = NWM_CONFIGURATION_DESCRIPTIONS[nwm_config_name] \
                + f" {ev_member}"
        else:
            ev_config_desc = NWM_CONFIGURATION_DESCRIPTIONS[nwm_config_name]
    else:
        ev_config_desc = "NWM operational forecasts"  # default description
    return {
        "name": ev_config_name,
        "member": ev_member,
        "description": ev_config_desc
    }


def format_nwm_variable_name(variable_name: str) -> str:
    """Format the NWM variable name for the Evaluation."""
    logger.info(f"Getting schema variable name for {variable_name}.")
    return NWM_VARIABLE_MAPPER[VARIABLE_NAME]. \
        get(variable_name, variable_name)


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
        fs = fsspec.filesystem("s3", anon=True)
        results = []
        for gcs_path in gcs_component_paths:
            results.append(check_for_prebuilt_json_paths(fs, gcs_path))
        json_paths = dask.compute(results)[0]
        json_paths = [path for path in json_paths if path is not None]

    elif kerchunk_method == SupportedKerchunkMethod.auto:
        # Use whatever pre-builts exist, and create the missing
        #  files, if any
        fs = fsspec.filesystem("s3", anon=True)
        results = []
        for gcs_path in gcs_component_paths:
            results.append(
                check_for_prebuilt_json_paths(
                    fs, gcs_path, return_gcs_path=True
                )
            )
        s3_or_gcs_paths = dask.compute(results)[0]

        # Build any jsons that do not already exist in s3
        gcs_paths = []
        json_paths = []
        for path in s3_or_gcs_paths:
            if path.split("://")[0] == "gcs":
                gcs_paths.append(path)
            else:
                json_paths.append(path)

        if len(gcs_paths) > 0:
            json_paths.extend(
                build_zarr_references(gcs_paths,
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
    elif timeseries_type == TimeseriesTypeEnum.secondary:
        schema = schemas.secondary_timeseries_schema(type="pandas")

    try:
        validated_df = schema.validate(df, lazy=True)
    except pandera.errors.SchemaErrors as exc:
        msg = json.dumps(exc.message, indent=2)
        logger.error(
            f"Validation error: {msg}"
            f"\nThis file '{filepath}' will be skipped."
        )
        return

    if not filepath.is_file():
        validated_df.to_parquet(filepath)
    elif filepath.is_file() and overwrite_output:
        logger.info(f"Overwriting {filepath.name}")
        validated_df.to_parquet(filepath)
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
    """Retrieve a blob from the data service as xarray.Dataset.

    Parameters
    ----------
    filepath : str
        Path to the kerchunk json file. Can be local or remote.
    ignore_missing_file : bool
        Flag controlling whether to ignore missing files.

    Returns
    -------
    xarray.Dataset
        The data stored in the blob.
    """
    logger.debug(f"Getting xarray dataset from: {filepath}")

    try:
        m = fsspec.filesystem(
            "reference", fo=filepath, **kwargs
        ).get_mapper()
    except FileNotFoundError as e:
        if not ignore_missing_file:
            raise e
        else:
            return None
    except ValueError:
        raise ValueError(f"There was a problem reading {filepath}")
    return xr.open_dataset(m, engine="zarr", consolidated=False)


def list_to_np(lst):
    """Convert list to a tuple."""
    return tuple([np.array(a) for a in lst])


@dask.delayed
def check_for_prebuilt_json_paths(
    fs: fsspec.filesystem, gcs_path: str, return_gcs_path=False
) -> str:
    """Check for existence of a pre-built kerchunk json in s3 based \
    on its GCS path.

    Parameters
    ----------
    fs : fsspec.filesystem
        S3-based filesystem.
    gcs_path : str
        Path to the netcdf file in GCS.
    return_gcs_path : bool, optional
        Flag to return GCS path of s3 is missing, by default False.

    Returns
    -------
    str
        Path to the json in s3 or netcdf file in GCS.
    """
    s3_path = f"{NWM_S3_JSON_PATH}/{gcs_path.split('://')[1]}.json"
    if fs.exists(s3_path):
        return s3_path
    else:
        if return_gcs_path:
            return gcs_path


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
        if configuration == "analysis_assim_hawaii":
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

        elif configuration == "analysis_assim_hawaii":
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
