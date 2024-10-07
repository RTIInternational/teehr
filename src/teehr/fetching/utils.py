"""Module defining common utilities for fetching and processing NWM data."""
from pathlib import Path
from typing import Union, Optional, Iterable, List, Dict
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
import logging

import dask
import fsspec
import ujson  # fast json
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd
import numpy as np
import xarray as xr
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

from teehr.models.fetching.utils import (
    SupportedKerchunkMethod
)
from teehr.models.fetching.utils import (
    SupportedNWMOperationalVersionsEnum,
    NWMChunkByEnum
)
from teehr.fetching.const import (
    NWM_BUCKET,
    NWM_S3_JSON_PATH,
    NWM30_START_DATE,
    TIMESERIES_DATA_TYPES,
    VALUE,
    VALUE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME
)


logger = logging.getLogger(__name__)


def format_timeseries_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert field types to TEEHR data model.

    Notes
    -----
    dataretrieval attempts to return values in UTC, here we explicitly convert
    to be sure. We also drop timezone information and convert to datetime64[ms].

    The fields types are specified in the TIMESERIES_DATA_TYPES dictionary.
    """
    logger.debug("Formatting timeseries data types.")

    # Convert to UTC if not already in UTC.
    if df[VALUE_TIME].dt.tz is not None:
        df[VALUE_TIME] = df[VALUE_TIME].dt.tz_convert("UTC")
    # Drop timezone information.
    df[VALUE_TIME] = df[VALUE_TIME].dt.tz_localize(None)
    # Convert to datetime64[ms].
    df[VALUE_TIME] = df[VALUE_TIME].astype(TIMESERIES_DATA_TYPES[VALUE_TIME])  # noqa
    # Convert remaining fields.
    df[VALUE] = df[VALUE].astype(TIMESERIES_DATA_TYPES[VALUE])
    df[UNIT_NAME] = df[UNIT_NAME].astype(TIMESERIES_DATA_TYPES[UNIT_NAME])  # noqa
    df[VARIABLE_NAME] = df[VARIABLE_NAME].astype(TIMESERIES_DATA_TYPES[VARIABLE_NAME])  # noqa
    df[CONFIGURATION_NAME] = df[CONFIGURATION_NAME].astype(TIMESERIES_DATA_TYPES[CONFIGURATION_NAME])  # noqa
    df[LOCATION_ID] = df[LOCATION_ID].astype(TIMESERIES_DATA_TYPES[LOCATION_ID])  # noqa

    return df


def check_dates_against_nwm_version(
    nwm_version: str,
    start_date: Union[str, datetime],
    ingest_days: int
):
    """Make sure start/end dates work with specified NWM version."""
    logger.debug("Checking dates against NWM version.")

    if isinstance(start_date, str):
        start_date = parse(start_date)

    if (
        (nwm_version == SupportedNWMOperationalVersionsEnum.nwm30) &
        (start_date < NWM30_START_DATE)
    ):
        raise ValueError(
            f"The specified start date ({start_date}) is before the NWM "
            f"v3.0 release date ({NWM30_START_DATE})"
        )

    end_date = start_date + timedelta(days=ingest_days)
    if (
        (nwm_version == SupportedNWMOperationalVersionsEnum.nwm22) &
        (end_date > NWM30_START_DATE)
    ):
        raise ValueError(
            f"The specified end date ({end_date}) is after the NWM "
            f"v2.2 to v3.0 transition date ({NWM30_START_DATE})"
        )


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


def write_parquet_file(
    filepath: Path,
    overwrite_output: bool,
    data: Union[pa.Table, pd.DataFrame]
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

    if not filepath.is_file():
        if isinstance(data, pa.Table):
            pq.write_table(data, filepath)
        else:
            data.to_parquet(filepath)
    elif filepath.is_file() and overwrite_output:
        logger.info(f"Overwriting {filepath.name}")
        if isinstance(data, pa.Table):
            pq.write_table(data, filepath)
        else:
            data.to_parquet(filepath)
    elif filepath.is_file() and not overwrite_output:
        logger.info(
            f"{filepath.name} already exists and overwrite_output=False;"
            " skipping"
        )


def load_gdf(filepath: Union[str, Path], **kwargs: str) -> gpd.GeoDataFrame:
    """Load any supported geospatial file type into a gdf using GeoPandas."""
    logger.debug(f"Loading geospatial file: {filepath}")

    try:
        gdf = gpd.read_file(filepath, **kwargs)
        return gdf
    except Exception:
        pass
    try:
        gdf = gpd.read_parquet(filepath, **kwargs)
        return gdf
    except Exception:
        pass
    try:
        gdf = gpd.read_feather(filepath, **kwargs)
        return gdf
    except Exception:
        raise Exception("Unsupported zone polygon file type")


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

    fs = fsspec.filesystem("gcs", anon=True)

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
        # whose value times that fall within the end day
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


def build_remote_nwm_filelist(
    configuration: str,
    output_type: str,
    start_dt: Union[str, datetime],
    ingest_days: int,
    analysis_config_dict: Dict,
    t_minus_hours: Optional[Iterable[int]],
    ignore_missing_file: Optional[bool],
    prioritize_analysis_valid_time: Optional[bool]
) -> List[str]:
    """Assemble a list of remote NWM files based on user parameters.

    Parameters
    ----------
    configuration : str
        Configuration type.
    output_type : str
        Output component of the configuration.
    start_dt : str “YYYY-MM-DD” or datetime
        Date to begin data ingest.
    ingest_days : int
        Number of days to ingest data after start date.
    t_minus_hours : Iterable[int]
        Only necessary if assimilation data is requested.
        Collection of lookback hours to include when fetching
        assimilation data.
    ignore_missing_file : bool
        Flag specifying whether or not to fail if a missing
        NWM file is encountered
        True = skip and continue
        False = fail.
    prioritize_analysis_valid_time : Optional[bool]
        A boolean flag that determines the method of fetching analysis data.
        When False (default), all hours of the reference time are included in the
        output. When True, only the hours within t_minus_hours are included.

    Returns
    -------
    list
        List of remote filepaths (strings).
    """
    logger.debug("Building remote NWM file list from GCS.")

    gcs_dir = f"gcs://{NWM_BUCKET}"
    fs = fsspec.filesystem("gcs", anon=True)
    dates = pd.date_range(start=start_dt, periods=ingest_days, freq="1d")

    if "assim" in configuration and prioritize_analysis_valid_time:
        cycle_z_hours = analysis_config_dict[configuration]["cycle_z_hours"]
        domain = analysis_config_dict[configuration]["domain"]
        configuration_name_in_filepath = analysis_config_dict[configuration][
            "configuration_name_in_filepath"
        ]
        max_lookback = analysis_config_dict[configuration]["num_lookback_hrs"]

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
    start_date: datetime,
    end_date: datetime,
    chunk_by: Union[NWMChunkByEnum, None]
) -> List[pd.Period]:
    """Create a list of periods of a given frequency, start, and end time.

    Parameters
    ----------
    start_date : datetime
        The start date.
    end_date : datetime
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
