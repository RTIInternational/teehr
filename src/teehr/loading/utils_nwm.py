from pathlib import Path
from typing import Union, Optional, Iterable
from datetime import datetime

import dask
import fsspec
import ujson  # fast json
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd

from teehr.loading.const_nwm import NWM22_RUN_CONFIG, NWM22_ANALYSIS_CONFIG, NWM_BUCKET


@dask.delayed
def gen_json(u: str, fs: fsspec.filesystem, json_dir: str) -> str:
    """Helper function for creating single-file kerchunk reference jsons

    Parameters
    ----------
    u : str
        Path to the file in the remote location (ie, GCS bucket)
    fs : fsspec.filesystem
        fsspec filesystem mapped to GCS
    json_dir : str
        Directory for saving zarr reference json files

    Returns
    -------
    str
        Path to the local zarr reference json file
    """
    so = dict(
        mode="rb", anon=True, default_fill_cache=False, default_cache_type="first"
    )
    with fs.open(u, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, u, inline_threshold=300)
        p = u.split("/")
        date = p[3]
        fname = p[5]
        outf = f"{json_dir}{date}.{fname}.json"
        with open(outf, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())
    return outf


def build_zarr_references(
    component_paths: list[str],
    json_dir: str,
) -> list[str]:
    """Builds the single file zarr json reference files using kerchunk

    Parameters
    ----------
    component_paths : list
        List of remote filepaths
    json_dir : str
        Local directory for caching json files

    Returns
    -------
    list[str]
        List of paths to the zarr reference json files
    """
    json_dir_obj = Path(json_dir)
    if not json_dir_obj.exists():
        json_dir_obj.mkdir(parents=True)

    fs = fsspec.filesystem("gcs", anon=True)

    results = []
    for u in component_paths:
        results.append(gen_json(u, fs, json_dir))
    json_paths = dask.compute(results)[0]

    return sorted(json_paths)


def validate_run_args(run: str, output_type: str, variable: str):
    """Validates user-provided NWMv22 run arguments

    Parameters
    ----------
    run : str
        Run type/configuration
    output_type : str
        Output component of the configuration
    variable : str
        Name of the variable to fetch within the output_type

    Raises
    ------
    KeyError
        Invalid key error
    """
    try:
        NWM22_RUN_CONFIG[run]
    except Exception as e:
        raise ValueError(f"Invalid RUN entry: {str(e)}")
    try:
        NWM22_RUN_CONFIG[run][output_type]
    except Exception as e:
        raise ValueError(f"Invalid RUN entry: {str(e)}")
    if variable not in NWM22_RUN_CONFIG[run][output_type]:
        raise KeyError(f"Invalid VARIABLE_NAME entry: {variable}")


def construct_assim_paths(
    gcs_dir: str,
    run: str,
    output_type: str,
    dates: pd.DatetimeIndex,
    t_minus: Iterable[int],
    run_name_in_filepath: str,
    cycle_z_hours: Iterable[int],
    domain: str,
) -> list[str]:
    """Constructs paths to NWM point assimilation data based on specified parameters. This function
        prioritizes value time over reference time so that only files with value times falling
        within the specified date range are included in the resulting file list.

    Parameters
    ----------
    gcs_dir : str
        Path to the NWM data on GCS
    run : str
        Run type/configuration
    output_type : str
        Output component of the configuration
    dates : pd.DatetimeIndex
        Range of days to fetch data
    t_minus : Iterable[int]
        Collection of lookback hours to include when fetching assimilation data
    run_name_in_filepath : str
        Name of the assimilation run as represented in the GCS file. Defined in const_nwm.py
    cycle_z_hours : Iterable[int]
        The z-hour of the assimilation run per day. Defined in const_nwm.py
    domain : str
        Geographic region covered by the assimilation run. Defined in const_nwm.py

    Returns
    -------
    list[str]
        List of remote filepaths
    """
    component_paths = []

    for dt in dates:
        dt_str = dt.strftime("%Y%m%d")

        # Add the values starting from day 1, skipping value times in the previous day
        if "hawaii" in run:
            for cycle_hr in cycle_z_hours:
                for tm in t_minus:
                    for tm2 in [0, 15, 30, 45]:
                        if (tm * 100 + tm2) > cycle_hr * 100:
                            continue
                        file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.t{cycle_hr:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}{tm2:02d}.{domain}.nc"
                        component_paths.append(file_path)
        else:
            for cycle_hr in cycle_z_hours:
                for tm in t_minus:
                    if tm > cycle_hr:
                        continue
                    file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.t{cycle_hr:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.nc"
                    component_paths.append(file_path)

        # Now add the values from the day following the end day, whose value times that fall within the end day
        if "extend" in run:
            for tm in t_minus:
                dt_add = dt + pd.Timedelta(cycle_hr + 24, unit="hours")
                hr_add = dt_add.hour
                if tm > hr_add:
                    dt_add_str = dt_add.strftime("%Y%m%d")
                    file_path = f"{gcs_dir}/nwm.{dt_add_str}/{run}/nwm.t{hr_add:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.nc"
                    component_paths.append(file_path)

        elif "hawaii" in run:
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
                                file_path = f"{gcs_dir}/nwm.{dt_add_str}/{run}/nwm.t{hr_add:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}{tm2:02d}.{domain}.nc"
                                component_paths.append(file_path)
        else:
            for cycle_hr2 in cycle_z_hours:
                for tm in t_minus:
                    if cycle_hr2 > 0:
                        dt_add = dt + pd.Timedelta(cycle_hr + cycle_hr2, unit="hours")
                        hr_add = dt_add.hour
                        if tm > hr_add:
                            dt_add_str = dt_add.strftime("%Y%m%d")
                            file_path = f"{gcs_dir}/nwm.{dt_add_str}/{run}/nwm.t{hr_add:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.nc"
                            component_paths.append(file_path)

    return sorted(component_paths)


def build_remote_nwm_filelist(
    run: str,
    output_type: str,
    start_dt: Union[str, datetime],
    ingest_days: int,
    t_minus_hours: Optional[Iterable[int]] = None,
) -> list:
    """Assembles a list of remote NWM files in GCS based on specified user parameters

    Parameters
    ----------
    run : str
        Run type/configuration
    output_type : str
        Output component of the configuration
    start_dt : str “YYYY-MM-DD” or datetime
        Date to begin data ingest
    ingest_days : int
        Number of days to ingest data after start date
    t_minus_hours: Iterable[int], only necessary if assimilation data is requested
        Collection of lookback hours to include when fetching assimilation data

    Returns
    -------
    list
        List of remote filepaths
    """
    gcs_dir = f"gs://{NWM_BUCKET}"
    fs = fsspec.filesystem("gcs", anon=True)
    dates = pd.date_range(start=start_dt, periods=ingest_days, freq="1d")

    if "assim" in run:
        cycle_z_hours = NWM22_ANALYSIS_CONFIG[run]["cycle_z_hours"]
        domain = NWM22_ANALYSIS_CONFIG[run]["domain"]
        run_name_in_filepath = NWM22_ANALYSIS_CONFIG[run]["run_name_in_filepath"]
        max_lookback = NWM22_ANALYSIS_CONFIG[run]["num_lookback_hrs"]

        if max(t_minus_hours) > max_lookback - 1:
            raise ValueError(
                f"The maximum specified t-minus hour exceeds the lookback period for this configuration: {run}; max t-minus: {max(t_minus_hours)} hrs; look-back period: {max_lookback} hrs"
            )

        component_paths = construct_assim_paths(
            gcs_dir,
            run,
            output_type,
            dates,
            t_minus_hours,
            run_name_in_filepath,
            cycle_z_hours,
            domain,
        )

    else:
        component_paths = []

        for dt in dates:
            dt_str = dt.strftime("%Y%m%d")
            file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.*.{output_type}*"
            component_paths.extend(fs.glob(file_path))
        component_paths = sorted([f"gs://{path}" for path in component_paths])

    return component_paths
