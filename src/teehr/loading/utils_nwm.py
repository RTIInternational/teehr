from pathlib import Path
from typing import Union, Optional, Iterable, List
from datetime import datetime

import dask
import fsspec
import ujson  # fast json
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd
import numpy as np
import xarray as xr
import geopandas as gpd

from teehr.loading.const_nwm import (
    NWM22_RUN_CONFIG,
    NWM22_ANALYSIS_CONFIG,
    NWM_BUCKET,
)


def load_gdf(filepath: Union[str, Path], **kwargs: str) -> gpd.GeoDataFrame:
    """Load any supported geospatial file type into a gdf using GeoPandas"""
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
    gdf = gpd.read_parquet(parquet_filepath)
    return gdf


def np_to_list(t):
    return [a.tolist() for a in t]


def get_dataset(zarr_json: str) -> xr.Dataset:
    """Retrieve a blob from the data service as xarray.Dataset.

    Parameters
    ----------
    blob_name: str, required
        Name of blob to retrieve.

    Returns
    -------
    ds : xarray.Dataset
        The data stored in the blob.

    """
    backend_args = {
        "consolidated": False,
        "storage_options": {
            "fo": zarr_json,
            "remote_protocol": "gcs",
            "remote_options": {"anon": True},
        },
    }
    ds = xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs=backend_args,
    )

    return ds


def list_to_np(lst):
    return tuple([np.array(a) for a in lst])


@dask.delayed
def gen_json(
    remote_path: str, fs: fsspec.filesystem, json_dir: Union[str, Path]
) -> str:
    """Helper function for creating single-file kerchunk reference JSONs.

    Parameters
    ----------
    remote_path : str
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
        mode="rb",
        anon=True,
        default_fill_cache=False,
        default_cache_type="first",  # noqa
    )
    with fs.open(remote_path, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, remote_path, inline_threshold=300)
        p = remote_path.split("/")
        date = p[3]
        fname = p[5]
        outf = str(Path(json_dir, f"{date}.{fname}.json"))
        with open(outf, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())
    return outf


def build_zarr_references(
    remote_paths: List[str], json_dir: Union[str, Path]
) -> list[str]:
    """Builds the single file zarr json reference files using kerchunk.

    Parameters
    ----------
    remote_paths : List[str]
        List of remote filepaths
    json_dir : str or Path
        Local directory for caching json files

    Returns
    -------
    list[str]
        List of paths to the zarr reference json files
    """
    json_dir_path = Path(json_dir)
    if not json_dir_path.exists():
        json_dir_path.mkdir(parents=True)

    fs = fsspec.filesystem("gcs", anon=True)

    results = []
    for path in remote_paths:
        results.append(gen_json(path, fs, json_dir))
    json_paths = dask.compute(results)[0]

    return sorted(json_paths)


def validate_run_args(run: str, output_type: str, variable: str):
    """Validates user-provided NWMv22 run arguments.

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
    """Constructs paths to NWM point assimilation data based on specified
        parameters. This function prioritizes value time over reference
        time so that only files with value times falling within the specified
        date range are included in the resulting file list.

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
        Name of the assimilation run as represented in the GCS file.
        Defined in const_nwm.py
    cycle_z_hours : Iterable[int]
        The z-hour of the assimilation run per day. Defined in const_nwm.py
    domain : str
        Geographic region covered by the assimilation run.
        Defined in const_nwm.py

    Returns
    -------
    list[str]
        List of remote filepaths
    """
    component_paths = []

    for dt in dates:
        dt_str = dt.strftime("%Y%m%d")

        # Add the values starting from day 1,
        # skipping value times in the previous day
        if "hawaii" in run:
            for cycle_hr in cycle_z_hours:
                for tm in t_minus:
                    for tm2 in [0, 15, 30, 45]:
                        if (tm * 100 + tm2) > cycle_hr * 100:
                            continue
                        file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.t{cycle_hr:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}{tm2:02d}.{domain}.nc"  # noqa
                        component_paths.append(file_path)
        else:
            for cycle_hr in cycle_z_hours:
                for tm in t_minus:
                    if tm > cycle_hr:
                        continue
                    file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.t{cycle_hr:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.nc"  # noqa
                    component_paths.append(file_path)

        # Now add the values from the day following the end day,
        # whose value times that fall within the end day
        if "extend" in run:
            for tm in t_minus:
                dt_add = dt + pd.Timedelta(cycle_hr + 24, unit="hours")
                hr_add = dt_add.hour
                if tm > hr_add:
                    dt_add_str = dt_add.strftime("%Y%m%d")
                    file_path = f"{gcs_dir}/nwm.{dt_add_str}/{run}/nwm.t{hr_add:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.nc"  # noqa
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
                                file_path = f"{gcs_dir}/nwm.{dt_add_str}/{run}/nwm.t{hr_add:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}{tm2:02d}.{domain}.nc"  # noqa
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
                            file_path = f"{gcs_dir}/nwm.{dt_add_str}/{run}/nwm.t{hr_add:02d}z.{run_name_in_filepath}.{output_type}.tm{tm:02d}.{domain}.nc"  # noqa
                            component_paths.append(file_path)

    return sorted(component_paths)


def build_remote_nwm_filelist(
    run: str,
    output_type: str,
    start_dt: Union[str, datetime],
    ingest_days: int,
    t_minus_hours: Optional[Iterable[int]],
) -> List[str]:
    """Assembles a list of remote NWM files in GCS based on specified user
        parameters.

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
    t_minus_hours: Iterable[int]
        Only necessary if assimilation data is requested.
        Collection of lookback hours to include when fetching assimilation data

    Returns
    -------
    list
        List of remote filepaths (strings)
    """
    gcs_dir = f"gcs://{NWM_BUCKET}"
    fs = fsspec.filesystem("gcs", anon=True)
    dates = pd.date_range(start=start_dt, periods=ingest_days, freq="1d")

    if "assim" in run:
        cycle_z_hours = NWM22_ANALYSIS_CONFIG[run]["cycle_z_hours"]
        domain = NWM22_ANALYSIS_CONFIG[run]["domain"]
        run_name_in_filepath = NWM22_ANALYSIS_CONFIG[run][
            "run_name_in_filepath"
        ]
        max_lookback = NWM22_ANALYSIS_CONFIG[run]["num_lookback_hrs"]

        if max(t_minus_hours) > max_lookback - 1:
            raise ValueError(
                f"The maximum specified t-minus hour exceeds the lookback "
                f"period for this configuration: {run}; max t-minus: "
                f"{max(t_minus_hours)} hrs; "
                f"look-back period: {max_lookback} hrs"
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
        component_paths = sorted([f"gcs://{path}" for path in component_paths])

    return component_paths
