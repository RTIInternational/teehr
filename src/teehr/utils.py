import os

import dask
import fsspec
import ujson  # fast json
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd

from teehr.const import NWM22_RUN_CONFIG, NWM22_ANALYSIS_CONFIG, NWM_BUCKET


def build_zarr_references(
    lst_component_paths: list,
    json_dir: str,
):
    def gen_json(u: str, fs: fsspec.filesystem, json_dir: str) -> str:
        """Helper function for creating single-file kerchunk reference jsons"""
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

    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
    fs = fsspec.filesystem("gcs", anon=True)

    json_paths = dask.compute(
        *[dask.delayed(gen_json)(u, fs, json_dir) for u in lst_component_paths],
        retries=1,
    )
    return sorted(json_paths)


def validate_run_args(run: str, output_type: str, variable: str):
    try:
        NWM22_RUN_CONFIG[run]
    except Exception as e:
        print(f"Invalid RUN entry: {str(e)}") 
        raise
        
    try:
        NWM22_RUN_CONFIG[run][output_type]
    except Exception as e:
        print(f"Invalid OUTPUT_TYPE entry: {str(e)}")
        raise

    if variable not in NWM22_RUN_CONFIG[run][output_type]:
        raise KeyError(f"Invalid VARIABLE_NAME entry: {variable}")


def build_remote_nwm_filelist(
    run: str, output_type: str, start_dt: str, ingest_days: int
) -> list:
    gcs_dir = f"gs://{NWM_BUCKET}"
    fs = fsspec.filesystem("gcs", anon=True)
    lst_component_paths = []

    if "assim" in run:
        timestep = NWM22_ANALYSIS_CONFIG[run]["timestep"]
        tm = NWM22_ANALYSIS_CONFIG[run]["tm"]
        offset = pd.Timedelta(timestep) * tm
        end_dt = pd.to_datetime(start_dt) + pd.Timedelta(ingest_days, unit="day")
        time_increments = pd.date_range(
            start=start_dt, end=end_dt, freq="1H", inclusive="left"
        )
        time_increments = time_increments + offset
        for dt in time_increments:
            tz = dt.hour
            dt_str = dt.strftime("%Y%m%d")
            file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.t{tz:02d}z.analysis_assim*.{output_type}*"
            lst_component_paths.extend(fs.glob(file_path))
    else:
        dates = pd.date_range(start=start_dt, periods=ingest_days, freq="1d")
        for dt in dates:
            dt_str = dt.strftime("%Y%m%d")
            file_path = f"{gcs_dir}/nwm.{dt_str}/{run}/nwm.*.{output_type}*"
            lst_component_paths.extend(fs.glob(file_path))

    lst_component_paths = sorted([f"gs://{path}" for path in lst_component_paths])

    return lst_component_paths
