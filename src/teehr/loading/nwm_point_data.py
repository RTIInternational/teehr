import os

import fsspec
import xarray as xr
import pandas as pd
import numpy as np
from kerchunk.combine import MultiZarrToZarr
import dask


from teehr.utils import (
    validate_run_args,
    build_remote_nwm_filelist,
    build_zarr_references,
)


def fetch_and_format_nwm_points(
    lst_json_paths: list,
    location_ids: np.array,
    run: str,
    variable_name: str,
    output_parquet_dir: str,
    concat_dims=["time"],
):
    """
    Reads in the multifile reference json, subsets the NWM data based on provided IDs
    and formats and saves the data as a parquet file

    Parameters
    ----------
    location_ids : np.array
        Array specifying NWM IDs of interest
    run : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    """
    # Format file list into a dataframe and group by reference time
    days = []
    filepaths = []
    tzs = []
    for path in lst_json_paths:
        filename = path.split("/")[6]
        filepaths.append(path)
        days.append(filename.split(".")[1])
        tzs.append(filename.split(".")[3])
    df_refs = pd.DataFrame({"day": days, "tz": tzs, "filepath": filepaths})
    gps = df_refs.groupby(["day", "tz"])
    
    out = dask.compute(*[dask.delayed(fetch_and_format)(gp, location_ids, run, variable_name, output_parquet_dir, concat_dims) for gp in gps], retries=1,)
        

def fetch_and_format(gp, location_ids, run, variable_name, output_parquet_dir, concat_dims):
    tpl, df = gp
    yrmoday, tz = tpl
    mzz = MultiZarrToZarr(
        df.filepath.tolist(),
        remote_protocol="gcs",
        remote_options={"anon": True},
        concat_dims=concat_dims,
    )
    json = mzz.translate()
    # opts = {"skip_instance_cache": True}
    fs = fsspec.filesystem("reference", fo=json)  # , ref_storage_opts=opts
    m = fs.get_mapper("")
    ds_nwm_subset = xr.open_zarr(m, consolidated=False, chunks={}).sel(feature_id=location_ids)  
    # Convert to dataframe and do some reformatting
    df_temp = ds_nwm_subset[variable_name].to_dataframe()
    df_temp.reset_index(inplace=True)
    df_temp.rename(
        columns={
            variable_name: "value",
            "time": "value_time",
            "feature_id": "location_id",
        },
        inplace=True,
    )
    df_temp.dropna(subset=["value"], inplace=True)
    if ds_nwm_subset[variable_name].units == "m3 s-1":
        df_temp["value"] = df_temp["value"] / (0.3048**3)
        df_temp["measurement_unit"] = "ft3/s"
    else:
        df_temp["measurement_unit"] = ds_nwm_subset[variable_name].units
    ref_time = ds_nwm_subset.reference_time.values[0]
    df_temp["reference_time"] = ref_time
    df_temp["lead_time"] = df_temp["value_time"] - df_temp["reference_time"]
    df_temp["configuration"] = run
    df_temp["variable_name"] = variable_name
    df_temp["location_id"] = df_temp.location_id.astype(int)
    # Save to parquet
    ref_time_str = pd.to_datetime(ref_time).strftime("%Y%m%dT%HZ")
    parquet_filepath = os.path.join(output_parquet_dir, f"{ref_time_str}.parquet")
    df_temp.to_parquet(parquet_filepath)


def nwm_to_parquet(
    run: str,
    output_type: str,
    variable_name: str,
    start_date: str,
    ingest_days: int,
    location_ids: np.array,
    json_dir: str,
    output_parquet_dir: str,
):
    validate_run_args(run, output_type, variable_name)

    lst_component_paths = build_remote_nwm_filelist(
        run, output_type, start_date, ingest_days
    )

    lst_json_paths = build_zarr_references(lst_component_paths, json_dir)

    fetch_and_format_nwm_points(
        lst_json_paths,
        location_ids,
        run,
        variable_name,
        output_parquet_dir,
    )
