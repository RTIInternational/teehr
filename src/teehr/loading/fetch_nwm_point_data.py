import os

import fsspec
import xarray as xr
import pandas as pd
import numpy as np


from utils import (
    validate_run_args,
    build_remote_nwm_filelist,
    build_zarr_references,
)


def fetch_and_format_nwm_points(
    multifile_filepath: str,
    location_ids: np.array,
    run: str,
    variable_name: str,
    output_parquet_dir: str,
):
    """
    Reads in the multifile reference json, subsets the NWM data based on provided IDs
    and formats and saves the data as a parquet file

    Parameters
    ----------
    multifile_filepath : str
        Filepath to the output multi-file reference json
    location_ids : np.array
        Array specifying NWM IDs of interest
    run : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    """
    # Access the multifile reference file as an xarray dataset
    fs = fsspec.filesystem("reference", fo=multifile_filepath)
    m = fs.get_mapper("")
    ds = xr.open_zarr(m, consolidated=False)

    # Subset the dataset by IDs of interest
    location_ids = location_ids.astype(float)
    ds_nwm_subset = ds.sel(feature_id=location_ids)

    # For each reference time, fetch the data, format it, and save to parquet
    for ref_time in ds_nwm_subset.reference_time.values:
        # Subset by reference time
        ds_temp = ds_nwm_subset.sel(reference_time=ref_time)
        # Convert to dataframe and do some reformatting
        df_temp = ds_temp[variable_name].to_dataframe()
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
        df_temp["lead_time"] = df_temp["value_time"] - df_temp["reference_time"]
        df_temp["configuration"] = run
        df_temp["variable_name"] = variable_name
        df_temp["location_id"] = df_temp.location_id.astype(int)
        ref_time_str = pd.to_datetime(ref_time).strftime("%Y%m%dT%HZ")
        # Save to parquet
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
    multifile_filepath: str,
    output_parquet_dir: str,
):
    validate_run_args(run, output_type, variable_name)

    lst_component_paths = build_remote_nwm_filelist(
        run, output_type, start_date, ingest_days
    )

    build_zarr_references(lst_component_paths, multifile_filepath, json_dir)

    fetch_and_format_nwm_points(
        multifile_filepath,
        location_ids,
        run,
        variable_name,
        output_parquet_dir,
    )
