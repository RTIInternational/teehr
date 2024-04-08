"""Module defining shared functions for processing NWM grid data."""
from pathlib import Path
from typing import Dict, List, Tuple
import re

import dask
import numpy as np
import pandas as pd
import xarray as xr

from teehr.loading.nwm.utils import get_dataset, write_parquet_file


def compute_zonal_mean(
    da: xr.DataArray, weights_filepath: str
) -> pd.DataFrame:
    """Compute weighted average of pixels for given zones and weights."""
    # Read weights file
    weights_df = pd.read_parquet(
        weights_filepath, columns=["row", "col", "weight", "location_id"]
    )
    # Get variable data
    arr_2d = da.values[0]
    arr_2d[arr_2d == da.rio.nodata] = np.nan
    # Get row/col indices
    rows = weights_df.row.values
    cols = weights_df.col.values
    # Get the values and apply weights
    var_values = arr_2d[rows, cols]
    weights_df["weighted_value"] = var_values * weights_df.weight.values

    # Compute weighted average
    df = weights_df.groupby(
        by="location_id", as_index=False)[["weighted_value", "weight"]].sum()
    df["value"] = df.weighted_value/df.weight

    return df[["location_id", "value"]]


@dask.delayed
def process_single_file(
    row: Tuple,
    configuration: str,
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    units_format_dict: Dict
) -> pd.DataFrame:
    """Fetch data for a single reference file and compute weighted average."""
    ds = get_dataset(
        row.filepath,
        ignore_missing_file,
        target_options={'anon': True}
    )
    if not ds:
        return None
    yrmoday = row.day
    z_hour = row.z_hour[1:3]
    ref_time = pd.to_datetime(yrmoday) \
        + pd.to_timedelta(int(z_hour), unit="h")

    nwm_units = ds[variable_name].attrs["units"]
    teehr_units = units_format_dict.get(nwm_units, nwm_units)
    value_time = ds.time.values[0]
    da = ds[variable_name]

    # Calculate mean areal value of selected variable
    df = compute_zonal_mean(da, weights_filepath)

    df["value_time"] = value_time
    df["reference_time"] = ref_time
    df["measurement_unit"] = teehr_units
    df["configuration"] = configuration
    df["variable_name"] = variable_name

    return df


def fetch_and_format_nwm_grids(
    json_paths: List[str],
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
    zonal_weights_filepath: str,
    ignore_missing_file: bool,
    units_format_dict: Dict,
    overwrite_output: bool,
):
    """Compute weighted average, grouping by reference time.

    Group a list of json files by reference time and compute the weighted
    average of the variable values for each zone. The results are saved to
    parquet files using TEEHR data model.
    """
    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    # Format file list into a dataframe and group by reference time
    pattern = re.compile(r'[0-9]+')
    days = []
    z_hours = []
    for path in json_paths:
        filename = Path(path).name
        if path.split(":")[0] == "s3":
            # If it's a remote json day and z-hour are in the path
            res = re.findall(pattern, path)
            days.append(res[1])
            z_hours.append(f"t{res[2]}z")
        else:
            days.append(filename.split(".")[1])
            z_hours.append(filename.split(".")[3])
    df_refs = pd.DataFrame(
        {"day": days, "z_hour": z_hours, "filepath": json_paths}
    )
    gps = df_refs.groupby(["day", "z_hour"])

    for gp in gps:
        _, df = gp

        results = []
        for row in df.itertuples():
            results.append(
                process_single_file(
                    row,
                    configuration,
                    variable_name,
                    zonal_weights_filepath,
                    ignore_missing_file,
                    units_format_dict,
                )
            )

        output = dask.compute(*results)

        output = [df for df in output if df is not None]
        if len(output) == 0:
            raise FileNotFoundError("No NWM files for specified input"
                                    "configuration were found in GCS!")
        z_hour_df = pd.concat(output)

        # Save to parquet
        yrmoday = df.day.iloc[0]
        z_hour = df.z_hour.iloc[0][1:3]
        ref_time_str = f"{yrmoday}T{z_hour}Z"
        parquet_filepath = Path(
            Path(output_parquet_dir), f"{ref_time_str}.parquet"
        )
        z_hour_df.sort_values(["location_id", "value_time"], inplace=True)
        write_parquet_file(parquet_filepath, overwrite_output, z_hour_df)
