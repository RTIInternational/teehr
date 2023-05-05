from pathlib import Path
from typing import Union, Iterable, Optional, List
from datetime import datetime

import xarray as xr
import numpy as np
import pandas as pd
import dask
from pydantic import validate_arguments

from teehr.loading.utils_nwm import (
    validate_run_args,
    build_remote_nwm_filelist,
    build_zarr_references,
    get_dataset,
)

from teehr.loading.const_nwm import (
    NWM22_UNIT_LOOKUP,
)


def compute_zonal_mean(
    da: xr.DataArray, weights_filepath: str
) -> pd.DataFrame:
    """Compute zonal mean for given zones and weights"""
    # Read weights file
    weights_df = pd.read_parquet(
        weights_filepath, columns=["row", "col", "weight", "zone"]
    )
    # Get variable data
    arr_2d = da.values[0]
    arr_2d[arr_2d == da.rio.nodata] = np.nan
    # Get row/col indices
    rows = weights_df.row.values
    cols = weights_df.col.values
    # Get the values and apply weights
    var_values = arr_2d[rows, cols]
    weights_df["value"] = var_values * weights_df.weight.values
    # Compute mean
    df = weights_df.groupby(by="zone")["value"].mean().to_frame()
    df.reset_index(inplace=True)

    return df


@dask.delayed
def process_single_file(
    singlefile: str, run: str, variable_name: str, weights_filepath: str
):
    """Compute zonal mean for a single json reference file and format
    to a dataframe using the TEEHR data model"""
    ds = get_dataset(singlefile)
    ref_time = ds.reference_time.values[0]
    nwm22_units = ds[variable_name].attrs["units"]
    teehr_units = NWM22_UNIT_LOOKUP.get(nwm22_units, nwm22_units)
    value_time = ds.time.values[0]
    da = ds[variable_name]

    # Calculate mean areal of selected variable
    df = compute_zonal_mean(da, weights_filepath)

    df["value_time"] = value_time
    df["reference_time"] = ref_time
    df["measurement_unit"] = teehr_units
    df["configuration"] = run
    df["variable_name"] = variable_name

    return df


def fetch_and_format_nwm_grids(
    json_paths: List[str],
    run: str,
    variable_name: str,
    output_parquet_dir: str,
    zonal_weights_filepath: str,
) -> None:
    """
    Reads in the single reference jsons, subsets the NWM data based on
    provided IDs and formats and saves the data as a parquet files
    """
    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    # Format file list into a dataframe and group by reference time
    days = []
    z_hours = []
    for path in json_paths:
        filename = Path(path).name
        days.append(filename.split(".")[1])
        z_hours.append(filename.split(".")[3])
    df_refs = pd.DataFrame(
        {"day": days, "z_hour": z_hours, "filepath": json_paths}
    )
    gps = df_refs.groupby(["day", "z_hour"])

    for gp in gps:
        _, df = gp

        results = []
        for singlefile in df.filepath.tolist():
            results.append(
                process_single_file(
                    singlefile,
                    run,
                    variable_name,
                    zonal_weights_filepath,
                )
            )
        z_hour_df = pd.concat(dask.compute(results)[0])

        # Save to parquet
        yrmoday = df.day.iloc[0]
        z_hour = df.z_hour.iloc[0][1:3]
        ref_time_str = f"{yrmoday}T{z_hour}Z"
        parquet_filepath = Path(
            Path(output_parquet_dir), f"{ref_time_str}.parquet"
        )
        z_hour_df.sort_values(["zone", "value_time"], inplace=True)
        z_hour_df.to_parquet(parquet_filepath)


@validate_arguments
def nwm_grids_to_parquet(
    run: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    zonal_weights_filepath: str,
    json_dir: str,
    output_parquet_dir: str,
    t_minus_hours: Optional[Iterable[int]] = None,
):
    """
    Fetches NWM gridded data, calculates zonal statistics (mean) of selected
    variable for given zones, converts and saves to TEEHR tabular format

    Parameters
    ----------
    run : str
        NWM forecast category.
        (e.g., "analysis_assim", "short_range", ...)
    output_type : str
        Output component of the configuration.
        (e.g., "channel_rt", "reservoir", ...)
    variable_name : str
        Name of the NWM data variable to download.
        (e.g., "streamflow", "velocity", ...)
    start_date : str or datetime
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
    ingest_days : int
        Number of days to ingest data after start date
    json_dir : str
        Directory path for saving json reference files
    zonal_indices_filepath: str
        Path to the json file containing row/col indices of zones
    zonal_weights_filepath: str
        Path to the array containing fraction of pixel overlap
        for each zone
    output_parquet_dir : str
        Path to the directory for the final parquet files
    t_minus_hours: Optional[Iterable[int]]
        Specifies the look-back hours to include if an assimilation
        run is specified.

    The NWM configuration variables, including run, output_type, and
    variable_name are stored in the NWM22_RUN_CONFIG dictionary in
    const_nwm.py.

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHHZ".  The tabular output
    parquet files follow the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries  # noqa
    """
    validate_run_args(run, output_type, variable_name)

    component_paths = build_remote_nwm_filelist(
        run,
        output_type,
        start_date,
        ingest_days,
        t_minus_hours,
    )

    json_paths = build_zarr_references(component_paths, json_dir)

    fetch_and_format_nwm_grids(
        json_paths,
        run,
        variable_name,
        output_parquet_dir,
        zonal_weights_filepath,
    )


if __name__ == "__main__":
    # For local testing
    single_filepath = "/mnt/sf_shared/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa
    weights_json = (
        "/mnt/sf_shared/data/ciroh/wbdhu10_medium_range_weights_SJL.pkl.json"
    )
    weights_parquet = "/mnt/sf_shared/data/ciroh/wbdhuc10_weights.parquet"

    nwm_to_parquet(
        "forcing_medium_range",
        "forcing",
        "RAINRATE",
        "2020-12-18",
        1,
        weights_json,
        "/home/sam/forcing_jsons",
        "/home/sam/forcing_parquet",
    )
