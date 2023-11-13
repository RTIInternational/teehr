from pathlib import Path
from typing import Union, Iterable, Optional, List, Dict
from datetime import datetime

import xarray as xr
import numpy as np
import pandas as pd
import dask

from teehr.loading.nwm22.utils_nwm import (
    build_remote_nwm_filelist,
    build_zarr_references,
    get_dataset,
)

from teehr.models.loading.nwm22_grid import GridConfigurationModel

from teehr.loading.nwm22.const_nwm import (
    NWM22_UNIT_LOOKUP, NWM22_ANALYSIS_CONFIG
)


def compute_zonal_mean(
    da: xr.DataArray, weights_filepath: str
) -> pd.DataFrame:
    """Compute zonal mean for given zones and weights"""
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
    weights_df["value"] = var_values * weights_df.weight.values
    # Compute mean
    df = weights_df.groupby(by="location_id")["value"].mean().to_frame()
    df.reset_index(inplace=True)

    return df


@dask.delayed
def process_single_file(
    singlefile: str,
    configuration: str,
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    units_format_dict: Dict
):
    """Compute zonal mean for a single json reference file and format
    to a dataframe using the TEEHR data model"""
    ds = get_dataset(singlefile, ignore_missing_file)
    if not ds:
        return None
    filename = Path(singlefile).name
    yrmoday = filename.split(".")[1]
    z_hour = filename.split(".")[3][1:3]
    ref_time = pd.to_datetime(yrmoday) \
        + pd.to_timedelta(int(z_hour), unit="H")

    nwm22_units = ds[variable_name].attrs["units"]
    teehr_units = units_format_dict.get(nwm22_units, nwm22_units)
    value_time = ds.time.values[0]
    da = ds[variable_name]

    # Calculate mean areal of selected variable
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
        z_hour_df.to_parquet(parquet_filepath)


def nwm_grids_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    zonal_weights_filepath: str,
    json_dir: str,
    output_parquet_dir: str,
    t_minus_hours: Optional[Iterable[int]] = None,
    ignore_missing_file: Optional[bool] = True,
):
    """
    Fetches NWM gridded data, calculates zonal statistics (mean) of selected
    variable for given zones, converts and saves to TEEHR tabular format

    Parameters
    ----------
    configuration : str
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
    zonal_weights_filepath: str
        Path to the array containing fraction of pixel overlap
        for each zone
    json_dir : str
        Directory path for saving json reference files
    output_parquet_dir : str
        Path to the directory for the final parquet files
    t_minus_hours: Optional[Iterable[int]]
        Specifies the look-back hours to include if an assimilation
        configuration is specified.
    ignore_missing_file: bool
        Flag specifying whether or not to fail if a missing NWM file is encountered
        True = skip and continue
        False = fail

    The NWM configuration variables, including configuration, output_type, and
    variable_name are stored as a pydantic model in grid_config_models.py

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHHZ".  The tabular output
    parquet files follow the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries  # noqa
    """

    # Parse input parameters to validate configuration
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }

    _ = GridConfigurationModel.parse_obj(vars)

    component_paths = build_remote_nwm_filelist(
        configuration,
        output_type,
        start_date,
        ingest_days,
        NWM22_ANALYSIS_CONFIG,
        t_minus_hours,
        ignore_missing_file,
    )

    json_paths = build_zarr_references(component_paths,
                                       json_dir,
                                       ignore_missing_file)

    fetch_and_format_nwm_grids(
        json_paths,
        configuration,
        variable_name,
        output_parquet_dir,
        zonal_weights_filepath,
        ignore_missing_file,
        NWM22_UNIT_LOOKUP,
    )


if __name__ == "__main__":
    # Local testing
    single_filepath = "/mnt/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa
    weights_parquet = "/mnt/data/ciroh/wbdhuc10_weights.parquet"
    ignore_missing_file = False

    nwm_grids_to_parquet(
        "forcing_analysis_assim",
        "forcing",
        "RAINRATE",
        "2020-12-18",
        1,
        weights_parquet,
        "/home/sam/forcing_jsons",
        "/home/sam/forcing_parquet",
        [0],
        ignore_missing_file,
    )