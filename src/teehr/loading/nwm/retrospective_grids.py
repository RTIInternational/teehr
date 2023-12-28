import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Optional, Tuple, Dict

import pandas as pd
import xarray as xr
import fsspec
from pydantic import validate_call
import numpy as np
import dask

from teehr.loading.nwm.const import NWM22_UNIT_LOOKUP
from teehr.models.loading.utils import (
    ChunkByEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum
)
from teehr.models.loading.nwm22_grid import ForcingVariablesEnum
from teehr.loading.nwm.utils import write_parquet_file, get_dataset
from teehr.loading.nwm.retrospective_points import (
    format_output_filename,
    validate_start_end_date,
    datetime_to_date
)


def get_data_array(var_da: xr.DataArray, rows: np.array, cols: np.array):
    """Read the forcing variable into memory for the grouped
    timesteps and zone rows and cols."""
    var_arr = var_da.compute().values[:, rows, cols]
    return var_arr


def process_group(
    da_i: xr.DataArray,
    rows: np.array,
    cols: np.array,
    weights_df: pd.DataFrame,
    weight_vals: np.array,
    variable_name: str
):
    """Fetch a chunk of NWM v3.0 gridded data, compute weighted
    values for each zone, and format to dataframe."""
    var_arr = get_data_array(da_i, rows, cols)

    # Get the subset data array of start and end times just for the time values
    time_subset_vals = da_i.time.values

    hourly_dfs = []
    for i, dt in enumerate(time_subset_vals):
        # Calculate weighted pixel values
        weights_df["value"] = var_arr[i, :] * weight_vals
        # Compute mean per group/location
        df = weights_df.groupby(
            by="location_id", observed=False  # TODO: What's 'observed'?
        )["value"].mean().to_frame()
        df["value_time"] = pd.to_datetime(dt)
        df.reset_index(inplace=True)
        hourly_dfs.append(df)

    chunk_df = pd.concat(hourly_dfs)
    chunk_df["reference_time"] = df.value_time
    chunk_df["measurement_unit"] = "some_units"
    chunk_df["configuration"] = "retro"
    chunk_df["variable_name"] = variable_name
    return chunk_df


def construct_nwm21_json_paths(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime]
):
    """Construct the remote paths for the NWM v2.1 zarr json files
    within the specified start and end dates."""
    base_path = (
        "s3://ciroh-nwm-zarr-retrospective-data-copy/"
        "noaa-nwm-retrospective-2-1-zarr-pds/forcing"
    )
    date_rng = pd.date_range(start_date, end_date, freq="H")
    dates = []
    paths = []
    for dt in date_rng:
        dates.append(dt)
        paths.append(
            f"{base_path}/{dt.year}/{dt.year}{dt.month:02d}"
            f"{dt.day:02d}{dt.hour:02d}.LDASIN_DOMAIN1.json"
        )
    paths_df = pd.DataFrame({"datetime": dates, "filepath": paths})
    return paths_df


def compute_zonal_mean(
    var_da: xr.DataArray, weights_filepath: str, time_dt: pd.Timestamp
) -> pd.DataFrame:
    """Compute zonal mean for given zones and weights"""
    weights_df = pd.read_parquet(
        weights_filepath, columns=["row", "col", "weight", "location_id"]
    )
    # Get row/col indices
    rows = weights_df.row.values
    cols = weights_df.col.values
    arr_2d = var_da.compute().values
    var_values = arr_2d[rows, cols]
    # Get the values and apply weights
    weights_df["value"] = var_values * weights_df.weight.values
    # Compute mean
    df = weights_df.groupby(by="location_id")["value"].mean().to_frame()
    df["value_time"] = time_dt
    df.reset_index(inplace=True)
    return df


@dask.delayed
def process_single_file(
    row: Tuple,
    configuration: str,
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    units_format_dict: Dict
):
    """Compute zonal mean for a single json reference file and format
    to a dataframe using the TEEHR data model"""
    ds = get_dataset(
        row.filepath,
        ignore_missing_file,
        target_options={'anon': True}
    )
    if not ds:
        return None

    nwm_units = ds[variable_name].attrs["units"]
    teehr_units = units_format_dict.get(nwm_units, nwm_units)
    value_time = row.datetime
    da = ds[variable_name].isel(Time=0)

    # Calculate mean areal of selected variable
    df = compute_zonal_mean(da, weights_filepath, value_time)

    df["value_time"] = value_time
    df["reference_time"] = value_time
    df["measurement_unit"] = teehr_units
    df["configuration"] = configuration
    df["variable_name"] = variable_name

    return df


@validate_call(config=dict(arbitrary_types_allowed=True))
def nwm_retro_grids_to_parquet(
    nwm_version: SupportedNWMRetroVersionsEnum,
    variable_name: ForcingVariablesEnum,
    zonal_weights_filepath: Union[str, Path],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[ChunkByEnum, None] = None,
    overwrite_output: Optional[bool] = False,
    domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS"
):
    """Fetch NWM v2.1 or v3.0 gridded data, summarize to zones using
    a pre-computed weight file, and save as a Parquet file.

    Parameters
    ----------
    nwm_version: SupportedNWMRetroVersionsEnum
        NWM retrospective version to fetch.
        Currently `nwm21` and `nwm30` supported
    variable_name: str
        Name of the NWM forcing data variable to download.
        (e.g., "PRECIP", "PSFC", "Q2D", ...)
    zonal_weights_filepath: str,
        Path to the array containing fraction of pixel overlap
        for each zone
    start_date: Union[str, datetime, pd.Timestamp]
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
        Rounds down to beginning of day
    end_date: Union[str, datetime, pd.Timestamp],
        Last date to fetch.  Rounds up to end of day
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
    output_parquet_dir: Union[str, Path],
        Directory where output will be saved.
    chunk_by: Union[ChunkByEnum, None] = None,
        If None (default) saves all timeseries to a single file, otherwise
        the data is processed using the specified parameter.
        Can be: 'location_id', 'day', 'week', 'month', or 'year'
    overwrite_output: bool = False,
        Whether output should overwrite files if they exist.  Default is False.
    domain: str = "CONUS"
        Geographical domain when NWM version is v3.0.
        Acceptable values are "Alaska", "CONUS" (default), "Hawaii", and "PR".
        Un-used when NWM version equals v2.1
    Returns
    -------
    None - saves file to specified path

    """
    start_date = datetime_to_date(pd.Timestamp(start_date))
    end_date = (
        datetime_to_date(pd.Timestamp(end_date)) +
        timedelta(days=1) -
        timedelta(minutes=1)
    )

    validate_start_end_date(nwm_version, start_date, end_date)

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    if nwm_version == SupportedNWMRetroVersionsEnum.nwm21:

        # Construct json paths within the selected time
        nwm21_paths = construct_nwm21_json_paths(start_date, end_date)

        if chunk_by is None:
            gps = [(None, nwm21_paths)]

        if chunk_by == "week":
            gps = nwm21_paths.groupby(
                pd.Grouper(key='datetime', axis=0, freq='W', sort=True)
            )

        if chunk_by == "month":
            gps = nwm21_paths.groupby(
                pd.Grouper(key='datetime', axis=0, freq='M', sort=True)
            )

        if chunk_by == "year":
            # TODO: Is this even feasible?
            gps = nwm21_paths.groupby(
                pd.Grouper(key='datetime', axis=0, freq='Y', sort=True)
            )

        for _, df in gps:
            # Process this chunk using dask delayed
            results = []
            for row in df.itertuples():
                results.append(
                    process_single_file(
                        row=row,
                        configuration=nwm_version,
                        variable_name=variable_name,
                        weights_filepath=zonal_weights_filepath,
                        ignore_missing_file=False,
                        units_format_dict=NWM22_UNIT_LOOKUP,
                    )
                )
            output = dask.compute(*results)

            output = [df for df in output if df is not None]
            if len(output) == 0:
                raise FileNotFoundError("No NWM files for specified input"
                                        "configuration were found in GCS!")
            chunk_df = pd.concat(output)

            start = df.datetime.min().strftime("%Y%m%d%H")
            end = df.datetime.max().strftime("%Y%m%d%H")
            output_filename = Path(
                output_parquet_dir,
                f"{start}_{end}_{nwm_version}_retrospective.parquet"
            )
            write_parquet_file(
                filepath=output_filename,
                overwrite_output=overwrite_output,
                data=chunk_df)

    if nwm_version == SupportedNWMRetroVersionsEnum.nwm30:

        if variable_name == "RAINRATE":
            zarr_name = "precip"
        else:
            zarr_name = variable_name.lower()
        s3_zarr_url = (
            f"s3://noaa-nwm-retrospective-3-0-pds/{domain}/"
            f"zarr/forcing/{zarr_name}.zarr"
        )

        var_da = xr.open_zarr(
            fsspec.get_mapper(s3_zarr_url, anon=True),
            chunks={}, consolidated=True
        )[variable_name].sel(time=slice(start_date, end_date))

        # Get weights and their row/col indices
        weights_df = pd.read_parquet(
            zonal_weights_filepath,
            columns=["row", "col", "weight", "location_id"]
        )
        weights_df["location_id"] = weights_df.location_id.astype("category")
        rows = weights_df.row.values
        cols = weights_df.col.values
        weight_vals = weights_df.weight.values

        if chunk_by is None:
            gps = [(None, var_da)]

        if chunk_by == "week":
            gps = var_da.groupby(var_da.time.dt.isocalendar().week)

        if chunk_by == "month":
            gps = var_da.groupby("time.month")

        if chunk_by == "year":
            # TODO: Is this even feasible?
            gps = var_da.groupby("time.year")

        for _, da_i in gps:
            chunk_df = process_group(
                da_i,
                rows,
                cols,
                weights_df,
                weight_vals,
                variable_name
            )

            fname = format_output_filename(da_i, nwm_version)
            output_filename = Path(
                output_parquet_dir,
                fname
            )
            write_parquet_file(
                filepath=output_filename,
                overwrite_output=overwrite_output,
                data=chunk_df)


if __name__ == "__main__":

    t0 = time.time()

    nwm_retro_grids_to_parquet(
        nwm_version="nwm21",
        variable_name="RAINRATE",
        zonal_weights_filepath="/mnt/data/ciroh/wbdhuc10_weights.parquet",
        start_date="2008-05-22 00:00",
        end_date="2008-05-22 23:00",
        output_parquet_dir="/mnt/data/ciroh/retro",
        chunk_by=None
    )

    print(f"Total elapsed: {(time.time() - t0):.2f} secs")
