"""A module for loading retrospective NWM gridded data."""
from datetime import datetime
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
from teehr.loading.nwm.grid_utils import update_location_id_prefix
from teehr.loading.nwm.utils import (
    write_parquet_file,
    get_dataset,
    get_period_start_end_times,
    create_periods_based_on_chunksize
)
from teehr.loading.nwm.retrospective_points import (
    format_grouped_filename,
    validate_start_end_date,
)


def get_data_array(var_da: xr.DataArray, rows: np.array, cols: np.array):
    """Read a subset of the data array into memory."""
    var_arr = var_da.values[:, rows, cols]
    return var_arr


def process_group(
    da_i: xr.DataArray,
    rows: np.array,
    cols: np.array,
    weights_df: pd.DataFrame,
    weight_vals: np.array,
    variable_name: str,
    units_format_dict: Dict,
    nwm_version: str,
    location_id_prefix: Union[str, None]
):
    """Compute the weighted average for a chunk of NWM v3.0 data.

    Pixel weights for each zone are defined in weights_df,
    and the output is saved to parquet files.
    """
    var_arr = get_data_array(da_i, rows, cols)

    # Get the subset data array of start and end times just for the time values
    time_subset_vals = da_i.time.values

    hourly_dfs = []
    for i, dt in enumerate(time_subset_vals):
        # Calculate weighted pixel values
        weights_df["value"] = var_arr[i, :] * weight_vals
        # Compute mean per group/location
        df = weights_df.groupby(
            by="location_id", observed=True
        )["value"].mean().to_frame()
        df["value_time"] = pd.to_datetime(dt)
        df.reset_index(inplace=True)
        hourly_dfs.append(df)

    chunk_df = pd.concat(hourly_dfs)
    chunk_df["reference_time"] = chunk_df.value_time
    nwm_units = da_i.attrs["units"]
    teehr_units = units_format_dict.get(nwm_units, nwm_units)
    chunk_df["measurement_unit"] = teehr_units
    chunk_df["configuration"] = f"{nwm_version}_retrospective"
    chunk_df["variable_name"] = variable_name

    if location_id_prefix:
        chunk_df = update_location_id_prefix(chunk_df, location_id_prefix)

    return chunk_df


def construct_nwm21_json_paths(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime]
):
    """Construct the remote paths for NWM v2.1 json files as a dataframe."""
    base_path = (
        "s3://ciroh-nwm-zarr-retrospective-data-copy/"
        "noaa-nwm-retrospective-2-1-zarr-pds/forcing"
    )
    date_rng = pd.date_range(start_date, end_date, freq="h")
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
    """Compute the zonal mean for given zones and weights."""
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
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    units_format_dict: Dict,
    nwm_version: str,
    location_id_prefix: Union[str, None]
):
    """Compute the zonal mean for a single json reference file.

    Results are formatted to a dataframe using the TEEHR data model.
    """
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
    df["configuration"] = f"{nwm_version}_retrospective"
    df["variable_name"] = variable_name

    if location_id_prefix:
        df = update_location_id_prefix(df, location_id_prefix)

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
    domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
    location_id_prefix: Optional[Union[str, None]] = None
):
    """Compute the weighted average for NWM v2.1 or v3.0 gridded data.

    Pixel values are summarized to zones based on a pre-computed
    zonal weights file, and the output is saved to parquet files.

    Parameters
    ----------
    nwm_version : SupportedNWMRetroVersionsEnum
        NWM retrospective version to fetch.
        Currently `nwm21` and `nwm30` supported.
    variable_name : str
        Name of the NWM forcing data variable to download.
        (e.g., "PRECIP", "PSFC", "Q2D", ...).
    zonal_weights_filepath : str,
        Path to the array containing fraction of pixel overlap
        for each zone. The values in the location_id field from
        the zonal weights file are used in the output of this function.
    start_date : Union[str, datetime, pd.Timestamp]
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY.
        Rounds down to beginning of day.
    end_date : Union[str, datetime, pd.Timestamp],
        Last date to fetch.  Rounds up to end of day.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY.
    output_parquet_dir : Union[str, Path],
        Directory where output will be saved.
    chunk_by : Union[ChunkByEnum, None] = None,
        If None (default) saves all timeseries to a single file, otherwise
        the data is processed using the specified parameter.
        Can be: 'location_id', 'day', 'week', 'month', or 'year'.
    overwrite_output : bool = False,
        Whether output should overwrite files if they exist.  Default is False.
    domain : str = "CONUS"
        Geographical domain when NWM version is v3.0.
        Acceptable values are "Alaska", "CONUS" (default), "Hawaii", and "PR".
        Only used when NWM version equals v3.0.
    location_id_prefix : Union[str, None]
        Optional prefix to add to or replace in the output location_id values.

    Notes
    -----
    The location_id values in the zonal weights file are used as location ids
    in the output of this function, unless a prefix is specified.
    """
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    validate_start_end_date(nwm_version, start_date, end_date)

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    if nwm_version == SupportedNWMRetroVersionsEnum.nwm21:

        # Construct Kerchunk-json paths within the selected time
        nwm21_paths = construct_nwm21_json_paths(start_date, end_date)

        if chunk_by in ["year", "location_id"]:
            raise ValueError(
                f"Chunkby '{chunk_by}' is not implemented for gridded data"
            )

        periods = create_periods_based_on_chunksize(
            start_date=start_date,
            end_date=end_date,
            chunk_by=chunk_by
        )

        for period in periods:
            if period is not None:
                dts = get_period_start_end_times(
                    period=period,
                    start_date=start_date,
                    end_date=end_date
                )
                df = nwm21_paths[nwm21_paths.datetime.between(
                    dts["start_dt"], dts["end_dt"]
                )].copy()
            else:
                df = nwm21_paths.copy()

            # Process this chunk using dask delayed
            results = []
            for row in df.itertuples():
                results.append(
                    process_single_file(
                        row=row,
                        variable_name=variable_name,
                        weights_filepath=zonal_weights_filepath,
                        ignore_missing_file=False,
                        units_format_dict=NWM22_UNIT_LOOKUP,
                        nwm_version=nwm_version,
                        location_id_prefix=location_id_prefix
                    )
                )
            output = dask.compute(*results)

            output = [df for df in output if df is not None]
            if len(output) == 0:
                raise FileNotFoundError("No NWM files for specified input"
                                        "configuration were found in GCS!")
            chunk_df = pd.concat(output)

            start = df.datetime.min().strftime("%Y%m%dZ")
            end = df.datetime.max().strftime("%Y%m%dZ")
            if start == end:
                output_filename = Path(output_parquet_dir, f"{start}.parquet")
            else:
                output_filename = Path(
                    output_parquet_dir,
                    f"{start}_{end}.parquet"
                )

            write_parquet_file(
                filepath=output_filename,
                overwrite_output=overwrite_output,
                data=chunk_df)

    if nwm_version == SupportedNWMRetroVersionsEnum.nwm30:

        # Construct the path to the zarr store
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

        if chunk_by in ["year", "location_id"]:
            raise ValueError(
                f"Chunkby '{chunk_by}' is not implemented for gridded data"
            )

        periods = create_periods_based_on_chunksize(
            start_date=start_date,
            end_date=end_date,
            chunk_by=chunk_by
        )

        for period in periods:

            if period is not None:
                dts = get_period_start_end_times(
                    period=period,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                dts = {"start_dt": start_date, "end_dt": end_date}

            da_i = var_da.sel(time=slice(dts["start_dt"], dts["end_dt"]))

            chunk_df = process_group(
                da_i=da_i,
                rows=rows,
                cols=cols,
                weights_df=weights_df,
                weight_vals=weight_vals,
                variable_name=variable_name,
                units_format_dict=NWM22_UNIT_LOOKUP,
                nwm_version=nwm_version,
                location_id_prefix=location_id_prefix
            )

            fname = format_grouped_filename(da_i)
            output_filename = Path(
                output_parquet_dir,
                fname
            )

            write_parquet_file(
                filepath=output_filename,
                overwrite_output=overwrite_output,
                data=chunk_df)


# if __name__ == "__main__":

#     # t0 = time.time()

#     nwm_retro_grids_to_parquet(
#         nwm_version="nwm21",
#         variable_name="RAINRATE",
#         zonal_weights_filepath="/mnt/data/merit/YalansBasins/cat_pfaf_7_conus_subset_nwm_v30_weights.parquet",
#         start_date="2007-09-01 00:00",
#         end_date="2008-3-22 23:00",
#         output_parquet_dir="/mnt/data/ciroh/retro",
#         overwrite_output=True,
#         chunk_by="week"
#     )

#     # print(f"Total elapsed: {(time.time() - t0):.2f} secs")
