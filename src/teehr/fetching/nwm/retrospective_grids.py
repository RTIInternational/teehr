"""A module for fetching retrospective NWM gridded data.

The function ``nwm_retro_grids_to_parquet()`` can be used to fetch and format
two different versions of retrospective NWM data (v2.1 and v3.0) for
specified variables and date ranges, and summarize the grid pixels
intersecting polygons provided in the weights file using an area-weighted mean
approach.

Each version of the NWM data is hosted on `AWS S3
<https://registry.opendata.aws/nwm-archive/>`__ and is stored in Zarr format
(v3.0) or as Kerchunk reference files (v2.1). Several options are included
for fetching the data in chunks, including by week or month which can be
specified using the ``chunk_by`` argument.

The NWM v3.0 retrospective forcing Zarr store has dimensions {time: 385704,
x: 3840, and y: 4608}, with a chunking scheme of {time: 672, x: 350, y: 350}.
Only the data variable chunks that intersect the polygons are read into memory.

The NWM v2.1 retrospective forcing data has the same x-y dimensions but is
fetched using Kerchunk reference files, which point to the original hourly
netcdf files, therefore the data is not chunked in the x-y dimensions. This
means that an entire data variable is read into memory regardless of the
spatial bounds of the polygons being processed.

Care must be taken when choosing a ``chunk_by`` value to minimize the amount
of data transferred over the network.

.. note::
   It is recommended to set the ``chunk_by`` parameter to the largest time
   period ('week' or 'month') that will fit into your systems memory
   given the number of polygons being processed.
"""
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, Tuple, Dict
import logging
import numpy as np

import pandas as pd
import xarray as xr
import fsspec
from pydantic import validate_call
import dask

from teehr.fetching.const import (
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME
)
from teehr.models.fetching.utils import (
    NWMChunkByEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum
)
from teehr.models.fetching.nwm22_grid import ForcingVariablesEnum
from teehr.fetching.nwm.grid_utils import (
    update_location_id_prefix,
    compute_weighted_average,
    get_nwm_grid_data,
    get_weights_row_col_stats
)
from teehr.fetching.utils import (
    write_parquet_file,
    get_dataset,
    get_period_start_end_times,
    create_periods_based_on_chunksize,
    format_timeseries_data_types
)
from teehr.fetching.nwm.retrospective_points import (
    format_grouped_filename,
    validate_start_end_date,
)

logger = logging.getLogger(__name__)


def get_nwm21_retro_grid_data(
    var_da: xr.DataArray,
    row_min: int,
    col_min: int,
    row_max: int,
    col_max: int
):
    """Read a subset nwm21 retro grid data into memory from row/col bounds."""
    logger.debug("Getting the nwm21 retro grid data")
    grid_values = var_da.isel(
        west_east=slice(col_min, col_max+1),
        south_north=slice(row_min, row_max+1)
    ).values
    return grid_values


def process_nwm30_retro_group(
    da_i: xr.DataArray,
    weights_filepath: str,
    variable_name: str,
    nwm_version: str,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, str]]
):
    """Compute the weighted average for a chunk of NWM v3.0 data.

    Pixel weights for each zone are defined in weights_df,
    and the output is saved to parquet files.
    """
    logger.debug("Processing NWM v3.0 retro grid data chunk.")
    weights_df = pd.read_parquet(
        weights_filepath, columns=["row", "col", "weight", LOCATION_ID]
    )

    weights_bounds = get_weights_row_col_stats(weights_df)

    grid_arr = get_nwm_grid_data(
        da_i,
        weights_bounds["row_min"],
        weights_bounds["col_min"],
        weights_bounds["row_max"],
        weights_bounds["col_max"]
    )

    hourly_dfs = []
    for i, time in enumerate(da_i.time.values):
        grid_values = grid_arr[
            i,
            weights_bounds["rows_norm"],
            weights_bounds["cols_norm"]
        ]
        df = compute_weighted_average(grid_values, weights_df)
        df.loc[:, VALUE_TIME] = pd.to_datetime(time)
        hourly_dfs.append(df)

    nwm_units = da_i.attrs["units"]
    chunk_df = pd.concat(hourly_dfs)
    if not variable_mapper:
        chunk_df.loc[:, UNIT_NAME] = nwm_units
        chunk_df.loc[:, VARIABLE_NAME] = variable_name.value
    else:
        chunk_df.loc[:, UNIT_NAME] = variable_mapper[UNIT_NAME].\
            get(nwm_units, nwm_units)
        chunk_df.loc[:, VARIABLE_NAME] = variable_mapper[VARIABLE_NAME].\
            get(variable_name, variable_name)

    chunk_df.loc[:, REFERENCE_TIME] = np.nan
    chunk_df.loc[:, CONFIGURATION_NAME] = f"{nwm_version}_retrospective"

    if location_id_prefix:
        chunk_df = update_location_id_prefix(chunk_df, location_id_prefix)

    chunk_df = format_timeseries_data_types(chunk_df)

    return chunk_df


def construct_nwm21_json_paths(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime]
):
    """Construct the remote paths for NWM v2.1 json files as a dataframe."""
    logger.debug("Constructing NWM v2.1 json paths.")
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


@dask.delayed
def process_single_nwm21_retro_grid_file(
    row: Tuple,
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    nwm_version: str,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, str]]
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

    value_time = row.datetime
    da = ds[variable_name].isel(Time=0)

    weights_df = pd.read_parquet(
        weights_filepath, columns=["row", "col", "weight", LOCATION_ID]
    )

    weights_bounds = get_weights_row_col_stats(weights_df)

    grid_arr = get_nwm21_retro_grid_data(
        da,
        weights_bounds["row_min"],
        weights_bounds["col_min"],
        weights_bounds["row_max"],
        weights_bounds["col_max"]
    )
    grid_values = grid_arr[
        weights_bounds["rows_norm"],
        weights_bounds["cols_norm"]
    ]

    # Calculate mean areal of selected variable
    df = compute_weighted_average(grid_values, weights_df)

    if not variable_mapper:
        df.loc[:, UNIT_NAME] = nwm_units
        df.loc[:, VARIABLE_NAME] = variable_name.value
    else:
        df.loc[:, UNIT_NAME] = variable_mapper[UNIT_NAME].\
            get(nwm_units, nwm_units)
        df.loc[:, VARIABLE_NAME] = variable_mapper[VARIABLE_NAME].\
            get(variable_name, variable_name)

    df.loc[:, VALUE_TIME] = value_time
    df.loc[:, REFERENCE_TIME] = np.nan
    df.loc[:, CONFIGURATION_NAME] = f"{nwm_version}_retrospective"

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
    chunk_by: Union[NWMChunkByEnum, None] = None,
    overwrite_output: Optional[bool] = False,
    domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
    location_id_prefix: Optional[Union[str, None]] = None,
    variable_mapper: Dict[str, Dict[str, str]] = None
):
    """Compute the weighted average for NWM v2.1 or v3.0 gridded data.

    Pixel values are summarized to zones based on a pre-computed
    zonal weights file, and the output is saved to parquet files.

    All dates and times within the files and in the file names are in UTC.

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
    chunk_by : Union[NWMChunkByEnum, None] = None,
        If None (default) saves all timeseries to a single file, otherwise
        the data is processed using the specified parameter.
        Can be: 'week' or 'month' for gridded data.
    overwrite_output : bool = False,
        Whether output should overwrite files if they exist.  Default is False.
    domain : str = "CONUS"
        Geographical domain when NWM version is v3.0.
        Acceptable values are "Alaska", "CONUS" (default), "Hawaii", and "PR".
        Only used when NWM version equals v3.0.
    location_id_prefix : Union[str, None]
        Optional location ID prefix to add (prepend) or replace.
    variable_mapper : Dict[str, Dict[str, str]]
        Dictionary mapping NWM variable and unit names to TEEHR variable
        and unit names.

    Notes
    -----
    The location_id values in the zonal weights file are used as
    location ids in the output of this function, unless a prefix is specified
    which will be prepended to the location_id values if none exists, or it
    will replace the existing prefix. It is assumed that the location_id
    follows the pattern '[prefix]-[unique id]'.
    """
    logger.info(
        f"Fetching NWM retrospective grid data, version: {nwm_version}."
    )

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    validate_start_end_date(nwm_version, start_date, end_date)

    # Include the entirety of the specified end day
    end_date = end_date.to_period(freq="D").end_time

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    if nwm_version == SupportedNWMRetroVersionsEnum.nwm21:

        # Construct Kerchunk-json paths within the selected time
        nwm21_paths = construct_nwm21_json_paths(start_date, end_date)

        if chunk_by in ["year"]:
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
                    process_single_nwm21_retro_grid_file(
                        row=row,
                        variable_name=variable_name,
                        weights_filepath=zonal_weights_filepath,
                        ignore_missing_file=False,
                        nwm_version=nwm_version,
                        location_id_prefix=location_id_prefix,
                        variable_mapper=variable_mapper
                    )
                )
            output = dask.compute(*results)

            output = [df for df in output if df is not None]
            if len(output) == 0:
                raise FileNotFoundError("No NWM files for specified input"
                                        "configuration were found in GCS!")
            chunk_df = pd.concat(output)

            start = df.datetime.min().strftime("%Y%m%d")
            end = df.datetime.max().strftime("%Y%m%d")
            if start == end:
                output_filename = Path(output_parquet_dir, f"{start}.parquet")
            else:
                output_filename = Path(
                    output_parquet_dir,
                    f"{start}_{end}.parquet"
                )

            chunk_df = format_timeseries_data_types(chunk_df)

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

            chunk_df = process_nwm30_retro_group(
                da_i=da_i,
                weights_filepath=zonal_weights_filepath,
                variable_name=variable_name,
                nwm_version=nwm_version,
                location_id_prefix=location_id_prefix,
                variable_mapper=variable_mapper
            )

            fname = format_grouped_filename(da_i)
            output_filename = Path(
                output_parquet_dir,
                fname
            )

            write_parquet_file(
                filepath=output_filename,
                overwrite_output=overwrite_output,
                data=chunk_df
            )
