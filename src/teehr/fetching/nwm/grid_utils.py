"""Module defining shared functions for processing NWM grid data."""
from pathlib import Path
from typing import Dict, List, Tuple, Union
import re

import dask
import pandas as pd
import xarray as xr
from exactextract import exact_extract, RasterSource, Operation
from geopandas import GeoDataFrame

from teehr.fetching.utils import (
    get_dataset,
    write_timeseries_parquet_file
)
from teehr.models.fetching.utils import TimeseriesTypeEnum
from teehr.fetching.const import (
    VALUE,
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME
)


def get_nwm_grid_data(
    var_da: xr.DataArray,
    row_min: int,
    col_min: int,
    row_max: int,
    col_max: int
):
    """Read a subset nwm grid data into memory using row/col bounds."""
    grid_values = var_da.isel(
        x=slice(col_min, col_max+1), y=slice(row_min, row_max+1)
    ).values
    return grid_values


def update_location_id_prefix(
    df: pd.DataFrame,
    new_prefix: str
) -> pd.DataFrame:
    """Replace or add the location_id prefix in a dataframe."""
    df = df.copy()
    tmp_df = df.location_id.str.split("-", expand=True)

    df[LOCATION_ID] = df[LOCATION_ID].astype(str)

    if tmp_df.columns.size == 1:
        df.loc[:, 'location_id'] = new_prefix + "-" + df['location_id']
    elif tmp_df.columns.size == 2:
        df.loc[:, 'location_id'] = new_prefix + "-" + tmp_df[1]
    else:
        raise ValueError("Location ID has more than two parts!")

    return df


def compute_zonal_stats(
    raster: RasterSource,
    features: GeoDataFrame,
    stats: List[Union[str, Operation]],
    **kwargs
) -> pd.DataFrame:
    """Compute zonal statistics using exactextract."""
    ee_result = exact_extract(raster, features, stats, **kwargs)
    return ee_result


@dask.delayed
def process_single_nwm_grid_file_ee(
    row: Tuple,
    configuration_name: str,
    variable_name: str,
    ignore_missing_file: bool,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, str]],
    features: GeoDataFrame,
    stats: List[Union[str, Operation]],
    **kwargs
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
    value_time = ds.time.values[0]
    da = ds[variable_name][0]

    df = compute_zonal_stats(
        raster=da,
        features=features,
        stats=stats,
        include_cols=LOCATION_ID,
        output="pandas",
        include_geom=False,
        **kwargs
    )

    if variable_mapper:
        variable_name = variable_mapper[VARIABLE_NAME].\
            get(variable_name, variable_name)
        nwm_units = variable_mapper[UNIT_NAME].\
            get(nwm_units, nwm_units)

    # Note: extactextract can calculate more than one stat at a time.
    # Combine the stat and variable name into a new variable_name, then
    # reformat the dataframe (basically stacking vertically).
    dfs = []
    for stat in stats:
        variable_name_stat = f"{variable_name}_{stat}"
        dfs.append(pd.DataFrame(
            {
                LOCATION_ID: df[LOCATION_ID],
                VALUE: df[stat],
                VARIABLE_NAME: variable_name_stat,
                UNIT_NAME: nwm_units,
                VALUE_TIME: value_time,
                REFERENCE_TIME: ref_time,
                CONFIGURATION_NAME: configuration_name
            }
        ))
    stacked_df = pd.concat(dfs, ignore_index=True)

    if location_id_prefix:
        stacked_df = update_location_id_prefix(stacked_df, location_id_prefix)

    return stacked_df


def fetch_and_format_nwm_grids(
    json_paths: List[str],
    configuration_name: str,
    variable_name: str,
    output_parquet_dir: str,
    ignore_missing_file: bool,
    overwrite_output: bool,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, str]],
    timeseries_type: TimeseriesTypeEnum,
    features: GeoDataFrame,
    unique_zone_id: str,
    stats: List[Union[str, Operation]],
    **kwargs
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

        # Note: Instead of looping over each individual file, we can use kerchunk to
        # combine several files into a single dataset, and reduce the number of times
        # exactextract needs to calculate the weights?

        results = []
        for row in df.itertuples():
            results.append(
                process_single_nwm_grid_file_ee(
                    row=row,
                    configuration_name=configuration_name,
                    variable_name=variable_name,
                    ignore_missing_file=ignore_missing_file,
                    location_id_prefix=location_id_prefix,
                    variable_mapper=variable_mapper,
                    features=features,
                    stats=stats,
                    **kwargs
                )
            )

        output = dask.compute(*results)

        output = [df for df in output if df is not None]
        if len(output) == 0:
            raise FileNotFoundError("No NWM files for specified input"
                                    "configuration were found in GCS!")
        z_hour_df = pd.concat(output)

        # Save to parquet.
        yrmoday = df.day.iloc[0]
        z_hour = df.z_hour.iloc[0][1:3]
        ref_time_str = f"{yrmoday}T{z_hour}"
        parquet_filepath = Path(
            Path(output_parquet_dir), f"{ref_time_str}.parquet"
        )
        z_hour_df.sort_values([LOCATION_ID, VALUE_TIME], inplace=True)
        write_timeseries_parquet_file(
            filepath=parquet_filepath,
            overwrite_output=overwrite_output,
            data=z_hour_df,
            timeseries_type=timeseries_type
        )
