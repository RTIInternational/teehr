"""Module defining shared functions for processing NWM grid data."""
from pathlib import Path
from typing import Dict, List, Tuple, Union
import re

import dask
import numpy as np
import pandas as pd
import xarray as xr

from teehr.fetching.utils import (
    get_dataset,
    write_parquet_file,
    format_timeseries_data_types
)
from teehr.fetching.const import (
    VALUE,
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME
)


def get_weights_row_col_stats(weights_df: pd.DataFrame) -> Dict:
    """Get row and column statistics for weights dataframe."""
    row_min = weights_df.row.values.min()
    col_min = weights_df.col.values.min()
    row_max = weights_df.row.values.max()
    col_max = weights_df.col.values.max()

    rows_norm = weights_df.row.values - row_min
    cols_norm = weights_df.col.values - col_min
    return {
        "row_min": row_min,
        "row_max": row_max,
        "col_min": col_min,
        "col_max": col_max,
        "rows_norm": rows_norm,
        "cols_norm": cols_norm
    }


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


def compute_weighted_average(
    grid_values: np.ndarray,
    weights_df: pd.DataFrame
) -> pd.DataFrame:
    """Compute weighted average of pixels for given zones and weights."""
    weights_df.loc[:, "weighted_value"] = grid_values * \
        weights_df.weight.values

    # Compute weighted average
    df = weights_df.groupby(
        by=LOCATION_ID, as_index=False)[["weighted_value", "weight"]].sum()
    df.loc[:, VALUE] = df.weighted_value/df.weight

    return df[[LOCATION_ID, VALUE]].copy()


@dask.delayed
def process_single_nwm_grid_file(
    row: Tuple,
    configuration_name: str,
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, str]]
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

    weights_df = pd.read_parquet(
        weights_filepath, columns=["row", "col", "weight", LOCATION_ID]
    )

    weights_bounds = get_weights_row_col_stats(weights_df)

    grid_arr = get_nwm_grid_data(
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

    # Calculate mean areal value of selected variable
    df = compute_weighted_average(grid_values, weights_df)

    if not variable_mapper:
        df.loc[:, UNIT_NAME] = nwm_units
        df.loc[:, VARIABLE_NAME] = variable_name
    else:
        df.loc[:, UNIT_NAME] = variable_mapper[UNIT_NAME].\
            get(nwm_units, nwm_units)
        df.loc[:, VARIABLE_NAME] = variable_mapper[VARIABLE_NAME].\
            get(variable_name, variable_name)

    df.loc[:, VALUE_TIME] = value_time
    df.loc[:, REFERENCE_TIME] = ref_time
    df.loc[:, CONFIGURATION_NAME] = configuration_name

    if location_id_prefix:
        df = update_location_id_prefix(df, location_id_prefix)

    return df


def fetch_and_format_nwm_grids(
    json_paths: List[str],
    configuration_name: str,
    variable_name: str,
    output_parquet_dir: str,
    zonal_weights_filepath: str,
    ignore_missing_file: bool,
    overwrite_output: bool,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, str]]
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
                process_single_nwm_grid_file(
                    row,
                    configuration_name,
                    variable_name,
                    zonal_weights_filepath,
                    ignore_missing_file,
                    location_id_prefix,
                    variable_mapper
                )
            )

        output = dask.compute(*results)

        output = [df for df in output if df is not None]
        if len(output) == 0:
            raise FileNotFoundError("No NWM files for specified input"
                                    "configuration were found in GCS!")
        z_hour_df = pd.concat(output)

        # Assign data types.
        z_hour_df = format_timeseries_data_types(z_hour_df)

        # Save to parquet.
        yrmoday = df.day.iloc[0]
        z_hour = df.z_hour.iloc[0][1:3]
        ref_time_str = f"{yrmoday}T{z_hour}"
        parquet_filepath = Path(
            Path(output_parquet_dir), f"{ref_time_str}.parquet"
        )
        z_hour_df.sort_values([LOCATION_ID, VALUE_TIME], inplace=True)
        write_parquet_file(parquet_filepath, overwrite_output, z_hour_df)
