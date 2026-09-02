"""Module defining shared functions for processing NWM grid data."""
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import functools
import logging

import numpy as np
import pandas as pd
import xarray as xr

import teehr.models.pandera_dataframe_schemas as schemas
from obspec_utils.registry import ObjectStoreRegistry

from teehr.fetching.utils import (
    build_kerchunk_registry,
    map_variable_and_unit_name,
    open_kerchunk_dataset,
    write_timeseries_parquet_file,
    parse_nwm_json_paths,
    format_nwm_configuration_metadata,
    convert_value_from_kelvin_to_celsius
)
from teehr.utils.concurrency import run_concurrent_map
from teehr.fetching.models.utils import TimeseriesTypeEnum
from teehr.fetching.const import (
    VALUE,
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME
)

logger = logging.getLogger(__name__)


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
    col_max: int,
    x_dim: str = "x",
    y_dim: str = "y",
):
    """Read a subset nwm grid data into memory using row/col bounds.

    ``x_dim``/``y_dim`` default to NWM's usual "x"/"y" dimension names, but
    can be overridden for grids that use different names (e.g. NWM v2.1
    retrospective forcing's "west_east"/"south_north").
    """
    grid_values = var_da.isel(
        **{x_dim: slice(col_min, col_max + 1), y_dim: slice(row_min, row_max + 1)}
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
    """Coverage-weighted mean of grid pixels for each zone.

    Parameters
    ----------
    grid_values : np.ndarray
        Pixel values aligned row-for-row with ``weights_df``. Either
        ``(n_pixels,)`` for one timestep or ``(n_times, n_pixels)`` for several.
    weights_df : pd.DataFrame
        Weights with ``location_id`` and ``weight`` columns.

    Returns
    -------
    pd.DataFrame
        ``location_id`` and ``value``, plus ``time_index`` when
        ``grid_values`` is 2-D.

    Raises
    ------
    ValueError
        If a zone's weights sum to zero, which would make the mean undefined.
    """
    values = np.asarray(grid_values)
    single_step = values.ndim == 1
    if single_step:
        values = values[np.newaxis, :]

    # Factorize (hash-based) rather than sorting the label column, then sort
    # only the unique labels and remap -- sorting millions of location strings
    # dominates otherwise.
    codes, unique_locations = pd.factorize(weights_df[LOCATION_ID].to_numpy())
    label_order = np.argsort(unique_locations)
    rank = np.empty_like(label_order)
    rank[label_order] = np.arange(label_order.size)
    codes = rank[codes]
    unique_locations = unique_locations[label_order]
    weights = weights_df["weight"].to_numpy("float64")

    n_zones = len(unique_locations)
    total_weight = np.bincount(codes, weights=weights, minlength=n_zones)
    if not (total_weight > 0).all():
        empty = unique_locations[total_weight <= 0]
        raise ValueError(
            f"Total coverage weight is 0 for {empty.size} location(s), "
            f"e.g. {empty[:5].tolist()}."
        )

    # Accumulate in float64; the weights and values are float32 and a zone can
    # cover thousands of pixels.
    weighted = np.stack([
        np.bincount(codes, weights=weights * step, minlength=n_zones)
        for step in values.astype("float64")
    ])
    means = weighted / total_weight

    if single_step:
        return pd.DataFrame({LOCATION_ID: unique_locations, VALUE: means[0]})
    return pd.DataFrame({
        "time_index": np.repeat(np.arange(means.shape[0]), n_zones),
        LOCATION_ID: np.tile(unique_locations, means.shape[0]),
        VALUE: means.ravel(),
    })


def read_and_validate_weights_file(
    weights_filepath: str
) -> pd.DataFrame:
    """Read weights file from parquet, validating data types."""
    schema = schemas.weights_file_schema()
    weights_df = pd.read_parquet(
        weights_filepath, columns=list(schema.columns.keys())
    )
    return schema.validate(weights_df)


def process_single_nwm_grid_file(
    row: Tuple,
    configuration_name: str,
    variable_name: str,
    weights_filepath: str,
    ignore_missing_file: bool,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, Dict[str, str]]],
    registry: Optional[ObjectStoreRegistry] = None,
) -> pd.DataFrame:
    """Fetch data for a single reference file and compute weighted average."""
    # get_nwm_grid_data's .isel(x=..., y=...) only needs positions, not real
    # x/y coordinate values -- but xarray's .isel() still re-indexes the x/y
    # coordinate variables themselves to keep them aligned with the sliced
    # data, which fails if they're still virtual (unmaterialized) arrays, so
    # x/y must be materialized here even though their values are unused.
    ds = open_kerchunk_dataset(
        row.filepath,
        loadable_variables=[variable_name, "time", "x", "y"],
        ignore_missing_file=ignore_missing_file,
        registry=registry,
    )
    if ds is None:
        return None
    yrmoday = row.day
    z_hour = row.z_hour[1:3]
    ref_time = pd.to_datetime(yrmoday) \
        + pd.to_timedelta(int(z_hour), unit="h")

    nwm_units = ds[variable_name].attrs["units"]
    value_time = ds.time.values[0]
    da = ds[variable_name][0]

    weights_df = read_and_validate_weights_file(weights_filepath)

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

    teehr_variable_name, teehr_units = map_variable_and_unit_name(
        variable_name, nwm_units, variable_mapper
    )
    df.loc[:, UNIT_NAME] = teehr_units
    df.loc[:, VARIABLE_NAME] = teehr_variable_name

    df.loc[:, VALUE_TIME] = value_time
    df.loc[:, REFERENCE_TIME] = ref_time
    df.loc[:, CONFIGURATION_NAME] = configuration_name

    if location_id_prefix:
        df = update_location_id_prefix(df, location_id_prefix)

    return df


def fetch_and_format_nwm_grids(
    json_paths: List[str],
    nwm_configuration_name: str,
    nwm_version: str,
    variable_name: str,
    output_parquet_dir: str,
    zonal_weights_filepath: str,
    ignore_missing_file: bool,
    overwrite_output: bool,
    location_id_prefix: Union[str, None],
    variable_mapper: Dict[str, Dict[str, Dict[str, str]]],
    timeseries_type: TimeseriesTypeEnum,
    drop_overlapping_assimilation_values: bool,
    convert_k_to_c: bool = True,
    cpu_workers: Optional[int] = None
):
    """Compute weighted average, grouping by reference time.

    Group a list of json files by reference time and compute the weighted
    average of the variable values for each zone. The results are saved to
    parquet files using TEEHR data model.

    ``cpu_workers`` bounds how many files are processed at once -- one file
    per worker, so it is the only budget that applies here.
    """
    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    # Format file list into a dataframe and group by reference time
    df_refs = parse_nwm_json_paths(
        json_paths=json_paths
    )

    gps = df_refs.groupby(["day", "z_hour"])

    teehr_config = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )

    # Built once from every file in the run (not per group) so obstore's
    # stores/connection pools are reused across all groups below.
    registry = build_kerchunk_registry(json_paths)

    for gp in gps:
        _, df = gp

        rows = list(df.itertuples())
        output = run_concurrent_map(
            functools.partial(
                process_single_nwm_grid_file,
                configuration_name=teehr_config["name"],
                variable_name=variable_name,
                weights_filepath=zonal_weights_filepath,
                ignore_missing_file=ignore_missing_file,
                location_id_prefix=location_id_prefix,
                variable_mapper=variable_mapper,
                registry=registry,
            ),
            rows,
            cpu_workers,
        )

        output = [df for df in output if df is not None]
        if len(output) == 0:
            raise FileNotFoundError("No NWM files for specified input"
                                    "configuration were found in GCS!")
        z_hour_df = pd.concat(output)

        if timeseries_type == TimeseriesTypeEnum.secondary:
            z_hour_df.loc[:, "member"] = teehr_config["member"]

        # Save to parquet.
        yrmoday = df.day.iloc[0]
        z_hour = df.z_hour.iloc[0][1:3]
        ref_time_str = f"{yrmoday}T{z_hour}"
        parquet_filepath = Path(output_parquet_dir, f"{ref_time_str}.parquet")
        z_hour_df.sort_values([LOCATION_ID, VALUE_TIME], inplace=True)

        if convert_k_to_c and variable_name == "T2D":
            z_hour_df = convert_value_from_kelvin_to_celsius(df=z_hour_df)

        if drop_overlapping_assimilation_values and "assim" in nwm_configuration_name:
            # Set reference_time to NaT for assimilation values
            z_hour_df.loc[:, REFERENCE_TIME] = pd.NaT

        write_timeseries_parquet_file(
            filepath=parquet_filepath,
            overwrite_output=overwrite_output,
            data=z_hour_df,
            timeseries_type=timeseries_type
        )
