"""Test results of weighted average of pixels within polygons."""
from pathlib import Path

import pandas as pd
import xarray as xr

from teehr.loading.nwm.grid_utils import (
    compute_weighted_average,
    get_nwm_grid_data,
    get_weights_row_col_stats
)


TEST_DIR = Path("tests", "data", "nwm22")

TEMPLATE_FILEPATH = Path(TEST_DIR, "test_template_grid_nwm.nc")

WEIGHTS_FILEPATH = Path(TEST_DIR, "test_weights_results.parquet")
TEST_ZONAL_MEAN = Path(TEST_DIR, "test_zonal_mean_results.parquet")


def test_zonal_mean():
    """Test zonal mean results.

    The truth data set  ``test_zonal_mean_results.parquet`` was validated
    against ``exactextract`` results and checked for a single catchment
    (cat-77566) by calculating the weighted average manually
    (sum of weights * values divided by the sum of the weights).

    The command used to run ``exactextract``:

    exactextract -r temp:NETCDF:test_template_grid_nwm.nc:RAINRATE
    -p test_ngen_divides_nwm.shp -o ee_results.csv -s "mean"
    --include-col "id"

    Since ``exactextract`` cannot read parquet files the
    test_ngen_divides.parquet file was converted to a shapefile and
    reprojected to the crs specified in NWM_CONUS_WKT.
    """
    grid_ds = xr.open_dataset(TEMPLATE_FILEPATH)

    weights_df = pd.read_parquet(
        WEIGHTS_FILEPATH, columns=["row", "col", "weight", "location_id"]
    )

    weights_bounds = get_weights_row_col_stats(weights_df)

    grid_arr = get_nwm_grid_data(
        grid_ds.RAINRATE[0],
        weights_bounds["row_min"],
        weights_bounds["col_min"],
        weights_bounds["row_max"],
        weights_bounds["col_max"]
    )
    grid_values = grid_arr[
        weights_bounds["rows_norm"],
        weights_bounds["cols_norm"]
    ]

    df = compute_weighted_average(
        grid_values=grid_values,
        weights_df=weights_df
    )

    df_test = pd.read_parquet(TEST_ZONAL_MEAN)

    df_test.sort_values(["location_id"], inplace=True)
    df.sort_values(["location_id"], inplace=True)

    assert (df.value.values == df_test.value.values).all()
    assert (df.location_id.values == df_test.location_id.values).all()


if __name__ == "__main__":
    test_zonal_mean()
    pass
