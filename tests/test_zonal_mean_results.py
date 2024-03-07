"""Test results of weighted average of pixels within polygons."""
from pathlib import Path

import pandas as pd
import xarray as xr

from teehr.loading.nwm.grid_utils import compute_zonal_mean


TEST_DIR = Path("tests", "data", "nwm22")

TEMPLATE_FILEPATH = Path(TEST_DIR, "test_template_grid_nwm.nc")

WEIGHTS_FILEPATH = Path(TEST_DIR, "test_weights_results.parquet")
TEST_ZONAL_MEAN = Path(TEST_DIR, "test_zonal_mean_results.parquet")


def test_zonal_mean():
    """Test zonal mean results."""
    grid_ds = xr.open_dataset(TEMPLATE_FILEPATH)

    df = compute_zonal_mean(
        da=grid_ds.RAINRATE,
        weights_filepath=WEIGHTS_FILEPATH
    )

    df_test = pd.read_parquet(TEST_ZONAL_MEAN)

    df_test.sort_values(["location_id"], inplace=True)
    df.sort_values(["location_id"], inplace=True)

    assert (df.value.values == df_test.value.values).all()
    assert (df.location_id.values == df_test.location_id.values).all()


if __name__ == "__main__":
    test_zonal_mean()
    pass
