"""Test weight generation."""
import pandas as pd
from pathlib import Path
import numpy as np

from teehr_v0_3.utilities.generate_weights import generate_weights_file
from teehr_v0_3.loading.nwm.const import CONUS_NWM_WKT


TEST_DIR = Path("tests", "v0_3", "data", "nwm22")
TEMP_DIR = Path("tests", "v0_3", "data", "temp")
TEMPLATE_FILEPATH = Path(TEST_DIR, "test_template_grid_nwm.nc")
ZONES_FILEPATH = Path(TEST_DIR, "test_ngen_divides.parquet")
WEIGHTS_FILEPATH = Path(TEST_DIR, "test_weights_results.parquet")


def test_weights_adding_prefix():
    """Test generate weights file while adding a prefix to the location ID."""
    df = generate_weights_file(
        zone_polygon_filepath=ZONES_FILEPATH,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=None,
        location_id_prefix="ngen",
        unique_zone_id="id",
    )

    df_test = pd.read_parquet(WEIGHTS_FILEPATH)

    df_test.sort_values(["row", "col", "weight"], inplace=True)
    df.sort_values(["row", "col", "weight"], inplace=True)

    assert (df.row.values == df_test.row.values).all()
    assert (df.col.values == df_test.col.values).all()
    assert (df.weight.values == df_test.weight.values).all()

    location_id_arr = df_test.location_id.values
    pre_arr = np.full(location_id_arr.shape, "ngen-", dtype=object)
    prepended_location_id_arr = pre_arr + location_id_arr

    assert (df.location_id.values == prepended_location_id_arr).all()


def test_weights_no_prefix():
    """Test generate weights file without adding a location ID prefix."""
    df = generate_weights_file(
        zone_polygon_filepath=ZONES_FILEPATH,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=None,
        unique_zone_id="id",
    )

    df_test = pd.read_parquet(WEIGHTS_FILEPATH)

    df_test.sort_values(["row", "col", "weight"], inplace=True)
    df.sort_values(["row", "col", "weight"], inplace=True)

    assert (df.row.values == df_test.row.values).all()
    assert (df.col.values == df_test.col.values).all()
    assert (df.weight.values == df_test.weight.values).all()

    assert (df.location_id.values == df_test.location_id.values).all()


if __name__ == "__main__":
    test_weights_adding_prefix()
    test_weights_no_prefix()
    pass
