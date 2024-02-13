import pandas as pd
from pathlib import Path
from teehr.utilities.generate_weights import generate_weights_file
from teehr.loading.nwm.const import CONUS_NWM_WKT


TEST_DIR = Path("tests", "data", "nwm22")
TEMP_DIR = Path("tests", "data", "temp")
TEMPLATE_FILEPATH = Path(TEST_DIR, "test_template_grid.nc")
ZONES_FILEPATH = Path(TEST_DIR, "test_ngen_divides.parquet")
WEIGHTS_FILEPATH = Path(TEST_DIR, "test_weights_results.parquet")


def test_weights():
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
    test_weights()
    pass
