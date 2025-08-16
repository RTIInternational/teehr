"""Test the generation of weights."""
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
from teehr.utilities.generate_weights import generate_weights_file
from teehr.fetching.const import CONUS_NWM_WKT


TEST_DIR = Path("tests", "data", "nwm30")
TEMPLATE_FILEPATH = Path(TEST_DIR, "nwm_retro_v3_template_grid.nc")
ZONES_FILEPATH = Path(TEST_DIR, "one_huc10_conus_1016000606.parquet")
WEIGHTS_FILEPATH = Path(TEST_DIR, "one_huc10_1016000606_teehr_weights.parquet")


def test_weights(tmpdir):
    """Test the generation of weights."""
    generate_weights_file(
        zone_polygons=ZONES_FILEPATH,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=Path(tmpdir, "weights.parquet"),
        unique_zone_id="id",
    )
    df_test = pd.read_parquet(WEIGHTS_FILEPATH).astype({"weight": np.float32})
    df = pd.read_parquet(Path(tmpdir, "weights.parquet"))
    assert df.equals(df_test)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_weights(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
