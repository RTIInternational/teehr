"""Tests for retrospective NWM grid loading."""
from pathlib import Path
import math

import pandas as pd

from teehr.loading.nwm.retrospective_grids import nwm_retro_grids_to_parquet


TEMP_DIR = Path("tests", "data", "temp", "retro")
ZONAL_WEIGHTS = Path("tests", "data", "nwm22", "onehuc10_weights.parquet")


def test_nwm30_grid_loading():
    """Test NWM30 grid loading."""
    TEST_DIR = Path(TEMP_DIR, "nwm30_grid")

    nwm_retro_grids_to_parquet(
        nwm_version="nwm30",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        output_parquet_dir=TEST_DIR,
        overwrite_output=True,
        chunk_by=None
    )

    df = pd.read_parquet(Path(TEST_DIR, "20080523Z.parquet"))
    assert len(df) == 2
    assert df["value_time"].min() == pd.Timestamp("2008-05-23 09:00")
    assert df["value_time"].max() == pd.Timestamp("2008-05-23 10:00")
    assert df["location_id"].unique()[0] == "nwm30-1016000606"
    assert df["configuration"].unique()[0] == "nwm30_retrospective"
    test_val = df[df.value_time == "2008-05-23 09:00:00"].value.values[0]
    assert math.isclose(test_val, 0.00025471, rel_tol=1e-4)


def test_nwm21_grid_loading():
    """Test NWM21 grid loading."""
    TEST_DIR = Path(TEMP_DIR, "nwm21_grid")

    nwm_retro_grids_to_parquet(
        nwm_version="nwm21",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        output_parquet_dir=TEST_DIR,
        overwrite_output=True,
        chunk_by=None
    )

    df = pd.read_parquet(Path(TEST_DIR, "20080523Z.parquet"))
    assert len(df) == 2
    assert df["value_time"].min() == pd.Timestamp("2008-05-23 09:00")
    assert df["value_time"].max() == pd.Timestamp("2008-05-23 10:00")
    assert df["location_id"].unique()[0] == "nwm21-1016000606"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"
    test_val = df[df.value_time == "2008-05-23 09:00:00"].value.values[0]
    assert math.isclose(test_val, 0.00025555, rel_tol=1e-4)


if __name__ == "__main__":
    test_nwm30_grid_loading()
    test_nwm21_grid_loading()
