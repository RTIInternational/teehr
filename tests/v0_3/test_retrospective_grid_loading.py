"""Tests for retrospective NWM grid loading."""
from pathlib import Path
import math

import pandas as pd

from teehr_v0_3.loading.nwm.retrospective_grids import nwm_retro_grids_to_parquet


TEMP_DIR = Path("tests", "v0_3", "data", "temp", "retro")
ZONAL_WEIGHTS = Path(
    "tests", "v0_3", "data", "nwm22", "onehuc10_weights_retro.parquet"
)


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
        chunk_by=None,
        location_id_prefix="ngen"
    )

    df = pd.read_parquet(Path(TEST_DIR, "20080523.parquet"))
    assert len(df) == 15
    assert df["value_time"].min() == pd.Timestamp("2008-05-23 09:00")
    assert df["value_time"].max() == pd.Timestamp("2008-05-23 23:00")
    assert df["location_id"].unique()[0] == "ngen-1016000606"
    assert df["configuration"].unique()[0] == "nwm30_retrospective"
    test_val = df[df.value_time == "2008-05-23 09:00:00"].value.values[0]
    assert math.isclose(test_val, 0.00025580, rel_tol=1e-4)
    assert df.columns.to_list() == [
        "location_id",
        "value",
        "value_time",
        "reference_time",
        "measurement_unit",
        "configuration",
        "variable_name",
    ]


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
        chunk_by=None,
        location_id_prefix="xyz"
    )

    df = pd.read_parquet(Path(TEST_DIR, "20080523.parquet"))
    assert len(df) == 15
    assert df["value_time"].min() == pd.Timestamp("2008-05-23 09:00")
    assert df["value_time"].max() == pd.Timestamp("2008-05-23 23:00")
    assert df["location_id"].unique()[0] == "xyz-1016000606"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"
    test_val = df[df.value_time == "2008-05-23 09:00:00"].value.values[0]
    assert math.isclose(test_val, 0.00025678, rel_tol=1e-4)
    assert df.columns.to_list() == [
        "location_id",
        "value",
        "value_time",
        "reference_time",
        "measurement_unit",
        "configuration",
        "variable_name",
    ]


if __name__ == "__main__":
    test_nwm30_grid_loading()
    test_nwm21_grid_loading()
