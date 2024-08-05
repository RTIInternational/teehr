"""Tests for retrospective NWM grid loading."""
from pathlib import Path
import math
import shutil

import pandas as pd
import pytest

from teehr.evaluation.evaluation import Evaluation


TEMP_DIR = Path("tests", "data", "temp", "retro")
ZONAL_WEIGHTS = Path(
    "tests", "data", "nwm22", "onehuc10_weights_retro.parquet"
)


@pytest.fixture(scope="session")
def temp_dir_fixture(tmp_path_factory):
    """Create a temporary directory pytest fixture."""
    temp_dir = tmp_path_factory.mktemp("retro_grids")
    return temp_dir


def test_nwm30_grid_loading(temp_dir_fixture):
    """Test NWM30 grid loading."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_grids(
        nwm_version="nwm30",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        overwrite_output=True,
        chunk_by=None,
        location_id_prefix="ngen"
    )

    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20080523.parquet")
    )
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


def test_nwm21_grid_loading(temp_dir_fixture):
    """Test NWM21 grid loading."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_grids(
        nwm_version="nwm21",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        overwrite_output=True,
        chunk_by=None,
        location_id_prefix="xyz"
    )

    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20080523.parquet")
    )
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
    TEMP_DIR.mkdir(exist_ok=True)
    test_nwm30_grid_loading(TEMP_DIR)
    test_nwm21_grid_loading(TEMP_DIR)
    shutil.rmtree(TEMP_DIR)
