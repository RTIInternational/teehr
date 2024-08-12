"""Tests for retrospective NWM grid fetching."""
from pathlib import Path
import math
import tempfile

import pandas as pd

from teehr.evaluation.evaluation import Evaluation


ZONAL_WEIGHTS = Path(
    "tests", "data", "nwm22", "onehuc10_weights_retro.parquet"
)


def test_nwm30_grid_fetching(tmpdir):
    """Test NWM30 grid fetching."""
    eval = Evaluation(tmpdir)
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
        Path(
            eval.primary_timeseries_cache_dir,
            "20080523.parquet"
        )
    )
    assert len(df) == 15
    assert df["value_time"].min() == pd.Timestamp("2008-05-23 09:00")
    assert df["value_time"].max() == pd.Timestamp("2008-05-23 23:00")
    assert df["location_id"].unique()[0] == "ngen-1016000606"
    assert df["configuration_name"].unique()[0] == "nwm30_retrospective"
    test_val = df[df.value_time == "2008-05-23 09:00:00"].value.values[0]
    assert math.isclose(test_val, 0.00025580, rel_tol=1e-4)
    assert df.columns.to_list() == [
        "location_id",
        "value",
        "value_time",
        "reference_time",
        "unit_name",
        "configuration_name",
        "variable_name",
    ]


def test_nwm21_grid_fetching(tmpdir):
    """Test NWM21 grid fetching."""
    eval = Evaluation(tmpdir)
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
        Path(
            eval.primary_timeseries_cache_dir,
            "20080523.parquet"
        )
    )
    assert len(df) == 15
    assert df["value_time"].min() == pd.Timestamp("2008-05-23 09:00")
    assert df["value_time"].max() == pd.Timestamp("2008-05-23 23:00")
    assert df["location_id"].unique()[0] == "xyz-1016000606"
    assert df["configuration_name"].unique()[0] == "nwm21_retrospective"
    test_val = df[df.value_time == "2008-05-23 09:00:00"].value.values[0]
    assert math.isclose(test_val, 0.00025678, rel_tol=1e-4)
    assert df.columns.to_list() == [
        "location_id",
        "value",
        "value_time",
        "reference_time",
        "unit_name",
        "configuration_name",
        "variable_name",
    ]


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="teehr-") as tempdir:
        test_nwm30_grid_fetching(tempfile.mkdtemp(dir=tempdir))
        test_nwm21_grid_fetching(tempfile.mkdtemp(dir=tempdir))
