
from pathlib import Path
import filecmp

import pandas as pd
import pytest

from teehr.loading.nwm22.nwm_grid_data import fetch_and_format_nwm_grids
from teehr.loading.nwm22.utils_nwm import build_zarr_references

TEST_DIR = Path("tests", "data", "nwm22")
WEIGHTS_FILEPATH = Path(TEST_DIR, "onehuc10_weights.parquet")


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_grid_loading():

    component_paths = [
        "gcs://national-water-model/nwm.20201218/forcing_analysis_assim/nwm.t00z.analysis_assim.forcing.tm00.conus.nc" # noqa
    ]

    json_paths = build_zarr_references(component_paths,
                                       TEST_DIR,
                                       False)

    json_file = Path(TEST_DIR,
                     "nwm.20201218.nwm.t00z.analysis_assim.forcing.tm00.conus.nc.json") # noqa
    test_file = Path(TEST_DIR, "grid_benchmark.json")
    assert filecmp.cmp(test_file, json_file, shallow=False)

    fetch_and_format_nwm_grids(
        json_paths,
        "forcing_analysis_assim",
        "RAINRATE",
        TEST_DIR,
        WEIGHTS_FILEPATH,
        False,
    )

    parquet_file = Path(TEST_DIR, "20201218T00Z.parquet")
    test_file = Path(TEST_DIR, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


if __name__ == "__main__":
    test_grid_loading()
