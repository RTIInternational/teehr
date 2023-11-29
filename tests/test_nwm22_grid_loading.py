
from pathlib import Path
import filecmp

import pandas as pd

from teehr.loading.nwm22.nwm_grid_data import fetch_and_format_nwm_grids
from teehr.loading.nwm.utils import build_zarr_references
from teehr.loading.nwm22.const_nwm import (
    NWM22_UNIT_LOOKUP,
)

TEST_DIR = Path("tests", "data", "nwm22")
TEMP_DIR = Path("tests", "data", "temp")
WEIGHTS_FILEPATH = Path(TEST_DIR, "onehuc10_weights.parquet")


def test_grid_zarr_reference_file():

    component_paths = [
        "gcs://national-water-model/nwm.20201218/forcing_analysis_assim/nwm.t00z.analysis_assim.forcing.tm00.conus.nc" # noqa
    ]

    json_file = build_zarr_references(
        remote_paths=component_paths,
        json_dir=TEMP_DIR,
        ignore_missing_file=False
    )

    test_file = Path(TEST_DIR, "grid_benchmark.json")

    assert filecmp.cmp(test_file, json_file[0], shallow=False)


def test_grid_fetch_and_format():

    json_file = Path(TEMP_DIR,
                     "nwm.20201218.nwm.t00z.analysis_assim.forcing.tm00.conus.nc.json") # noqa
    json_paths = [str(json_file)]

    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        configuration="forcing_analysis_assim",
        variable_name="RAINRATE",
        output_parquet_dir=TEMP_DIR,
        zonal_weights_filepath=WEIGHTS_FILEPATH,
        ignore_missing_file=False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True
    )

    parquet_file = Path(TEMP_DIR, "20201218T00Z.parquet")
    test_file = Path(TEST_DIR, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


if __name__ == "__main__":
    test_grid_zarr_reference_file()
    test_grid_fetch_and_format()
