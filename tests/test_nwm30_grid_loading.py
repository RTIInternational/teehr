
from pathlib import Path
import filecmp

import pandas as pd

from teehr.loading.nwm30.nwm_grid_data import fetch_and_format_nwm_grids
from teehr.loading.nwm_common.utils_nwm import build_zarr_references
from teehr.loading.nwm22.const_nwm import (
    NWM22_UNIT_LOOKUP,
)

TEST_DIR = Path("tests", "data", "nwm30")
TEMP_DIR = Path("tests", "data", "temp")
WEIGHTS_FILEPATH = Path(TEST_DIR, "one_huc10_alaska_weights.parquet")


def test_grid_zarr_reference_file():

    component_paths = [
        "gcs://national-water-model/nwm.20231101/forcing_analysis_assim_alaska/nwm.t00z.analysis_assim.forcing.tm02.alaska.nc" # noqa
    ]

    _ = build_zarr_references(
        remote_paths=component_paths,
        json_dir=TEMP_DIR,
        ignore_missing_file=False
    )

    json_file = Path(TEMP_DIR,
                     "nwm.20231101.nwm.t00z.analysis_assim.forcing.tm02.alaska.nc.json") # noqa
    test_file = Path(TEST_DIR, "grid_benchmark.json")
    assert filecmp.cmp(test_file, json_file, shallow=False)


def test_grid_fetch_and_format():

    json_file = Path(TEMP_DIR,
                     "nwm.20231101.nwm.t00z.analysis_assim.forcing.tm02.alaska.nc.json") # noqa
    json_paths = [str(json_file)]

    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        configuration="forcing_analysis_assim_alaska",
        variable_name="RAINRATE",
        output_parquet_dir=TEMP_DIR,
        zonal_weights_filepath=WEIGHTS_FILEPATH,
        ignore_missing_file=False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True
    )

    parquet_file = Path(TEMP_DIR, "20231101T00Z.parquet")
    test_file = Path(TEST_DIR, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


if __name__ == "__main__":
    test_grid_zarr_reference_file()
    test_grid_fetch_and_format()
