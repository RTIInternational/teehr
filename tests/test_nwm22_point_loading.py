from pathlib import Path
import filecmp

import pandas as pd

from teehr.loading.nwm22.nwm_point_data import fetch_and_format_nwm_points
from teehr.loading.nwm22.const_nwm import (
    NWM22_UNIT_LOOKUP,
)
from teehr.loading.nwm_common.utils_nwm import build_zarr_references

TEST_DIR = Path("tests", "data", "nwm22")
TEMP_DIR = Path("tests", "data", "temp")


def test_point_zarr_reference_file():

    component_paths = [
        "gcs://national-water-model/nwm.20230318/short_range/nwm.t14z.short_range.channel_rt.f012.conus.nc" # noqa
    ]

    _ = build_zarr_references(
        remote_paths=component_paths,
        json_dir=TEMP_DIR,
        ignore_missing_file=False
    )

    json_file = Path(TEMP_DIR,
                     "nwm.20230318.nwm.t14z.short_range.channel_rt.f012.conus.nc.json") # noqa
    test_file = Path(TEST_DIR, "point_benchmark.json")

    assert filecmp.cmp(test_file, json_file, shallow=False)


def test_point_fetch_and_format():

    json_paths = [Path(
        TEMP_DIR,
        "nwm.20230318.nwm.t14z.short_range.channel_rt.f012.conus.nc.json"
    ).as_posix()]

    location_ids = [
        7086109,
        7040481,
        7053819,
        7111205,
    ]

    fetch_and_format_nwm_points(
        json_paths=json_paths,
        location_ids=location_ids,
        configuration="short_range",
        variable_name="streamflow",
        output_parquet_dir=TEMP_DIR,
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True
    )

    parquet_file = Path(TEMP_DIR, "20230318T14Z.parquet")
    test_file = Path(TEST_DIR, "point_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


if __name__ == "__main__":
    test_point_zarr_reference_file()
    test_point_fetch_and_format()
