from pathlib import Path
import filecmp

import pandas as pd

from teehr.loading.nwm.nwm_points import fetch_and_format_nwm_points
from teehr.loading.nwm.const import (
    NWM22_UNIT_LOOKUP,
)
from teehr.loading.nwm.utils import build_zarr_references

TEST_DIR = Path("tests", "data", "nwm30")
TEMP_DIR = Path("tests", "data", "temp")


def test_point_zarr_reference_file():

    component_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc" # noqa
    ]

    json_file = build_zarr_references(
        remote_paths=component_paths,
        json_dir=TEMP_DIR,
        ignore_missing_file=False
    )

    test_file = Path(TEST_DIR, "point_benchmark.json")

    assert filecmp.cmp(test_file, json_file[0], shallow=False)


def test_point_fetch_and_format():

    json_paths = [Path(
        TEMP_DIR,
        "nwm.20231101.nwm.t00z.short_range.channel_rt.f001.alaska.nc.json"
    ).as_posix()]

    location_ids = [
        19020190009995,
        19020190009996,
        19020190009997,
        19020190009998
    ]

    fetch_and_format_nwm_points(
        json_paths=json_paths,
        location_ids=location_ids,
        configuration="short_range",
        variable_name="streamflow",
        output_parquet_dir=TEMP_DIR,
        nwm_version="nwm30",
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True
    )

    parquet_file = Path(TEMP_DIR, "20231101T00Z.parquet")
    test_file = Path(TEST_DIR, "point_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


if __name__ == "__main__":
    # test_point_zarr_reference_file()
    test_point_fetch_and_format()
