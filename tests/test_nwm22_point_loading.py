from pathlib import Path
# import filecmp

import pandas as pd

from teehr.loading.nwm_common.point_utils import fetch_and_format_nwm_points
from teehr.loading.nwm22.const_nwm import (
    NWM22_UNIT_LOOKUP,
)

TEST_DIR = Path("tests", "data", "nwm22")


def test_point_loading():

    json_paths = [Path(
        TEST_DIR,
        "nwm.20230318.nwm.t14z.short_range.channel_rt.f012.conus.nc.json"
    ).as_posix()]

    location_ids = [
        7086109,
        7040481,
        7053819,
        7111205,
    ]

    fetch_and_format_nwm_points(
        json_paths,
        location_ids,
        "short_range",
        "streamflow",
        TEST_DIR,
        True,
        100,
        False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True
    )

    parquet_file = Path(TEST_DIR, "20230318T14Z.parquet")
    test_file = Path(TEST_DIR, "point_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


if __name__ == "__main__":
    test_point_loading()
