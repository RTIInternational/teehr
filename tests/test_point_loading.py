from pathlib import Path
import filecmp

from teehr.loading.nwm22.nwm_point_data import fetch_and_format_nwm_points

TEST_DIR = Path("tests", "data", "nwm22")


def test_point_loading():

    json_paths = [Path(TEST_DIR, "nwm.20230318.nwm.t14z.short_range.channel_rt.f012.conus.nc.json").as_posix()]

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
    )

    parquet_file = Path(TEST_DIR, "20230318T14Z.parquet")
    test_file = Path(TEST_DIR, "point_benchmark.parquet")
    assert filecmp.cmp(test_file, parquet_file, shallow=False)


if __name__ == "__main__":
    test_point_loading()
