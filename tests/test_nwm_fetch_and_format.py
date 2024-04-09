"""Test for fetching and formatting NWM data."""
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from teehr.loading.nwm.grid_utils import update_location_id_prefix
from teehr.loading.nwm.nwm_points import fetch_and_format_nwm_points
from teehr.loading.nwm.nwm_grids import fetch_and_format_nwm_grids
from teehr.loading.nwm.const import (
    NWM22_UNIT_LOOKUP,
)

TEMP_DIR = Path("tests", "data", "temp")


def test_nwm22_point_fetch_and_format():
    """Test NWM22 point fetch and format."""
    test_dir = Path("tests", "data", "nwm22")

    json_paths = [Path(
        test_dir,
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
        overwrite_output=True,
        nwm_version="nwm22"
    )

    parquet_file = Path(TEMP_DIR, "20230318T14Z.parquet")
    test_file = Path(test_dir, "point_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


def test_nwm30_point_fetch_and_format():
    """Test NWM30 point fetch and format."""
    test_dir = Path("tests", "data", "nwm30")

    json_paths = [Path(
        test_dir,
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
    test_file = Path(test_dir, "point_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


def test_nwm22_grid_fetch_and_format():
    """Test NWM22 grid fetch and format."""
    test_dir = Path("tests", "data", "nwm22")
    weights_filepath = Path(test_dir, "onehuc10_weights.parquet")

    json_file = Path(test_dir,
                     "nwm.20201218.nwm.t00z.analysis_assim.forcing.tm00.conus.nc.json") # noqa
    json_paths = [str(json_file)]

    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        configuration="forcing_analysis_assim",
        variable_name="RAINRATE",
        output_parquet_dir=TEMP_DIR,
        zonal_weights_filepath=weights_filepath,
        ignore_missing_file=False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True,
        location_id_prefix=None
    )

    parquet_file = Path(TEMP_DIR, "20201218T00Z.parquet")
    test_file = Path(test_dir, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


def test_nwm30_grid_fetch_and_format():
    """Test NWM30 grid fetch and format."""
    test_dir = Path("tests", "data", "nwm30")
    weights_filepath = Path(test_dir, "one_huc10_alaska_weights.parquet")

    json_file = Path(test_dir,
                     "nwm.20231101.nwm.t00z.analysis_assim.forcing.tm02.alaska.nc.json") # noqa
    json_paths = [str(json_file)]

    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        configuration="forcing_analysis_assim_alaska",
        variable_name="RAINRATE",
        output_parquet_dir=TEMP_DIR,
        zonal_weights_filepath=weights_filepath,
        ignore_missing_file=False,
        units_format_dict=NWM22_UNIT_LOOKUP,
        overwrite_output=True,
        location_id_prefix=None
    )

    parquet_file = Path(TEMP_DIR, "20231101T00Z.parquet")
    test_file = Path(test_dir, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(test_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


def test_replace_location_id_prefix():
    """Test replacing location_id prefix."""
    df = pd.DataFrame({
        "location_id": [
            "wbd10-1902020125",
            "wbd10-1902022256",
            "wbd10-1901234567"
        ],
        "value": [1, 2, 3]
    })
    df_new = update_location_id_prefix(df, new_prefix="ngen")
    assert (df_new.location_id.values == np.array(
                ["ngen-1902020125",
                 "ngen-1902022256",
                 "ngen-1901234567"])
            ).all()


def test_prepend_location_id_prefix():
    """Test prepend location_id prefix."""
    df = pd.DataFrame({
        "location_id": [
            "1902020125",
            "1902022256",
            "1901234567"
        ],
        "value": [1, 2, 3]
    })
    df_new = update_location_id_prefix(df, new_prefix="ngen")
    assert (df_new.location_id.values == np.array(
                ["ngen-1902020125",
                 "ngen-1902022256",
                 "ngen-1901234567"])
            ).all()


def test_raise_location_id_prefix_error():
    """Test raising location_id prefix error."""
    df = pd.DataFrame({
        "location_id": [
            "ngen-wbd10-1902020125",
            "ngen-wbd10-1902022256",
            "ngen-wbd10-1901234567"
        ],
        "value": [1, 2, 3]
    })
    with pytest.raises(ValueError):
        update_location_id_prefix(df, new_prefix="ngen2")


if __name__ == "__main__":
    test_nwm22_point_fetch_and_format()
    test_nwm30_point_fetch_and_format()
    test_nwm22_grid_fetch_and_format()
    test_nwm30_grid_fetch_and_format()
    test_replace_location_id_prefix()
    test_prepend_location_id_prefix()
    test_raise_location_id_prefix_error()
