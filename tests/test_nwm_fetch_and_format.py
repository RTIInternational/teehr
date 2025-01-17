"""Test for fetching and formatting NWM data."""
from pathlib import Path
import tempfile

import pandas as pd
import numpy as np
import pytest

from teehr.fetching.nwm.grid_utils import update_location_id_prefix
from teehr.fetching.nwm.nwm_points import fetch_and_format_nwm_points
from teehr.fetching.nwm.nwm_grids import fetch_and_format_nwm_grids

TEST_NWM_VARIABLE_MAPPER = {
    "variable_name": {
        # "streamflow": "streamflow",
        # "RAINRATE": "rainfall_hourly_rate",
    },
    "unit_name": {
        "m3 s-1": "m3/s",
    },
}


def test_nwm22_point_fetch_and_format(tmpdir):
    """Test NWM22 point fetch and format."""
    test_data_dir = Path("tests", "data", "nwm22")

    json_paths = [Path(
        test_data_dir,
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
        output_parquet_dir=tmpdir,
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=False,
        overwrite_output=True,
        nwm_version="nwm22",
        variable_mapper=TEST_NWM_VARIABLE_MAPPER,
        timeseries_type="secondary"
    )

    parquet_file = Path(tmpdir, "20230318T14.parquet")
    benchmark_file = Path(test_data_dir, "point_benchmark.parquet")

    bench_df = pd.read_parquet(benchmark_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


def test_nwm30_point_fetch_and_format(tmpdir):
    """Test NWM30 point fetch and format."""
    test_data_dir = Path("tests", "data", "nwm30")

    json_paths = [Path(
        test_data_dir,
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
        output_parquet_dir=tmpdir,
        nwm_version="nwm30",
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=False,
        overwrite_output=True,
        variable_mapper=TEST_NWM_VARIABLE_MAPPER,
        timeseries_type="secondary"
    )

    parquet_file = Path(tmpdir, "20231101T00.parquet")
    benchmark_file = Path(test_data_dir, "point_benchmark.parquet")

    bench_df = pd.read_parquet(benchmark_file)
    test_df = pd.read_parquet(parquet_file)

    assert test_df.compare(bench_df).index.size == 0


def test_nwm30_point_fetch_and_format_medium_range_member(tmpdir):
    """Test NWM30 point fetch and formatting medium range ensemble."""
    test_data_dir = Path("tests", "data", "nwm30")

    json_paths = [Path(
        test_data_dir,
        "nwm.20240222.nwm.t00z.medium_range.channel_rt_1.f001.conus.nc.json"
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
        configuration="medium_range_mem1",
        variable_name="streamflow",
        output_parquet_dir=tmpdir,
        nwm_version="nwm30",
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=False,
        overwrite_output=True,
        variable_mapper=TEST_NWM_VARIABLE_MAPPER,
        timeseries_type="secondary"
    )

    parquet_file = Path(tmpdir, "20240222T00.parquet")
    # benchmark_file = Path(test_data_dir, "point_benchmark.parquet")

    # bench_df = pd.read_parquet(benchmark_file)
    test_df = pd.read_parquet(parquet_file)

    # assert test_df.compare(bench_df).index.size == 0
    assert test_df.shape[0] == 4


def test_nwm22_grid_fetch_and_format(tmpdir):
    """Test NWM22 grid fetch and format."""
    test_data_dir = Path("tests", "data", "nwm22")
    weights_filepath = Path(test_data_dir, "onehuc10_weights.parquet")

    json_file = Path(test_data_dir,
                     "nwm.20201218.nwm.t00z.analysis_assim.forcing.tm00.conus.nc.json") # noqa
    json_paths = [str(json_file)]

    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        configuration_name="forcing_analysis_assim",
        variable_name="RAINRATE",
        output_parquet_dir=tmpdir,
        zonal_weights_filepath=weights_filepath,
        ignore_missing_file=False,
        overwrite_output=True,
        location_id_prefix=None,
        variable_mapper=None,
        timeseries_type="primary"
    )

    parquet_file = Path(tmpdir, "20201218T00.parquet")
    benchmark_file = Path(test_data_dir, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(benchmark_file)
    test_df = pd.read_parquet(parquet_file)
    # Match the column order.
    bench_df = bench_df[[
        'location_id',
        'value',
        'unit_name',
        'variable_name',
        'value_time',
        'reference_time',
        'configuration_name',
    ]].copy()

    assert test_df.compare(bench_df).index.size == 0


def test_nwm30_grid_fetch_and_format(tmpdir):
    """Test NWM30 grid fetch and format."""
    test_data_dir = Path("tests", "data", "nwm30")
    weights_filepath = Path(test_data_dir, "one_huc10_alaska_weights.parquet")

    json_file = Path(test_data_dir,
                     "nwm.20231101.nwm.t00z.analysis_assim.forcing.tm02.alaska.nc.json") # noqa
    json_paths = [str(json_file)]

    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        configuration_name="forcing_analysis_assim_alaska",
        variable_name="RAINRATE",
        output_parquet_dir=tmpdir,
        zonal_weights_filepath=weights_filepath,
        ignore_missing_file=False,
        overwrite_output=True,
        location_id_prefix=None,
        variable_mapper=TEST_NWM_VARIABLE_MAPPER,
        timeseries_type="primary"
    )

    parquet_file = Path(tmpdir, "20231101T00.parquet")
    benchmark_file = Path(test_data_dir, "grid_benchmark.parquet")

    bench_df = pd.read_parquet(benchmark_file)
    test_df = pd.read_parquet(parquet_file)
    # Match the column order.
    # TODO: fix bench df
    bench_df = bench_df[[
        'location_id',
        'value',
        'unit_name',
        'variable_name',
        'value_time',
        'reference_time',
        'configuration_name'
    ]].copy()

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
    with tempfile.TemporaryDirectory(prefix="teehr") as tempdir:
        test_nwm22_point_fetch_and_format(tempfile.mkdtemp(dir=tempdir))
        test_nwm30_point_fetch_and_format(tempfile.mkdtemp(dir=tempdir))
        test_nwm22_grid_fetch_and_format(tempfile.mkdtemp(dir=tempdir))
        test_nwm30_grid_fetch_and_format(tempfile.mkdtemp(dir=tempdir))
        test_nwm30_point_fetch_and_format_medium_range_member(tempfile.mkdtemp(dir=tempdir))  # noqa
        test_replace_location_id_prefix()
        test_prepend_location_id_prefix()
        test_raise_location_id_prefix_error()
