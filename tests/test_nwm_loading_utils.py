"""Test NWM loading utils."""
from pathlib import Path

import pytest

from teehr.loading.nwm.utils import (
    build_zarr_references,
    check_dates_against_nwm_version,
    build_remote_nwm_filelist,
    generate_json_paths,
    get_dataset,
    create_periods_based_on_chunksize
)
from teehr.loading.nwm.const import (
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)

TIMEFORMAT = "%Y-%m-%d %H:%M:%S"

TEST_DIR = Path("tests", "data", "nwm30")
TEMP_DIR = Path("tests", "data", "temp")


def test_point_zarr_reference_file():
    """Test the point zarr reference file creation."""
    component_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc" # noqa
    ]

    built_files = build_zarr_references(
        remote_paths=component_paths,
        json_dir=TEMP_DIR,
        ignore_missing_file=False
    )
    test_file = Path(
        TEST_DIR,
        "nwm.20231101.nwm.t00z.short_range.channel_rt.f001.alaska.nc.json"
    )

    test_ds = get_dataset(str(test_file), ignore_missing_file=False)
    built_ds = get_dataset(built_files[0], ignore_missing_file=False)

    # Two Datasets are identical if they have matching variables and
    # coordinates, all of which are equal, and all dataset attributes
    # and the attributes on all variables and coordinates are equal.
    assert test_ds.identical(built_ds)


def test_dates_and_nwm_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm30"
    start_date = "2023-11-20"
    ingest_days = 1
    check_dates_against_nwm_version(nwm_version, start_date, ingest_days)

    try:
        nwm_version = "nwm22"
        check_dates_against_nwm_version(nwm_version, start_date, ingest_days)
    except ValueError:
        failed = True
    assert failed


def test_building_nwm30_gcs_paths():
    """Test building NWM30 GCS paths."""
    configuration = "analysis_assim_extend"
    output_type = "channel_rt"
    start_date = "2023-11-28"
    ingest_days = 1
    analysis_config_dict = NWM30_ANALYSIS_CONFIG
    t_minus_hours = [0]
    ignore_missing_file = False

    # Build paths to netcdf files on GCS
    gcs_component_paths = build_remote_nwm_filelist(
        configuration,
        output_type,
        start_date,
        ingest_days,
        analysis_config_dict,
        t_minus_hours,
        ignore_missing_file,
    )

    assert (
        gcs_component_paths == \
            ['gcs://national-water-model/nwm.20231128/analysis_assim_extend/nwm.t16z.analysis_assim_extend.channel_rt.tm00.conus.nc'] # noqa
    )


def test_building_nwm22_gcs_paths():
    """Test building NWM22 GCS paths."""
    configuration = "analysis_assim_extend"
    output_type = "channel_rt"
    start_date = "2019-01-12"
    ingest_days = 1
    analysis_config_dict = NWM22_ANALYSIS_CONFIG
    t_minus_hours = [0]
    ignore_missing_file = False

    # Build paths to netcdf files on GCS
    gcs_component_paths = build_remote_nwm_filelist(
        configuration,
        output_type,
        start_date,
        ingest_days,
        analysis_config_dict,
        t_minus_hours,
        ignore_missing_file,
    )

    assert (
        gcs_component_paths == \
            ['gcs://national-water-model/nwm.20190112/analysis_assim_extend/nwm.t16z.analysis_assim_extend.channel_rt.tm00.conus.nc'] # noqa
    )


def test_generate_json_paths():
    """Test generating kerchunk json paths."""
    kerchunk_method = "auto"
    gcs_component_paths = \
        ['gcs://national-water-model/nwm.20220112/analysis_assim_extend/nwm.t16z.analysis_assim_extend.channel_rt.tm00.conus.nc'] # noqa
    json_dir = ""
    ignore_missing_file = False

    json_paths = generate_json_paths(
        kerchunk_method,
        gcs_component_paths,
        json_dir,
        ignore_missing_file
    )

    assert json_paths == \
        ['s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220112/analysis_assim_extend/nwm.t16z.analysis_assim_extend.channel_rt.tm00.conus.nc.json'] # noqa

    pass


def test_generate_json_for_bad_file():
    """Test generating json paths for a corrupt GCS file."""
    kerchunk_method = "local"
    gcs_component_paths = \
        ['gcs://national-water-model/nwm.20240125/forcing_medium_range/nwm.t18z.medium_range.forcing.f104.conus.nc'] # noqa
    json_dir = ""
    ignore_missing_file = False

    with pytest.raises(Exception):
        _ = generate_json_paths(
            kerchunk_method,
            gcs_component_paths,
            json_dir,
            ignore_missing_file
        )


def test_create_periods_based_on_day():
    """Test creating periods based on daily chunksize."""
    start_date = "2023-12-30"
    end_date = "2024-01-02"
    chunk_by = "day"

    periods = create_periods_based_on_chunksize(
        start_date=start_date,
        end_date=end_date,
        chunk_by=chunk_by
    )
    assert periods[0].start_time.strftime(TIMEFORMAT) == "2023-12-30 00:00:00"
    assert periods[0].end_time.strftime(TIMEFORMAT) == "2023-12-30 23:59:59"
    assert periods[1].start_time.strftime(TIMEFORMAT) == "2023-12-31 00:00:00"
    assert periods[1].end_time.strftime(TIMEFORMAT) == "2023-12-31 23:59:59"
    assert periods[2].start_time.strftime(TIMEFORMAT) == "2024-01-01 00:00:00"
    assert periods[2].end_time.strftime(TIMEFORMAT) == "2024-01-01 23:59:59"
    assert periods[3].start_time.strftime(TIMEFORMAT) == "2024-01-02 00:00:00"
    assert periods[3].end_time.strftime(TIMEFORMAT) == "2024-01-02 23:59:59"


def test_create_periods_based_on_week():
    """Test creating periods based on weekly chunksize."""
    start_date = "2023-12-30"
    end_date = "2024-01-02"
    chunk_by = "week"
    periods = create_periods_based_on_chunksize(
        start_date=start_date,
        end_date=end_date,
        chunk_by=chunk_by
    )
    assert periods[0].start_time.strftime(TIMEFORMAT) == "2023-12-25 00:00:00"
    assert periods[0].end_time.strftime(TIMEFORMAT) == "2023-12-31 23:59:59"


def test_create_periods_based_on_month():
    """Test creating periods based on monthly chunksize."""
    start_date = "2023-12-30"
    end_date = "2024-01-02"
    chunk_by = "month"
    periods = create_periods_based_on_chunksize(
        start_date=start_date,
        end_date=end_date,
        chunk_by=chunk_by
    )
    assert periods[0].start_time.strftime(TIMEFORMAT) == "2023-12-01 00:00:00"
    assert periods[0].end_time.strftime(TIMEFORMAT) == "2023-12-31 23:59:59"


def test_create_periods_based_on_year():
    """Test creating periods based on yearly chunksize."""
    start_date = "2023-12-30"
    end_date = "2024-01-02"
    chunk_by = "year"
    periods = create_periods_based_on_chunksize(
        start_date=start_date,
        end_date=end_date,
        chunk_by=chunk_by
    )
    assert periods[0].start_time.strftime(TIMEFORMAT) == "2023-01-01 00:00:00"
    assert periods[0].end_time.strftime(TIMEFORMAT) == "2023-12-31 23:59:59"


if __name__ == "__main__":
    test_dates_and_nwm_version()
    test_building_nwm30_gcs_paths()
    test_building_nwm22_gcs_paths()
    test_generate_json_paths()
    test_point_zarr_reference_file()
    test_generate_json_for_bad_file()
    test_create_periods_based_on_day()
    test_create_periods_based_on_week()
    test_create_periods_based_on_month()
    test_create_periods_based_on_year()
