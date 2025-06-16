"""Test NWM fetching utils."""
from pathlib import Path
from datetime import datetime

import tempfile
import pytest

from teehr.fetching.utils import (
    build_zarr_references,
    validate_operational_start_end_date,
    build_remote_nwm_filelist,
    generate_json_paths,
    get_dataset,
    create_periods_based_on_chunksize,
    parse_nwm_json_paths,
    start_on_z_hour,
    end_on_z_hour

)
from teehr.fetching.const import (
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)

TIMEFORMAT = "%Y-%m-%d %H:%M:%S"


def test_parsing_remote_json_paths(tmpdir):
    """Test parsing z_hour and date from remote json paths."""
    json_paths = [
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/analysis_assim_extend_no_da/nwm.t06z.analysis_assim_extend_no_da.channel_rt.tm00.conus.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/analysis_assim_hawaii/nwm.t06z.analysis_assim.channel_rt.tm0100.hawaii.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/long_range_mem1/nwm.t06z.long_range.channel_rt_1.f102.conus.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/medium_range_mem1/nwm.t06z.medium_range.channel_rt_1.f009.conus.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/medium_range_no_da/nwm.t06z.medium_range_no_da.channel_rt.f063.conus.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/short_range/nwm.t06z.short_range.channel_rt.f010.conus.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/short_range_puertorico/nwm.t06z.short_range.channel_rt.f020.puertorico.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/short_range_puertorico_no_da/nwm.t06z.short_range_no_da.channel_rt.f029.puertorico.nc.json", # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/forcing_short_range/nwm.t06z.short_range.forcing.f005.conus.nc.json",  # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/forcing_analysis_assim/nwm.t06z.analysis_assim.forcing.tm02.conus.nc.json",  # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/forcing_analysis_assim_puertorico/nwm.t06z.analysis_assim.forcing.tm00.puertorico.nc.json",  # noqa
        "s3://ciroh-nwm-zarr-copy/national-water-model/nwm.20220101/forcing_medium_range/nwm.t06z.medium_range.forcing.f039.conus.nc.json"  # noqa
    ]

    df = parse_nwm_json_paths(
        json_paths=json_paths
    )

    assert df["day"].eq("20220101").all()
    assert df["z_hour"].eq("t06z").all()
    assert df["filepath"].eq(json_paths).all()


def test_point_zarr_reference_file(tmpdir):
    """Test the point zarr reference file creation."""
    component_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc" # noqa
    ]

    built_files = build_zarr_references(
        remote_paths=component_paths,
        json_dir=tmpdir,
        ignore_missing_file=False
    )
    test_file = Path(
        Path("tests", "data", "nwm30"),
        "nwm.20231101.nwm.t00z.short_range.channel_rt.f001.alaska.nc.json"
    )

    test_ds = get_dataset(str(test_file), ignore_missing_file=False)
    built_ds = get_dataset(built_files[0], ignore_missing_file=False)

    # Two Datasets are identical if they have matching variables and
    # coordinates, all of which are equal, and all dataset attributes
    # and the attributes on all variables and coordinates are equal.
    assert test_ds.identical(built_ds)


def test_dates_and_nwm30_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm30"
    start_date = "2023-11-20"
    end_date = "2023-11-20"
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = "2022-11-20"
        validate_operational_start_end_date(
            nwm_version,
            start_date,
            end_date
        )
    except ValueError:
        failed = True
    assert failed


def test_dates_and_nwm22_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm22"
    start_date = "2022-11-20"
    end_date = "2022-11-20"
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = "2023-11-20"
        end_date = "2025-11-20"
        validate_operational_start_end_date(
            nwm_version,
            start_date,
            end_date
        )
    except ValueError:
        failed = True
    assert failed


def test_dates_and_nwm21_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm21"
    start_date = "2021-04-30"
    end_date = "2021-04-30"
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = "2019-11-20"
        validate_operational_start_end_date(
            nwm_version,
            start_date,
            end_date
        )
    except ValueError:
        failed = True
    assert failed


def test_dates_and_nwm20_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm20"
    start_date = "2019-06-20"
    end_date = "2019-06-20"
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = "2018-11-20"
        validate_operational_start_end_date(
            nwm_version,
            start_date,
            end_date
        )
    except ValueError:
        failed = True
    assert failed


def test_dates_and_nwm12_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm12"
    start_date = "2018-11-20"
    end_date = "2018-11-20"
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = "2017-11-20"
        validate_operational_start_end_date(
            nwm_version,
            start_date,
            end_date
        )
    except ValueError:
        failed = True
    assert failed


def test_building_nwm30_gcs_paths():
    """Test building NWM30 GCS paths."""
    gcs_component_paths = build_remote_nwm_filelist(
        configuration="forcing_analysis_assim_extend",
        output_type="forcing",
        start_dt="2023-11-28",
        end_dt="2023-11-29",
        analysis_config_dict=NWM30_ANALYSIS_CONFIG,
        t_minus_hours=None,
        ignore_missing_file=False,
        prioritize_analysis_value_time=False,
        drop_overlapping_assimilation_values=True
    )
    assert len(gcs_component_paths) == 52
    assert (
        gcs_component_paths[0] == \
            'gcs://national-water-model/nwm.20231128/forcing_analysis_assim_extend/nwm.t16z.analysis_assim_extend.forcing.tm27.conus.nc' # noqa
    )
    assert (
        gcs_component_paths[-1] == \
            'gcs://national-water-model/nwm.20231129/forcing_analysis_assim_extend/nwm.t16z.analysis_assim_extend.forcing.tm00.conus.nc' # noqa
    )


def test_building_nwm22_gcs_paths():
    """Test building NWM22 GCS paths."""
    gcs_component_paths = build_remote_nwm_filelist(
        configuration="analysis_assim",
        output_type="channel_rt",
        start_dt="2019-01-12",
        end_dt="2019-01-12",
        analysis_config_dict=NWM22_ANALYSIS_CONFIG,
        t_minus_hours=[0],
        ignore_missing_file=False,
        prioritize_analysis_value_time=False,
        drop_overlapping_assimilation_values=False
    )
    assert len(gcs_component_paths) == 24
    assert (
        gcs_component_paths[-1] == \
            'gcs://national-water-model/nwm.20190112/analysis_assim/nwm.t23z.analysis_assim.channel_rt.tm00.conus.nc' # noqa
    )
    assert (
        gcs_component_paths[0] == \
            'gcs://national-water-model/nwm.20190112/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc' # noqa
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


def test_start_end_z_hours():
    """Test building NWM30 GCS paths and specifying start/end z-hour."""
    gcs_component_paths = build_remote_nwm_filelist(
        configuration="short_range",
        output_type="channel_rt",
        start_dt="2023-11-28",
        end_dt="2023-11-29",
        analysis_config_dict=NWM30_ANALYSIS_CONFIG,
        t_minus_hours=[0],
        ignore_missing_file=False,
        prioritize_analysis_value_time=False,
        drop_overlapping_assimilation_values=False
    )

    gcs_component_paths = start_on_z_hour(
        gcs_component_paths=gcs_component_paths,
        start_z_hour=3,
        start_date=datetime.strptime("2023-11-28", "%Y-%m-%d")
    )
    gcs_component_paths = end_on_z_hour(
        gcs_component_paths=gcs_component_paths,
        end_z_hour=12,
        end_date=datetime.strptime("2023-11-29", "%Y-%m-%d")
    )

    assert gcs_component_paths[-1] == 'gcs://national-water-model/nwm.20231129/short_range/nwm.t12z.short_range.channel_rt.f018.conus.nc'  # noqa
    assert gcs_component_paths[0] == 'gcs://national-water-model/nwm.20231128/short_range/nwm.t03z.short_range.channel_rt.f001.conus.nc'  # noqa
    assert len(gcs_component_paths) == 612


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="teehr-") as tempdir:
        test_parsing_remote_json_paths(tempdir)
        test_point_zarr_reference_file(tempdir)
    test_building_nwm30_gcs_paths()
    test_building_nwm22_gcs_paths()
    test_generate_json_paths()
    test_dates_and_nwm30_version()
    test_dates_and_nwm22_version()
    test_dates_and_nwm21_version()
    test_dates_and_nwm20_version()
    test_dates_and_nwm12_version()
    test_generate_json_for_bad_file()
    test_create_periods_based_on_day()
    test_create_periods_based_on_week()
    test_create_periods_based_on_month()
    test_create_periods_based_on_year()
    test_start_end_z_hours()
