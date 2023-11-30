from pathlib import Path
import filecmp

from teehr.loading.nwm.utils import (
    build_zarr_references,
    check_dates_against_nwm_version,
    build_remote_nwm_filelist,
    generate_json_paths
)
from teehr.loading.nwm.const import (
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)

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
    test_file = Path(
        TEST_DIR,
        "nwm.20231101.nwm.t00z.short_range.channel_rt.f001.alaska.nc.json"
    )

    assert filecmp.cmp(test_file, json_file[0], shallow=False)


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


if __name__ == "__main__":
    test_dates_and_nwm_version()
    test_building_nwm30_gcs_paths()
    test_building_nwm22_gcs_paths()
    test_generate_json_paths()
