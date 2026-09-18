"""Test NWM fetching utils."""
from datetime import timedelta
from pathlib import Path
from dateutil.parser import parse

import logging
import pytest

import numpy as np

from teehr.fetching.utils import (
    _feature_id_positions,
    build_zarr_references_virtualizarr,
    validate_operational_start_end_date,
    build_remote_nwm_filelist,
    generate_json_paths,
    open_kerchunk_dataset,
    read_nwm_global_attrs,
    read_nwm_file_version,
    validate_nwm_version_against_files,
    nwm_version_at,
    create_periods_based_on_chunksize,
    parse_nwm_json_paths,
    start_on_z_hour,
    end_on_z_hour,
    format_nwm_configuration_metadata
)
from teehr.fetching.const import (
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
    NWM_VERSION_BOUNDARIES,
    NWM31_START_DATE,
    NWM30_START_DATE,
    NWM21_START_DATE,
    NWM20_START_DATE,
    NWM12_START_DATE,
)
from teehr.fetching.models.utils import (
    SupportedNWMOperationalVersionsEnum,
)
from teehr.evaluation.evaluation import create_spark_session

TIMEFORMAT = "%Y-%m-%d %H:%M:%S"


def test_parsing_remote_json_paths():
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

    built_files = build_zarr_references_virtualizarr(
        remote_paths=component_paths,
        json_dir=tmpdir,
        ignore_missing_file=False
    )
    test_file = Path(
        Path("tests", "data", "nwm30"),
        "nwm.20231101.nwm.t00z.short_range.channel_rt.f001.alaska.nc.json"
    )

    all_vars = [
        "streamflow", "nudge", "velocity", "qSfcLatRunoff", "qBucket",
        "qBtmVertRunoff", "feature_id", "time", "reference_time", "crs",
    ]
    test_ds = open_kerchunk_dataset(str(test_file), loadable_variables=all_vars, ignore_missing_file=False)
    built_ds = open_kerchunk_dataset(built_files[0], loadable_variables=all_vars, ignore_missing_file=False)

    # `crs` is a dummy CF grid-mapping placeholder variable -- only its
    # attributes are ever used (e.g. `esri_pe_string`); VirtualiZarr doesn't
    # capture its on-disk scalar fill-value byte identically across builds,
    # so compare it by attributes only, and compare every other
    # variable/coordinate for full equality (values, dtypes, and attrs).
    #
    # Two Datasets are identical if they have matching variables and
    # coordinates, all of which are equal, and all dataset attributes
    # and the attributes on all variables and coordinates are equal.
    assert test_ds.crs.attrs == built_ds.crs.attrs
    assert test_ds.drop_vars("crs").identical(built_ds.drop_vars("crs"))


def test_dates_and_nwm30_version():
    """Make sure start/end dates work with specified NWM version."""
    nwm_version = "nwm30"
    start_date = parse("2023-11-20")
    end_date = parse("2023-11-20")
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = parse("2022-11-20")
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
    start_date = parse("2022-11-20")
    end_date = parse("2022-11-20")
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = parse("2023-11-20")
        end_date = parse("2025-11-20")
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
    start_date = parse("2021-04-30")
    end_date = parse("2021-04-30")
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = parse("2019-11-20")
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
    start_date = parse("2019-06-20")
    end_date = parse("2019-06-20")
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = parse("2018-11-20")
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
    start_date = parse("2018-11-20")
    end_date = parse("2018-11-20")
    validate_operational_start_end_date(nwm_version, start_date, end_date)

    try:
        failed = False
        start_date = parse("2017-11-20")
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
        start_dt=parse("2023-11-28"),
        end_dt=parse("2023-11-29"),
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


def test_building_nwm30_gcs_paths_alaska():
    """Test building NWM30 GCS paths."""
    gcs_component_paths = build_remote_nwm_filelist(
        configuration="forcing_analysis_assim_extend_alaska",
        output_type="forcing",
        start_dt=parse("2023-11-28"),
        end_dt=parse("2023-11-29"),
        analysis_config_dict=NWM30_ANALYSIS_CONFIG,
        t_minus_hours=[8],
        ignore_missing_file=False,
        prioritize_analysis_value_time=False,
        drop_overlapping_assimilation_values=True
    )
    assert len(gcs_component_paths) == 2
    assert (
        gcs_component_paths[0] == \
            'gcs://national-water-model/nwm.20231128/forcing_analysis_assim_extend_alaska/nwm.t20z.analysis_assim_extend.forcing.tm08.alaska.nc' # noqa
    )
    assert (
        gcs_component_paths[-1] == \
            'gcs://national-water-model/nwm.20231129/forcing_analysis_assim_extend_alaska/nwm.t20z.analysis_assim_extend.forcing.tm08.alaska.nc' # noqa
    )


def test_building_nwm22_gcs_paths():
    """Test building NWM22 GCS paths."""
    gcs_component_paths = build_remote_nwm_filelist(
        configuration="analysis_assim",
        output_type="channel_rt",
        start_dt=parse("2019-01-12"),
        end_dt=parse("2019-01-12"),
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
        start_dt=parse("2023-11-28"),
        end_dt=parse("2023-11-29"),
        analysis_config_dict=NWM30_ANALYSIS_CONFIG,
        t_minus_hours=[0],
        ignore_missing_file=False,
        prioritize_analysis_value_time=False,
        drop_overlapping_assimilation_values=False
    )

    gcs_component_paths = start_on_z_hour(
        gcs_component_paths=gcs_component_paths,
        start_z_hour=3
    )
    gcs_component_paths = end_on_z_hour(
        gcs_component_paths=gcs_component_paths,
        end_z_hour=12
    )

    assert gcs_component_paths[-1] == 'gcs://national-water-model/nwm.20231129/short_range/nwm.t12z.short_range.channel_rt.f018.conus.nc'  # noqa
    assert gcs_component_paths[0] == 'gcs://national-water-model/nwm.20231128/short_range/nwm.t03z.short_range.channel_rt.f001.conus.nc'  # noqa
    assert len(gcs_component_paths) == 612


def test_nwm_configuration_metadata():
    """Test the NWM configuration metadata."""
    nwm_configuration_name = "short_range"
    nwm_version = "nwm30"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_short_range"
    assert config_meta["description"] == "CONUS NWM short range, HRRR forcing"
    assert config_meta["member"] is None

    nwm_configuration_name = "analysis_assim_extend_no_da"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_analysis_assim_extend_no_da"
    assert config_meta["description"] == "CONUS NWM extended analysis, no nudging, STAGEIV forcing"

    nwm_configuration_name = "analysis_assim_extend"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_analysis_assim_extend"
    assert config_meta["description"] == "CONUS NWM extended analysis, with nudging, STAGEIV forcing"

    nwm_configuration_name = "medium_range_mem1"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_medium_range"
    assert config_meta["description"] == "CONUS NWM medium range, GFS forcing"
    assert config_meta["member"] == "1"

    nwm_configuration_name = "medium_range_mem6"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_medium_range"
    assert config_meta["description"] == "CONUS NWM medium range, GFS forcing"
    assert config_meta["member"] == "6"

    nwm_configuration_name = "medium_range_blend"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_medium_range_blend"
    assert config_meta["description"] == "CONUS NWM medium range, NBM forcing"

    nwm_configuration_name = "medium_range_alaska_mem1"
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name=nwm_configuration_name,
        nwm_version=nwm_version
    )
    assert config_meta["name"] == "nwm30_medium_range_alaska"
    assert config_meta["description"] == "Alaska NWM medium range, GFS forcing"


def test_nwm31_configuration_metadata():
    """Test the 3.1 descriptions that differ from the shared table."""
    # PRVI short range switched from NAM-NEST to NBM forcing in 3.1.
    for nwm_configuration_name, nwm30_desc, nwm31_desc in [
        (
            "short_range_puertorico",
            "PRVI NWM short range, NAM-NEST forcing",
            "PRVI NWM short range, NBM forcing",
        ),
        (
            "short_range_puertorico_no_da",
            "PRVI NWM short range, NAM-NEST forcing, initialized by no_da analysis_assim",
            "PRVI NWM short range, NBM forcing, initialized by no_da analysis_assim",
        ),
    ]:
        config_meta = format_nwm_configuration_metadata(
            nwm_config_name=nwm_configuration_name,
            nwm_version="nwm30"
        )
        assert config_meta["description"] == nwm30_desc

        config_meta = format_nwm_configuration_metadata(
            nwm_config_name=nwm_configuration_name,
            nwm_version=SupportedNWMOperationalVersionsEnum.nwm31
        )
        assert config_meta["name"] == f"nwm31_{nwm_configuration_name}"
        assert config_meta["description"] == nwm31_desc

    # Everything else is inherited from the shared table.
    config_meta = format_nwm_configuration_metadata(
        nwm_config_name="short_range",
        nwm_version="nwm31"
    )
    assert config_meta["name"] == "nwm31_short_range"
    assert config_meta["description"] == "CONUS NWM short range, HRRR forcing"


@pytest.mark.skip(reason="This must be run manually since it requires an isolated spark session")
def test_reading_nwm_operational_from_gcs():
    """Test reading NWM operational forcing data from GCS with sedona."""
    spark = create_spark_session(
        app_name="test_nwm_operational_from_gcs",
        enable_gcs=True
    )
    filepaths = [
        "gs://national-water-model/nwm.20260404/forcing_analysis_assim/nwm.t00z.analysis_assim.forcing.tm00.conus.nc"
    ]
    nc_sdf = (
        spark
        .read
        .format("binaryFile")
        .load(filepaths)
        .selectExpr("RS_FromNetCDF(content, 'RAINRATE', 'x', 'y') as raster", "path as filepath")
    )
    # Check that some data was returned
    assert nc_sdf.count() == 1
    assert "raster" in nc_sdf.columns
    assert "filepath" in nc_sdf.columns


def test_feature_id_positions_sorted():
    """Requested ids map to their positions, in the order requested."""
    feature_ids = np.array([10, 20, 30, 40, 50])
    positions = _feature_id_positions(feature_ids, np.array([40, 10, 30]))
    assert np.array_equal(positions, [3, 0, 2])
    assert np.array_equal(feature_ids[positions], [40, 10, 30])


def test_feature_id_positions_unsorted_coordinate():
    """An out-of-order coordinate still maps correctly rather than silently wrong."""
    feature_ids = np.array([50, 10, 40, 30, 20])
    positions = _feature_id_positions(feature_ids, np.array([30, 50]))
    assert np.array_equal(feature_ids[positions], [30, 50])


def test_feature_id_positions_duplicated_request():
    """The same id can be requested more than once."""
    feature_ids = np.array([10, 20, 30])
    positions = _feature_id_positions(feature_ids, np.array([20, 20, 10]))
    assert np.array_equal(feature_ids[positions], [20, 20, 10])


def test_feature_id_positions_missing_id_raises():
    """A location that isn't in the file is an error, not a silent drop."""
    feature_ids = np.array([10, 20, 30])
    with pytest.raises(ValueError, match="location_ids not found"):
        _feature_id_positions(feature_ids, np.array([20, 999]))


def test_feature_id_positions_beyond_range_raises():
    """Ids past either end of the coordinate are reported, not clipped."""
    feature_ids = np.array([10, 20, 30])
    with pytest.raises(ValueError, match="location_ids not found"):
        _feature_id_positions(feature_ids, np.array([1]))
    with pytest.raises(ValueError, match="location_ids not found"):
        _feature_id_positions(feature_ids, np.array([99]))


def test_read_nwm_global_attrs():
    """Global attributes come back from a remote NWM file's metadata alone."""
    attrs = read_nwm_global_attrs(
        "gcs://national-water-model/nwm.20240222/analysis_assim/nwm.t23z.analysis_assim.channel_rt.tm00.conus.nc"  # noqa
    )
    assert attrs["NWM_version_number"] == "v3.0"
    assert attrs["model_configuration"] == "analysis_and_assimilation"
    assert attrs["model_output_valid_time"] == "2024-02-22_23:00:00"


def test_read_nwm_global_attrs_for_bad_file():
    """A missing or corrupt file raises rather than returning empty attrs."""
    with pytest.raises(Exception):
        read_nwm_global_attrs(
            "gcs://national-water-model/nwm.20240125/forcing_medium_range/nwm.t18z.medium_range.forcing.f104.conus.nc"  # noqa
        )


# One channel_rt file per NWM era, keyed by the version it reports. Also pins
# normalization: nwm12 writes "NWM 1.2", later eras "v2.0".."v3.1".
ERA_FILES = {
    "1.2": "nwm.20180918/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm01.conus.nc",  # noqa
    "2.0": "nwm.20190620/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc",  # noqa
    "2.1": "nwm.20210421/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc",  # noqa
    "2.2": "nwm.20230501/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc",  # noqa
    "3.0": "nwm.20240222/analysis_assim/nwm.t23z.analysis_assim.channel_rt.tm00.conus.nc",  # noqa
    "3.1": "nwm.20260915/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc",  # noqa
}
# nwm12-era forcing files record only their init/valid times, no version.
UNVERSIONED_FILE = "nwm.20180918/forcing_analysis_assim/nwm.t00z.analysis_assim.forcing.tm00.conus.nc"  # noqa


def _era_path(version):
    return f"gcs://national-water-model/{ERA_FILES[version]}"


@pytest.mark.parametrize("expected", list(ERA_FILES))
def test_read_nwm_file_version_across_eras(expected):
    """Each era's version attribute normalizes to bare digits."""
    assert read_nwm_file_version(_era_path(expected)) == expected


def test_read_nwm_file_version_when_absent():
    """A file with no version attribute returns None rather than raising."""
    assert read_nwm_file_version(
        f"gcs://national-water-model/{UNVERSIONED_FILE}"
    ) is None


@pytest.mark.parametrize("nwm_version,file_version", [
    ("nwm12", "1.2"),
    ("nwm20", "2.0"),
    ("nwm21", "2.1"),
    ("nwm30", "3.0"),
    ("nwm31", "3.1"),
])
def test_validate_nwm_version_matching(nwm_version, file_version):
    """A file stamped with the requested version passes."""
    validate_nwm_version_against_files(
        [_era_path(file_version)], nwm_version
    )


@pytest.mark.parametrize("nwm_version", ["nwm21", "nwm22"])
@pytest.mark.parametrize("file_version", ["2.1", "2.2"])
def test_validate_nwm_version_21_and_22_interchangeable(
    nwm_version, file_version
):
    """v2.1 and v2.2 are treated as one version, in either direction."""
    validate_nwm_version_against_files(
        [_era_path(file_version)], nwm_version
    )


def test_validate_nwm_version_mismatch_raises():
    """A file from a different era is an error, not a silent relabel."""
    with pytest.raises(ValueError, match="NWM version mismatch"):
        validate_nwm_version_against_files([_era_path("3.0")], "nwm21")


def test_validate_nwm_version_checks_both_ends():
    """A range straddling a version boundary raises on the far end alone."""
    with pytest.raises(ValueError, match="reports 3.1"):
        validate_nwm_version_against_files(
            [_era_path("3.0"), _era_path("3.1")], "nwm30"
        )
    with pytest.raises(ValueError, match="reports 3.0"):
        validate_nwm_version_against_files(
            [_era_path("3.0"), _era_path("3.1")], "nwm31"
        )


def test_validate_nwm_version_skips_unversioned_file():
    """A file carrying no version attribute cannot disagree, so it passes."""
    validate_nwm_version_against_files(
        [f"gcs://national-water-model/{UNVERSIONED_FILE}"], "nwm12"
    )


def test_validate_nwm_version_empty_and_unknown():
    """No paths is a no-op; an unmapped version is a programming error."""
    validate_nwm_version_against_files([], "nwm30")
    with pytest.raises(ValueError, match="NWM_VERSION_ATTR_VALUES"):
        validate_nwm_version_against_files([_era_path("3.0")], "nwm99")


def _analysis_assim_path(when):
    """Build the analysis_assim channel_rt path for a given cycle."""
    return (
        f"gcs://national-water-model/nwm.{when:%Y%m%d}/analysis_assim/"
        f"nwm.t{when.hour:02d}z.analysis_assim.channel_rt.tm00.conus.nc"
    )


@pytest.mark.parametrize("boundary,version", NWM_VERSION_BOUNDARIES[1:])
def test_nwm_version_boundaries(boundary, version):
    """Each boundary is the exact cycle the new version starts, to the z-hour.

    Reads the boundary cycle and the one before it, so a constant drifting to
    the wrong day *or* hour fails here. Skips the earliest entry, an
    availability floor rather than a switch.
    """
    assert read_nwm_file_version(_analysis_assim_path(boundary)) == version

    previous = boundary - timedelta(hours=1)
    assert read_nwm_file_version(_analysis_assim_path(previous)) != version


@pytest.mark.parametrize("nwm_version,boundary", [
    ("nwm20", NWM20_START_DATE),
    ("nwm21", NWM21_START_DATE),
    ("nwm30", NWM30_START_DATE),
    ("nwm31", NWM31_START_DATE),
])
def test_dates_validated_at_z_hour_resolution(nwm_version, boundary):
    """The cycle before a boundary is rejected, the boundary itself accepted.

    A day-level comparison would accept the whole switchover day, including
    the hours still produced by the previous version.
    """
    validate_operational_start_end_date(nwm_version, boundary, boundary)

    previous = boundary - timedelta(hours=1)
    with pytest.raises(ValueError):
        validate_operational_start_end_date(nwm_version, previous, previous)


# Adjacent cycles on the v3.1 switchover day (2026-08-18): one still v3.0, the
# next already v3.1.
SWITCHOVER_V30_FILE = "gcs://national-water-model/nwm.20260818/analysis_assim/nwm.t04z.analysis_assim.channel_rt.tm00.conus.nc"  # noqa
SWITCHOVER_V31_FILE = "gcs://national-water-model/nwm.20260818/analysis_assim/nwm.t05z.analysis_assim.channel_rt.tm00.conus.nc"  # noqa


def test_switchover_day_really_is_mixed():
    """The premise: one switchover day holds both versions, cycle by cycle."""
    assert read_nwm_file_version(SWITCHOVER_V30_FILE) == "3.0"
    assert read_nwm_file_version(SWITCHOVER_V31_FILE) == "3.1"


def test_outgoing_version_on_switchover_day_is_tolerated(caplog):
    """An outgoing-version rerun warns instead of failing the fetch."""
    with caplog.at_level(logging.WARNING):
        validate_nwm_version_against_files(
            [SWITCHOVER_V30_FILE, SWITCHOVER_V31_FILE], "nwm31"
        )
    assert "reports NWM version 3.0" in caplog.text
    assert "switchover" in caplog.text


def test_matching_file_on_switchover_day_is_silent():
    """The grace window does not warn about files that already agree."""
    validate_nwm_version_against_files([SWITCHOVER_V31_FILE], "nwm31")


def test_outgoing_version_outside_grace_still_raises():
    """Past the grace window, an old-version file is a real error."""
    with pytest.raises(ValueError, match="NWM version mismatch"):
        validate_nwm_version_against_files([_era_path("3.0")], "nwm31")


def test_nwm_version_at_boundaries():
    """The in-force version steps exactly at each boundary."""
    assert nwm_version_at(NWM12_START_DATE - timedelta(hours=1)) is None
    for boundary, version in NWM_VERSION_BOUNDARIES:
        assert nwm_version_at(boundary) == version
    assert nwm_version_at(NWM30_START_DATE - timedelta(hours=1)) == "2.1"
    assert nwm_version_at(NWM31_START_DATE + timedelta(days=365)) == "3.1"


def test_dates_and_nwm31_version():
    """nwm30 stops where nwm31 starts, at the z-hour."""
    last_v30 = NWM31_START_DATE - timedelta(hours=1)
    validate_operational_start_end_date("nwm30", NWM30_START_DATE, last_v30)

    with pytest.raises(ValueError):
        validate_operational_start_end_date(
            "nwm30", NWM30_START_DATE, NWM31_START_DATE
        )

    validate_operational_start_end_date(
        "nwm31", NWM31_START_DATE, NWM31_START_DATE + timedelta(days=1)
    )
