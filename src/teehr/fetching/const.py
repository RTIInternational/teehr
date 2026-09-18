"""Data types for the TEEHR schema."""
from datetime import datetime, timedelta

import numpy as np

VALUE = "value"
VALUE_TIME = "value_time"
REFERENCE_TIME = "reference_time"
LOCATION_ID = "location_id"
UNIT_NAME = "unit_name"
VARIABLE_NAME = "variable_name"
CONFIGURATION_NAME = "configuration_name"
MEMBER = "member"

USGS_NODATA_VALUES = [-999999, -999, -9999, -99999]
USGS_CONFIGURATION_NAME = "usgs_observations"

NWM_BUCKET = "national-water-model"

# First cycle of each NWM version, read from the files' own attributes and
# pinned by test_nwm_version_boundaries. Switches land on a forecast cycle, not
# midnight, so comparing whole days accepts up to 14 hours of the neighbouring
# version. v1.2's entry is the earliest data teehr reads, not a switch.
NWM31_START_DATE = datetime(2026, 8, 18, 0)   # t00z
NWM30_START_DATE = datetime(2023, 9, 19, 12)  # t12z; t00z-t11z still v2.2
NWM21_START_DATE = datetime(2021, 4, 20, 14)  # t14z; v2.1 and 2.2 are the same
NWM20_START_DATE = datetime(2019, 6, 19, 14)  # t14z; t00z-t13z still v1.2
NWM12_START_DATE = datetime(2018, 9, 17, 0)

# The boundaries in order, each paired with the version in force from it until
# the next. v2.2 has no entry: teehr treats v2.1 and v2.2 as one version. This
# is NOAA's intent, not a promise about every file -- read the file's own
# attribute (read_nwm_file_version) when it has to be exact.
NWM_VERSION_BOUNDARIES = (
    (NWM12_START_DATE, "1.2"),
    (NWM20_START_DATE, "2.0"),
    (NWM21_START_DATE, "2.1"),
    (NWM30_START_DATE, "3.0"),
    (NWM31_START_DATE, "3.1"),
)

# How long after a boundary a file still reporting the outgoing version is
# accepted (with a warning) rather than raising. NOAA reruns some cycles on the
# outgoing system mid-switch, scattered across cycles and configurations -- a
# quarter of the files sampled on 2026-08-18 still said v3.0, interleaved with
# v3.1 -- so they can be bounded in time but not enumerated. One day suffices:
# 2026-08-19 through 08-21 sampled clean.
NWM_VERSION_SWITCHOVER_GRACE = timedelta(days=1)

# Version(s) each SupportedNWMOperationalVersionsEnum member accepts from a
# file's attributes, normalized by _normalize_nwm_version_attr. The nwm12 era
# writes model_version ("NWM 1.2"), later eras NWM_version_number ("v2.0"...).
# nwm21 and nwm22 share an entry, as above.
NWM_VERSION_ATTR_VALUES = {
    "nwm12": frozenset({"1.2"}),
    "nwm20": frozenset({"2.0"}),
    "nwm21": frozenset({"2.1", "2.2"}),
    "nwm22": frozenset({"2.1", "2.2"}),
    "nwm30": frozenset({"3.0"}),
    "nwm31": frozenset({"3.1"}),
}

# Global attributes that carry the model version, newest convention first.
NWM_VERSION_ATTRS = ("NWM_version_number", "model_version")

NWM_S3_JSON_PATH = "s3://ciroh-nwm-zarr-copy"

# Each public bucket teehr reads, and the region it actually lives in. A wrong
# entry surfaces as "Received redirect without LOCATION" rather than anything
# mentioning regions, so verify against the bucket before adding one.
S3_BUCKET_REGIONS = {
    "ciroh-nwm-zarr-copy": "us-east-1",
    "ciroh-nwm-zarr-retrospective-data-copy": "us-east-1",
    "noaa-nwm-retro-v2-zarr-pds": "us-west-2",
    "noaa-nwm-retrospective-2-1-zarr-pds": "us-east-1",
    "noaa-nwm-retrospective-3-0-pds": "us-east-1",
}
# Buckets we have not pinned fall back to this, which is right for most of
# NOAA's open data.
DEFAULT_S3_REGION = "us-east-1"

USGS_VARIABLE_MAPPER = {
    VARIABLE_NAME: {
        "iv": "streamflow_none_inst",
        "dv": "streamflow_daily_mean",
    },
    UNIT_NAME: {
        "SI": "m^3/s",
        "Imperial": "ft^3/s",
    },
}

NWM_CONFIGURATION_DESCRIPTIONS = {
    # conus
    "analysis_assim_extend_no_da": "CONUS NWM extended analysis, no nudging, STAGEIV forcing",
    "analysis_assim_extend": "CONUS NWM extended analysis, with nudging, STAGEIV forcing",
    "analysis_assim_no_da": "CONUS NWM standard analysis, no nudging, MRMS forcing",
    "analysis_assim": "CONUS NWM standard analysis, with nudging, MRMS forcing",
    "short_range": "CONUS NWM short range, HRRR forcing",
    "medium_range_mem": "CONUS NWM medium range, GFS forcing",
    "medium_range_blend": "CONUS NWM medium range, NBM forcing",
    "medium_range_no_da": "CONUS NWM medium range, GFS forcing, initialized by no_da analysis_assim",
    "forcing_analysis_assim_extend": "CONUS STAGEIV mean areal forcing for NWM extended analysis",
    "forcing_analysis_assim": "CONUS MRMS mean areal forcing for NWM standard analysis",
    "forcing_short_range": "CONUS HRRR mean areal forcing for NWM short range",
    "forcing_medium_range": "CONUS GFS mean areal forcing for NWM medium range mem1",
    "forcing_medium_range_blend": "CONUS NBM mean areal forcing for NWM medium range blend",
    # hawaii
    "analysis_assim_hawaii_no_da": "Hawaii NWM standard analysis, no nudging, MRMS forcing",
    "analysis_assim_hawaii": "Hawaii NWM standard analysis, with nudging, MRMS forcing",
    "short_range_hawaii": "Hawaii NWM short range, NAM-NEST forcing",
    "short_range_hawaii_no_da": "Hawaii NWM short range, NAM-NEST forcing, initialized by no_da analysis_assim",
    "forcing_analysis_assim_hawaii": "Hawaii MRMS mean areal forcing for NWM standard analysis",
    "forcing_short_range_hawaii": "Hawaii NAM-NEST mean areal forcing for NWM short range",
    # alaska
    "analysis_assim_extend_alaska_no_da": "Alaska NWM extended analysis, no nudging, APRFC MPE forcing",
    "analysis_assim_extend_alaska": "Alaska NWM extended analysis, with nudging, APRFC MPE forcing",
    "analysis_assim_alaska_no_da": "Alaska NWM standard analysis, no nudging, MRMS forcing",
    "analysis_assim_alaska": "Alaska NWM standard analysis, with nudging, MRMS forcing",
    "short_range_alaska": "Alaska NWM short range, HRRR-AK/NBM forcing",
    "medium_range_alaska_mem": "Alaska NWM medium range, GFS forcing",
    "medium_range_blend_alaska": "Alaska NWM medium range, NBM forcing",
    "forcing_analysis_assim_extend_alaska": "Alaska StageIV mean areal forcing for NWM extended analysis",
    "forcing_analysis_assim_alaska": "Alaska MRMS mean areal forcing for NWM standard analysis",
    "forcing_short_range_alaska": "Alaska HRRR mean areal forcing for NWM short range",
    "forcing_medium_range_alaska": "Alaska GFS mean areal forcing for NWM medium range mem1",
    "forcing_medium_range_blend_alaska": "Alaska NBM mean areal forcing for NWM medium range blend",
    # puerto rico
    "analysis_assim_puertorico_no_da": "PRVI NWM standard analysis, no nudging, MRMS forcing",
    "analysis_assim_puertorico": "PRVI NWM standard analysis, with nudging, MRMS forcing",
    "short_range_puertorico": "PRVI NWM short range, NAM-NEST forcing",
    "short_range_puertorico_no_da": "PRVI NWM short range, NAM-NEST forcing, initialized by no_da analysis_assim",
    "forcing_analysis_assim_puertorico": "PRVI MRMS mean areal forcing for NWM standard analysis",
    "forcing_short_range_puertorico": "PRVI NAM-NEST mean areal forcing for NWM short range",
}

# NWM 3.1 switched the PRVI short range forcing from NAM-NEST to NBM. Only the
# entries that differ are written out; the rest are inherited, so a description
# added above reaches 3.1 too.
NWM31_CONFIGURATION_DESCRIPTIONS = NWM_CONFIGURATION_DESCRIPTIONS | {
    "forcing_short_range_puertorico": "PRVI NBM mean areal forcing for NWM short range",
    "short_range_puertorico": "PRVI NWM short range, NBM forcing",
    "short_range_puertorico_no_da": "PRVI NWM short range, NBM forcing, initialized by no_da analysis_assim",
}

# Versions absent here use NWM_CONFIGURATION_DESCRIPTIONS.
NWM_CONFIGURATION_DESCRIPTIONS_BY_VERSION = {
    "nwm31": NWM31_CONFIGURATION_DESCRIPTIONS,
}

NWM_HAWAII_VARIABLE_MAPPER = {
    VARIABLE_NAME: {
        "streamflow": {"name": "streamflow_15min_inst", "long_name": "15-minute Instantaneous Streamflow"},
        "RAINRATE": {"name": "rainrate_hourly_mean", "long_name": "Hourly Mean Rainfall Rate"},
        "T2D": {"name": "temperature_hourly_mean", "long_name": "Hourly Mean Temperature"}
    },
    UNIT_NAME: {
        "m3 s-1": {"name": "m^3/s", "long_name": "Cubic Meters per Second"},
        "mm s^-1": {"name": "mm/s", "long_name": "Millimeters per Second"},  # NWM 3.0 forcing
        "mm s-1": {"name": "mm/s", "long_name": "Millimeters per Second"},   # NWM 2.2 forcing
    }
}

NWM_VARIABLE_MAPPER = {
    VARIABLE_NAME: {
        "streamflow": {"name": "streamflow_hourly_inst", "long_name": "Hourly Instantaneous Streamflow"},
        "RAINRATE": {"name": "rainrate_hourly_mean", "long_name": "Hourly Mean Rainfall Rate"},
        "T2D": {"name": "temperature_hourly_mean", "long_name": "Hourly Mean Temperature"}
    },
    UNIT_NAME: {
        "m3 s-1": {"name": "m^3/s", "long_name": "Cubic Meters per Second"},
        "mm s^-1": {"name": "mm/s", "long_name": "Millimeters per Second"},  # NWM 3.0 forcing
        "mm s-1": {"name": "mm/s", "long_name": "Millimeters per Second"},   # NWM 2.2 forcing
    }
}

NWM12_ANALYSIS_CONFIG = {
    "analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    }
}

NWM20_ANALYSIS_CONFIG = {
    "analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "analysis_assim_long": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_long",
    },
    "analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "forcing_analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim",
    }
}


NWM22_ANALYSIS_CONFIG = {
    "analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "analysis_assim_extend_no_da": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend_no_da",
    },
    "analysis_assim_long": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_long",
    },
    "analysis_assim_long_no_da": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_long_no_da",
    },
    "analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_hawaii_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_puertorico": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_puertorico_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "forcing_analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "forcing_analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_puertorico": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "configuration_name_in_filepath": "analysis_assim",
    },
}

NWM30_ANALYSIS_CONFIG = {
    "analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "analysis_assim_extend_no_da": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend_no_da",
    },
    "analysis_assim_long": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_long",
    },
    "analysis_assim_long_no_da": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_long_no_da",
    },
    "analysis_assim_alaska": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "alaska",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_alaska_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "alaska",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_extend_alaska": {
        "num_lookback_hrs": 32,
        "cycle_z_hours": [20],
        "domain": "alaska",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "analysis_assim_extend_alaska_no_da": {
        "num_lookback_hrs": 32,
        "cycle_z_hours": [20],
        "domain": "alaska",
        "configuration_name_in_filepath": "analysis_assim_extend_no_da",
    },
    "analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_hawaii_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_puertorico": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_puertorico_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "configuration_name_in_filepath": "analysis_assim_no_da",
    },
    "forcing_analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "forcing_analysis_assim_alaska": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "alaska",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_extend_alaska": {
        "num_lookback_hrs": 32,
        "cycle_z_hours": [20],
        "domain": "alaska",
        "configuration_name_in_filepath": "analysis_assim_extend",
    },
    "forcing_analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "configuration_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_puertorico": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "configuration_name_in_filepath": "analysis_assim",
    },
}


# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]' # noqa

HI_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-157.42],PARAMETER["standard_parallel_1",10.0],\
PARAMETER["standard_parallel_2",30.0],PARAMETER["latitude_of_origin",20.6],UNIT["Meter",1.0]]' # noqa

PR_NWM_WKT = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-65.91],PARAMETER["standard_parallel_1",18.1],\
PARAMETER["standard_parallel_2",18.1],PARAMETER["latitude_of_origin",18.1],UNIT["Meter",1.0]]' # noqa

AL_NWM_WKT = 'PROJCS["Sphere_Stereographic",GEOGCS["Sphere",DATUM["Sphere",SPHEROID["unnamed",6370000,0]], \
PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]], \
PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",60],PARAMETER["central_meridian",-135], \
PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1],AXIS["Easting",SOUTH], \
AXIS["Northing",SOUTH]]' # noqa
