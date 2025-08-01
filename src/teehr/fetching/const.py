"""Data types for the TEEHR schema."""
from datetime import datetime

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
NWM22_UNIT_LOOKUP = {"m3 s-1": "m^3/s"}

NWM30_START_DATE = datetime(2023, 9, 19, 0)
NWM21_START_DATE = datetime(2021, 4, 20, 0)  # v2.1 and 2.2 are the same
NWM20_START_DATE = datetime(2019, 6, 19, 0)
NWM12_START_DATE = datetime(2018, 9, 17, 0)

NWM_S3_JSON_PATH = "s3://ciroh-nwm-zarr-copy"

USGS_VARIABLE_MAPPER = {
    VARIABLE_NAME: {
        "iv": "streamflow_hourly_inst",
        "dv": "streamflow_daily_mean",
    },
    UNIT_NAME: {
        "SI": "m^3/s",
        "Imperial": "ft^3/s",
    },
}

NWM_CONFIGURATION_DESCRIPTIONS = {
    "analysis_assim_extend_no_da": "NWM analysis extended - no nudging - STAGE IV forcing",
    "analysis_assim_extend": "NWM analysis extended - with nudging - STAGE IV forcing",
    "medium_range_mem": "NWM medium range - GFS forcing member",
    "medium_range_blend": "NWM medium range - NBM forcing",
    "short_range": "NWM short range - HRRR forcing"
}

NWM_VARIABLE_MAPPER = {
    VARIABLE_NAME: {
        "streamflow": "streamflow_hourly_inst",
        "RAINRATE": "rainfall_hourly_rate",
    },
    UNIT_NAME: {
        "m3 s-1": "m^3/s",
        "mm s^-1": "mm/s",  # NWM 3.0 forcing
        "mm s-1": "mm/s",   # NWM 2.2 forcing
    },
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
        "cycle_cycle_z_hourstimes": np.arange(0, 24, 1),
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
        "cycle_cycle_z_hourstimes": np.arange(0, 24, 1),
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