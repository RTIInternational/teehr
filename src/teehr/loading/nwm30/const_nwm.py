import numpy as np

NWM_BUCKET = "national-water-model"

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

NWM30_UNIT_LOOKUP = {"m3 s-1": "m3/s"}

# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'  # noqa

HI_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-157.42],PARAMETER["standard_parallel_1",10.0],\
PARAMETER["standard_parallel_2",30.0],PARAMETER["latitude_of_origin",20.6],UNIT["Meter",1.0]]'  # noqa

PR_NWM_WKT = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-65.91],PARAMETER["standard_parallel_1",18.1],\
PARAMETER["standard_parallel_2",18.1],PARAMETER["latitude_of_origin",18.1],UNIT["Meter",1.0]]'  # noqa
