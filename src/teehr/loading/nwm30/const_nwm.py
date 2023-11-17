import numpy as np

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
AL_NWM_WKT = 'PROJCS["Sphere_Stereographic",GEOGCS["Sphere",DATUM["Sphere",SPHEROID["unnamed",6370000,0]], \
    PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]], \
    PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",60],PARAMETER["central_meridian",-135], \
    PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1],AXIS["Easting",SOUTH], \
    AXIS["Northing",SOUTH]]' # noqa