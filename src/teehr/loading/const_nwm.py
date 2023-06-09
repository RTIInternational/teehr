import numpy as np

NWM_BUCKET = "national-water-model"

NWM22_CHANNEL_RT_VARS = [
    "nudge",
    "qBtmVertRunoff",
    "qBucket",
    "qSfcLatRunoff",
    "streamflow",
    "velocity",
]

NWM22_CHANNEL_RT_VARS_NO_DA = [
    "nudge",
    "qBucket",
    "qSfcLatRunoff",
    "streamflow",
    "velocity",
]

NWM22_CHANNEL_RT_VARS_LONG = ["nudge", "streamflow", "velocity"]

NWM22_TERRAIN_VARS = ["sfcheadsubrt", "zwattablrt"]

NWM22_RESERVOIR_VARS = [
    "inflow",
    "outflow",
    "reservoir_assimiated_value",
    "water_sfc_elev",
]

NWM22_LAND_VARS_ASSIM = [
    "ACCET",
    "ACSNOM",
    "EDIR",
    "FSNO",
    "ISNOW",
    "QRAIN",
    "QSNOW",
    "SNEQV",
    "SNLIQ",
    "SNOWH",
    "SNOWT_AVG",
    "SOILICE",
    "SOILSAT_TOP",
    "SOIL_M",
    "SOIL_T",
]

NWM22_LAND_VARS_SHORT = [
    "ACCET",
    "SNOWT_AVG",
    "SOILSAT_TOP",
    "FSNO",
    "SNOWH",
    "SNEQV",
]

NWM22_LAND_VARS_MEDIUM = [
    "FSA",
    "FIRA",
    "GRDFLX",
    "HFX",
    "LH",
    "UGDRNOFF",
    "ACCECAN",
    "ACCEDIR",
    "ACCETRAN",
    "TRAD",
    "SNLIQ",
    "SOIL_T",
    "SOIL_M",
    "SNOWH",
    "SNEQV",
    "ISNOW",
    "FSNO",
    "ACSNOM",
    "ACCET",
    "CANWAT",
    "SOILICE",
    "SOILSAT_TOP",
    "SNOWT_AVG",
]

NWM22_LAND_VARS_LONG = [
    "UGDRNOFF",
    "SFCRNOFF",
    "SNEQV",
    "ACSNOM",
    "ACCET",
    "CANWAT",
    "SOILSAT_TOP",
    "SOILSAT",
]

NWM22_FORCING_VARS = [
    "U2D",
    "V2D",
    "T2D",
    "Q2D",
    "LWDOWN",
    "SWDOWN",
    "RAINRATE",
    "PSFC",
]

NWM22_RUN_CONFIG = {
    "analysis_assim": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_extend": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_extend_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_long": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_long_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_hawaii": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_hawaii_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_puertorico": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "analysis_assim_puertorico_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_ASSIM,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "short_range": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_SHORT,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "short_range_hawaii": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_SHORT,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "short_range_puertorico": {
        "channel_rt": NWM22_CHANNEL_RT_VARS,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_SHORT,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "short_range_hawaii_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_SHORT,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "short_range_puertorico_no_da": {
        "channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA,
        "terrain_rt": NWM22_TERRAIN_VARS,
        "land": NWM22_LAND_VARS_SHORT,
        "reservoir": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem1": {
        "channel_rt_1": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_1": NWM22_TERRAIN_VARS,
        "land_1": NWM22_LAND_VARS_MEDIUM,
        "reservoir_1": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem2": {
        "channel_rt_2": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_2": NWM22_TERRAIN_VARS,
        "land_2": NWM22_LAND_VARS_MEDIUM,
        "reservoir_2": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem3": {
        "channel_rt_3": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_3": NWM22_TERRAIN_VARS,
        "land_3": NWM22_LAND_VARS_MEDIUM,
        "reservoir_3": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem4": {
        "channel_rt_4": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_4": NWM22_TERRAIN_VARS,
        "land_4": NWM22_LAND_VARS_MEDIUM,
        "reservoir_4": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem5": {
        "channel_rt_5": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_5": NWM22_TERRAIN_VARS,
        "land_5": NWM22_LAND_VARS_MEDIUM,
        "reservoir_5": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem6": {
        "channel_rt_6": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_6": NWM22_TERRAIN_VARS,
        "land_6": NWM22_LAND_VARS_MEDIUM,
        "reservoir_6": NWM22_RESERVOIR_VARS,
    },
    "medium_range_mem7": {
        "channel_rt_7": NWM22_CHANNEL_RT_VARS,
        "terrain_rt_7": NWM22_TERRAIN_VARS,
        "land_7": NWM22_LAND_VARS_MEDIUM,
        "reservoir_7": NWM22_RESERVOIR_VARS,
    },
    "medium_range_no_da": {"channel_rt": NWM22_CHANNEL_RT_VARS_NO_DA},
    "long_range_mem1": {
        "channel_rt_1": NWM22_CHANNEL_RT_VARS_LONG,
        "land_1": NWM22_LAND_VARS_LONG,
        "reservoir_1": NWM22_RESERVOIR_VARS,
    },
    "long_range_mem2": {
        "channel_rt_2": NWM22_CHANNEL_RT_VARS_LONG,
        "land_2": NWM22_LAND_VARS_LONG,
        "reservoir_2": NWM22_RESERVOIR_VARS,
    },
    "long_range_mem3": {
        "channel_rt_3": NWM22_CHANNEL_RT_VARS_LONG,
        "land_3": NWM22_LAND_VARS_LONG,
        "reservoir_3": NWM22_RESERVOIR_VARS,
    },
    "long_range_mem4": {
        "channel_rt_4": NWM22_CHANNEL_RT_VARS_LONG,
        "land_4": NWM22_LAND_VARS_LONG,
        "reservoir_4": NWM22_RESERVOIR_VARS,
    },
    "forcing_medium_range": {"forcing": NWM22_FORCING_VARS},
    "forcing_short_range": {"forcing": NWM22_FORCING_VARS},
    "forcing_short_range_hawaii": {"forcing": NWM22_FORCING_VARS},
    "forcing_short_range_puertorico": {"forcing": NWM22_FORCING_VARS},
    "forcing_analysis_assim": {"forcing": NWM22_FORCING_VARS},
    "forcing_analysis_assim_extend": {"forcing": NWM22_FORCING_VARS},
    "forcing_analysis_assim_hawaii": {"forcing": NWM22_FORCING_VARS},
    "forcing_analysis_assim_puertorico": {"forcing": NWM22_FORCING_VARS},
}

NWM22_ANALYSIS_CONFIG = {
    "analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim_extend",
    },
    "analysis_assim_extend_no_da": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim_extend_no_da",
    },
    "analysis_assim_long": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim_long",
    },
    "analysis_assim_long_no_da": {
        "num_lookback_hrs": 12,
        "cycle_z_hours": np.arange(0, 24, 6),
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim_long_no_da",
    },
    "analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "run_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_hawaii_no_da": {
        "num_lookback_hrs": 3,
        "cycle_cycle_z_hourstimes": np.arange(0, 24, 1),
        "domain": "hawaii",
        "run_name_in_filepath": "analysis_assim_no_da",
    },
    "analysis_assim_puertorico": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "run_name_in_filepath": "analysis_assim",
    },
    "analysis_assim_puertorico_no_da": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "run_name_in_filepath": "analysis_assim_no_da",
    },
    "forcing_analysis_assim": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_extend": {
        "num_lookback_hrs": 28,
        "cycle_z_hours": [16],
        "domain": "conus",
        "run_name_in_filepath": "analysis_assim_extend",
    },
    "forcing_analysis_assim_hawaii": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "hawaii",
        "run_name_in_filepath": "analysis_assim",
    },
    "forcing_analysis_assim_puertorico": {
        "num_lookback_hrs": 3,
        "cycle_z_hours": np.arange(0, 24, 1),
        "domain": "puertorico",
        "run_name_in_filepath": "analysis_assim",
    },
}


NWM22_UNIT_LOOKUP = {"m3 s-1": "m3/s"}

# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'

HI_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-157.42],PARAMETER["standard_parallel_1",10.0],\
PARAMETER["standard_parallel_2",30.0],PARAMETER["latitude_of_origin",20.6],UNIT["Meter",1.0]]'

PR_NWM_WKT = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-65.91],PARAMETER["standard_parallel_1",18.1],\
PARAMETER["standard_parallel_2",18.1],PARAMETER["latitude_of_origin",18.1],UNIT["Meter",1.0]]'
