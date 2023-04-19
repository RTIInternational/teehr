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

NWM22_LAND_VARS_SHORT = ["ACCET", "SNOWT_AVG", "SOILSAT_TOP", "FSNO", "SNOWH", "SNEQV"]

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
}

NWM22_ANALYSIS_CONFIG = {
    "analysis_assim": {"tm": 3, "timestep": "1H"},
    "analysis_assim_no_da": {"tm": 3, "timestep": "1H"},
    "analysis_assim_extend": {"tm": 28, "timestep": "1H"},
    "analysis_assim_extend_no_da": {"tm": 28, "timestep": "1H"},
    "analysis_assim_long": {"tm": 12, "timestep": "1H"},
    "analysis_assim_long_no_da": {"tm": 12, "timestep": "1H"},
    "analysis_assim_hawaii": {"tm": 12, "timestep": "15min"},
    "analysis_assim_hawaii_no_da": {"tm": 12, "timestep": "15min"},
    "analysis_assim_puertorico": {"tm": 3, "timestep": "1H"},
    "analysis_assim_puertorico_no_da": {"tm": 12, "timestep": "1H"},
}
