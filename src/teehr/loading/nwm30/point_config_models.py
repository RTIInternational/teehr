from enum import Enum
from typing import Optional

from pydantic import BaseModel


# VARIABLE ENUMS: Potential variable names for each output_type
class ChannelRtVariableEnum(str, Enum):
    nudge = "nudge"
    qBtmVertRunoff = "qBtmVertRunoff"
    qBucket = "qBucket"
    qSfcLatRunoff = "qSfcLatRunoff"
    streamflow = "streamflow"
    velocity = "velocity"


class ChannelRtNoDAVariableEnum(str, Enum):
    nudge = "nudge"
    qBucket = "qBucket"
    qSfcLatRunoff = "qSfcLatRunoff"
    streamflow = "streamflow"
    velocity = "velocity"


class ChannelRtLongVariableEnum(str, Enum):
    nudge = "nudge"
    streamflow = "streamflow"
    velocity = "velocity"


class TerrainRtVariableEnum(str, Enum):
    sfcheadsubrt = "sfcheadsubrt"
    zwattablrt = "zwattablrt"


class ReservoirVariableEnum(str, Enum):
    inflow = "inflow"
    outflow = "outflow"
    reservoir_assimiated_value = "reservoir_assimiated_value"
    water_sfc_elev = "water_sfc_elev"


# OUTPUT ENUMS: Potential output_type names for each configuration
class ShortAndAnalysisOutputEnum(str, Enum):
    channel_rt = "channel_rt"
    terrain_rt = "terrain_rt"
    reservoir = "reservoir"


class Medium1OutputEnum(str, Enum):
    channel_rt_1 = "channel_rt_1"
    terrain_rt_1 = "terrain_rt_1"
    reservoir_1 = "reservoir_1"


class Medium2OutputEnum(str, Enum):
    channel_rt_2 = "channel_rt_2"
    terrain_rt_2 = "terrain_rt_2"
    reservoir_2 = "reservoir_2"


class Medium3OutputEnum(str, Enum):
    channel_rt_3 = "channel_rt_3"
    terrain_rt_3 = "terrain_rt_3"
    reservoir_3 = "reservoir_3"


class Medium4OutputEnum(str, Enum):
    channel_rt_4 = "channel_rt_4"
    terrain_rt_4 = "terrain_rt_4"
    reservoir_4 = "reservoir_4"


class Medium5OutputEnum(str, Enum):
    channel_rt_5 = "channel_rt_5"
    terrain_rt_5 = "terrain_rt_5"
    reservoir_5 = "reservoir_5"


class Medium6OutputEnum(str, Enum):
    channel_rt_6 = "channel_rt_6"
    terrain_rt_6 = "terrain_rt_6"
    reservoir_6 = "reservoir_6"


class Medium7OutputEnum(str, Enum):
    channel_rt_7 = "channel_rt_7"
    terrain_rt_7 = "terrain_rt_7"
    reservoir_7 = "reservoir_7"


class MediumNoDAEnum(str, Enum):
    channel_rt = "channel_rt"


class Long1OutputEnum(str, Enum):
    channel_rt_1 = "channel_rt_1"
    reservoir_1 = "reservoir_1"


class Long2OutputEnum(str, Enum):
    channel_rt_2 = "channel_rt_2"
    reservoir_2 = "reservoir_2"


class Long3OutputEnum(str, Enum):
    channel_rt_3 = "channel_rt_3"
    reservoir_3 = "reservoir_3"


class Long4OutputEnum(str, Enum):
    channel_rt_4 = "channel_rt_4"
    reservoir_4 = "reservoir_4"


# POINT OUTPUT TYPE MODELS
class ShortAndAnalysis(BaseModel):
    output_type: ShortAndAnalysisOutputEnum
    channel_rt: Optional[ChannelRtVariableEnum]
    terrain_rt: Optional[TerrainRtVariableEnum]
    reservoir: Optional[ReservoirVariableEnum]


class ShortAndAnalysisNoDA(BaseModel):
    output_type: ShortAndAnalysisOutputEnum
    channel_rt: Optional[ChannelRtNoDAVariableEnum]
    terrain_rt: Optional[TerrainRtVariableEnum]
    reservoir: Optional[ReservoirVariableEnum]


class MediumRangeMem1(BaseModel):
    output_type: Medium1OutputEnum
    channel_rt_1: Optional[ChannelRtVariableEnum]
    terrain_rt_1: Optional[TerrainRtVariableEnum]
    reservoir_1: Optional[ReservoirVariableEnum]


class MediumRangeMem2(BaseModel):
    output_type: Medium2OutputEnum
    channel_rt_2: Optional[ChannelRtVariableEnum]
    terrain_rt_2: Optional[TerrainRtVariableEnum]
    reservoir_2: Optional[ReservoirVariableEnum]


class MediumRangeMem3(BaseModel):
    output_type: Medium3OutputEnum
    channel_rt_3: Optional[ChannelRtVariableEnum]
    terrain_rt_3: Optional[TerrainRtVariableEnum]
    reservoir_3: Optional[ReservoirVariableEnum]


class MediumRangeMem4(BaseModel):
    output_type: Medium4OutputEnum
    channel_rt_4: Optional[ChannelRtVariableEnum]
    terrain_rt_4: Optional[TerrainRtVariableEnum]
    reservoir_4: Optional[ReservoirVariableEnum]


class MediumRangeMem5(BaseModel):
    output_type: Medium5OutputEnum
    channel_rt_5: Optional[ChannelRtVariableEnum]
    terrain_rt_5: Optional[TerrainRtVariableEnum]
    reservoir_5: Optional[ReservoirVariableEnum]


class MediumRangeMem6(BaseModel):
    output_type: Medium6OutputEnum
    channel_rt_6: Optional[ChannelRtVariableEnum]
    terrain_rt_6: Optional[TerrainRtVariableEnum]
    reservoir_6: Optional[ReservoirVariableEnum]


class MediumRangeMem7(BaseModel):
    output_type: Medium7OutputEnum
    channel_rt_7: Optional[ChannelRtVariableEnum]
    terrain_rt_7: Optional[TerrainRtVariableEnum]
    reservoir_7: Optional[ReservoirVariableEnum]


class MediumRangeNoDA(BaseModel):
    output_type = MediumNoDAEnum
    channel_rt: Optional[ChannelRtNoDAVariableEnum]


class LongRangeMem1(BaseModel):
    output_type: Long1OutputEnum
    channel_rt_1: Optional[ChannelRtLongVariableEnum]
    reservoir_1: Optional[ReservoirVariableEnum]


class LongRangeMem2(BaseModel):
    output_type: Long2OutputEnum
    channel_rt_2: Optional[ChannelRtLongVariableEnum]
    reservoir_2: Optional[ReservoirVariableEnum]


class LongRangeMem3(BaseModel):
    output_type: Long3OutputEnum
    channel_rt_3: Optional[ChannelRtLongVariableEnum]
    reservoir_3: Optional[ReservoirVariableEnum]


class LongRangeMem4(BaseModel):
    output_type: Long4OutputEnum
    channel_rt_4: Optional[ChannelRtLongVariableEnum]
    reservoir_4: Optional[ReservoirVariableEnum]


# POINT CONFIGURATION ENUM: Potential configuration names
class ConfigurationsEnum(str, Enum):
    analysis_assim = "analysis_assim"
    analysis_assim_no_da = "analysis_assim_no_da"
    analysis_assim_extend = "analysis_assim_extend"
    analysis_assim_extend_no_da = "analysis_assim_extend_no_da"
    analysis_assim_long = "analysis_assim_long"
    analysis_assim_long_no_da = "analysis_assim_long_no_da"
    analysis_assim_hawaii = "analysis_assim_hawaii"
    analysis_assim_hawaii_no_da = "analysis_assim_hawaii_no_da"
    analysis_assim_puertorico = "analysis_assim_puertorico"
    analysis_assim_puertorico_no_da = "analysis_assim_puertorico_no_da"
    analysis_assim_alaska = "analysis_assim_alaska"
    analysis_assim_alaska_no_da = "analysis_assim_alaska_no_da"
    analysis_assim_extend_alaska = "analysis_assim_extend_alaska"
    analysis_assim_extend_alaska_no_da = "analysis_assim_extend_alaska_no_da"
    short_range = "short_range"
    short_range_hawaii = "short_range_hawaii"
    short_range_puertorico = "short_range_puertorico"
    short_range_hawaii_no_da = "short_range_hawaii_no_da"
    short_range_puertorico_no_da = "short_range_puertorico_no_da"
    short_range_alaska = "short_range_alaska"
    medium_range_mem1 = "medium_range_mem1"
    medium_range_mem2 = "medium_range_mem2"
    medium_range_mem3 = "medium_range_mem3"
    medium_range_mem4 = "medium_range_mem4"
    medium_range_mem5 = "medium_range_mem5"
    medium_range_mem6 = "medium_range_mem6"
    medium_range_mem7 = "medium_range_mem7"
    medium_range_no_da = "medium_range_no_da"
    medium_range_alaska_mem1 = "medium_range_alaska_mem1"
    medium_range_alaska_mem2 = "medium_range_alaska_mem2"
    medium_range_alaska_mem3 = "medium_range_alaska_mem3"
    medium_range_alaska_mem4 = "medium_range_alaska_mem4"
    medium_range_alaska_mem5 = "medium_range_alaska_mem5"
    medium_range_alaska_mem6 = "medium_range_alaska_mem6"
    medium_range_alaska_no_da = "medium_range_alaska_no_da"
    medium_range_blend = "medium_range_blend"
    medium_range_blend_alaska = "medium_range_blend_alaska"
    long_range_mem1 = "long_range_mem1"
    long_range_mem2 = "long_range_mem2"
    long_range_mem3 = "long_range_mem3"
    long_range_mem4 = "long_range_mem4"


# POINT CONFIGURATION MODEL
class PointConfigurationModel(BaseModel):
    configuration: ConfigurationsEnum
    analysis_assim: Optional[ShortAndAnalysis]
    analysis_assim_no_da: Optional[ShortAndAnalysisNoDA]
    analysis_assim_extend: Optional[ShortAndAnalysis]
    analysis_assim_extend_no_da: Optional[ShortAndAnalysisNoDA]
    analysis_assim_long: Optional[ShortAndAnalysis]
    analysis_assim_long_no_da: Optional[ShortAndAnalysisNoDA]
    analysis_assim_hawaii: Optional[ShortAndAnalysis]
    analysis_assim_hawaii_no_da: Optional[ShortAndAnalysisNoDA]
    analysis_assim_puertorico: Optional[ShortAndAnalysis]
    analysis_assim_puertorico_no_da: Optional[ShortAndAnalysisNoDA]
    analysis_assim_alaska: Optional[ShortAndAnalysis]
    analysis_assim_alaska_no_da: Optional[ShortAndAnalysisNoDA]
    analysis_assim_extend_alaska: Optional[ShortAndAnalysis]
    analysis_assim_extend_alaska_no_da: Optional[ShortAndAnalysisNoDA]
    short_range: Optional[ShortAndAnalysis]
    short_range_hawaii: Optional[ShortAndAnalysis]
    short_range_puertorico: Optional[ShortAndAnalysis]
    short_range_hawaii_no_da: Optional[ShortAndAnalysisNoDA]
    short_range_puertorico_no_da: Optional[ShortAndAnalysisNoDA]
    short_range_alaska: Optional[ShortAndAnalysis]
    medium_range_mem1: Optional[MediumRangeMem1]
    medium_range_mem2: Optional[MediumRangeMem2]
    medium_range_mem3: Optional[MediumRangeMem3]
    medium_range_mem4: Optional[MediumRangeMem4]
    medium_range_mem5: Optional[MediumRangeMem5]
    medium_range_mem6: Optional[MediumRangeMem6]
    medium_range_mem7: Optional[MediumRangeMem7]
    medium_range_no_da: Optional[MediumRangeNoDA]
    medium_range_alaska_mem1: Optional[MediumRangeMem1]
    medium_range_alaska_mem2: Optional[MediumRangeMem2]
    medium_range_alaska_mem3: Optional[MediumRangeMem3]
    medium_range_alaska_mem4: Optional[MediumRangeMem4]
    medium_range_alaska_mem5: Optional[MediumRangeMem5]
    medium_range_alaska_mem6: Optional[MediumRangeMem6]
    medium_range_alaska_no_da: Optional[ShortAndAnalysisNoDA]
    medium_range_blend: Optional[ShortAndAnalysis]
    medium_range_blend_alaska: Optional[ShortAndAnalysis]
    long_range_mem1: Optional[LongRangeMem1]
    long_range_mem2: Optional[LongRangeMem2]
    long_range_mem3: Optional[LongRangeMem3]
    long_range_mem4: Optional[LongRangeMem4]


if __name__ == "__main__":
    # So for example:
    configuration = "short_range_hawaii"
    output_type = "channel_rt"
    variable_name = "streamflow"

    # Assemble input parameters
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }

    # Check input parameters
    cm = PointConfigurationModel.parse_obj(vars)

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    var_name = getattr(forecast_obj, out_type).name

    pass
