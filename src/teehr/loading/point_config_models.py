from enum import Enum
from typing import Optional

from pydantic import BaseModel


# POINT OUTPUT VARIABLES ENUMS
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


class VariableNamesEnum(str, Enum):
    """All possible variable names for point-based output"""

    nudge = "nudge"
    qBucket = "qBucket"
    qSfcLatRunoff = "qSfcLatRunoff"
    streamflow = "streamflow"
    velocity = "velocity"
    inflow = "inflow"
    outflow = "outflow"
    reservoir_assimiated_value = "reservoir_assimiated_value"
    water_sfc_elev = "water_sfc_elev"
    sfcheadsubrt = "sfcheadsubrt"
    zwattablrt = "zwattablrt"


# POINT OUTPUT TYPES ENUM
class OutputTypesEnum(str, Enum):
    """All possible output types for point-based output"""

    channel_rt = "channel_rt"
    terrain_rt = "terrain_rt"
    reservoir = "reservoir"
    channel_rt_1 = "channel_rt_1"
    terrain_rt_1 = "terrain_rt_1"
    reservoir_1 = "reservoir_1"
    channel_rt_2 = "channel_rt_2"
    terrain_rt_2 = "terrain_rt_2"
    reservoir_2 = "reservoir_2"
    channel_rt_3 = "channel_rt_3"
    terrain_rt_3 = "terrain_rt_3"
    reservoir_3 = "reservoir_3"
    channel_rt_4 = "channel_rt_4"
    terrain_rt_4 = "terrain_rt_4"
    reservoir_4 = "reservoir_4"
    channel_rt_5 = "channel_rt_5"
    terrain_rt_5 = "terrain_rt_5"
    reservoir_5 = "reservoir_5"
    channel_rt_6 = "channel_rt_6"
    terrain_rt_6 = "terrain_rt_6"
    reservoir_6 = "reservoir_6"
    channel_rt_7 = "channel_rt_7"
    terrain_rt_7 = "terrain_rt_7"
    reservoir_7 = "reservoir_7"


# POINT OUTPUT TYPE MODELS (needed for each configuration enum)
class Analysis(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisExtend(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisExtendNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisLong(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisLongNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisHawaii(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisHawaiiNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisPuertoRico(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class AnalysisPuertoRicoNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class ShortRange(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class ShortRangeHawaii(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class ShortRangePuertoRico(BaseModel):
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class ShortRangeHawaiiNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class ShortRangePuertoRicoNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class MediumRangeMem1(BaseModel):
    channel_rt_1: Optional[ChannelRtVariableEnum] = None
    terrain_rt_1: Optional[TerrainRtVariableEnum] = None
    reservoir_1: Optional[ReservoirVariableEnum] = None


class MediumRangeMem2(BaseModel):
    channel_rt_2: Optional[ChannelRtVariableEnum] = None
    terrain_rt_2: Optional[TerrainRtVariableEnum] = None
    reservoir_2: Optional[ReservoirVariableEnum] = None


class MediumRangeMem3(BaseModel):
    channel_rt_3: Optional[ChannelRtVariableEnum] = None
    terrain_rt_3: Optional[TerrainRtVariableEnum] = None
    reservoir_3: Optional[ReservoirVariableEnum] = None


class MediumRangeMem4(BaseModel):
    channel_rt_4: Optional[ChannelRtVariableEnum] = None
    terrain_rt_4: Optional[TerrainRtVariableEnum] = None
    reservoir_4: Optional[ReservoirVariableEnum] = None


class MediumRangeMem5(BaseModel):
    channel_rt_5: Optional[ChannelRtVariableEnum] = None
    terrain_rt_5: Optional[TerrainRtVariableEnum] = None
    reservoir_5: Optional[ReservoirVariableEnum] = None


class MediumRangeMem6(BaseModel):
    channel_rt_6: Optional[ChannelRtVariableEnum] = None
    terrain_rt_6: Optional[TerrainRtVariableEnum] = None
    reservoir_6: Optional[ReservoirVariableEnum] = None


class MediumRangeMem7(BaseModel):
    channel_rt_7: Optional[ChannelRtVariableEnum] = None
    terrain_rt_7: Optional[TerrainRtVariableEnum] = None
    reservoir_7: Optional[ReservoirVariableEnum] = None


class MediumRangeNoDA(BaseModel):
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None


class LongRangeMem1(BaseModel):
    channel_rt_1: Optional[ChannelRtLongVariableEnum] = None
    reservoir_1: Optional[ReservoirVariableEnum] = None


class LongRangeMem2(BaseModel):
    channel_rt_2: Optional[ChannelRtLongVariableEnum] = None
    reservoir_2: Optional[ReservoirVariableEnum] = None


class LongRangeMem3(BaseModel):
    channel_rt_3: Optional[ChannelRtLongVariableEnum] = None
    reservoir_3: Optional[ReservoirVariableEnum] = None


class LongRangeMem4(BaseModel):
    channel_rt_4: Optional[ChannelRtLongVariableEnum] = None
    reservoir_4: Optional[ReservoirVariableEnum] = None


# POINT CONFIGURATION ENUM
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
    short_range = "short_range"
    short_range_hawaii = "short_range_hawaii"
    short_range_puertorico = "short_range_puertorico"
    short_range_hawaii_no_da = "short_range_hawaii_no_da"
    short_range_puertorico_no_da = "short_range_puertorico_no_da"
    medium_range_mem1 = "medium_range_mem1"
    medium_range_mem2 = "medium_range_mem2"
    medium_range_mem3 = "medium_range_mem3"
    medium_range_mem4 = "medium_range_mem4"
    medium_range_mem5 = "medium_range_mem5"
    medium_range_mem6 = "medium_range_mem6"
    medium_range_mem7 = "medium_range_mem7"
    medium_range_no_da = "medium_range_no_da"
    long_range_mem1 = "long_range_mem1"
    long_range_mem2 = "long_range_mem2"
    long_range_mem3 = "long_range_mem3"
    long_range_mem4 = "long_range_mem4"


# POINT CONFIGURATION MODEL
class PointConfigurationModel(BaseModel):
    configuration: ConfigurationsEnum  # configuration is OK?
    output_type: OutputTypesEnum  # output_type is OK?
    variable_name: VariableNamesEnum  # variable_name is OK?
    analysis_assim: Optional[Analysis] = None
    analysis_assim_no_da: Optional[AnalysisNoDA] = None
    analysis_assim_extend: Optional[AnalysisExtend] = None
    analysis_assim_extend_no_da: Optional[AnalysisExtendNoDA] = None
    analysis_assim_long: Optional[AnalysisLong] = None
    analysis_assim_long_no_da: Optional[AnalysisLongNoDA] = None
    analysis_assim_hawaii: Optional[AnalysisHawaii] = None
    analysis_assim_hawaii_no_da: Optional[AnalysisHawaiiNoDA] = None
    analysis_assim_puertorico: Optional[AnalysisPuertoRico] = None
    analysis_assim_puertorico_no_da: Optional[AnalysisPuertoRicoNoDA] = None
    short_range: Optional[ShortRange] = None
    short_range_hawaii: Optional[ShortRangeHawaii] = None
    short_range_puertorico: Optional[ShortRangePuertoRico] = None
    short_range_hawaii_no_da: Optional[ShortRangeHawaiiNoDA] = None
    short_range_puertorico_no_da: Optional[ShortRangePuertoRicoNoDA] = None
    medium_range_mem1: Optional[MediumRangeMem1] = None
    medium_range_mem2: Optional[MediumRangeMem2] = None
    medium_range_mem3: Optional[MediumRangeMem3] = None
    medium_range_mem4: Optional[MediumRangeMem4] = None
    medium_range_mem5: Optional[MediumRangeMem5] = None
    medium_range_mem6: Optional[MediumRangeMem6] = None
    medium_range_mem7: Optional[MediumRangeMem7] = None
    medium_range_no_da: Optional[MediumRangeNoDA] = None
    long_range_mem1: Optional[LongRangeMem1] = None
    long_range_mem2: Optional[LongRangeMem2] = None
    long_range_mem3: Optional[LongRangeMem3] = None
    long_range_mem4: Optional[LongRangeMem4] = None


if __name__ == "__main__":
    # So for example:
    configuration = "short_range"  # Values in ConfigurationsEnum
    output_type = "channel_rt"  # Values in OutputTypesEnum
    variable_name = "streamflow"  # Values in VariableNamesEnum

    # Assemble input parameters
    vars = {
        "configuration": configuration,
        "output_type": output_type,
        "variable_name": variable_name,
        configuration: {
            output_type: variable_name,
        },
    }

    # Check input parameters
    cm = PointConfigurationModel.model_validate(vars)

    cm.configuration.name
    cm.output_type.name
    cm.variable_name.name

    pass
