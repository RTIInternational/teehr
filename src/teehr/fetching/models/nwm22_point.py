"""Module describing NWM v2.2 point data configuration variables."""
from teehr.models.str_enum import StrEnum

from typing import Optional

from pydantic import BaseModel


# VARIABLE ENUMS: Potential variable names for each output_type
class ChannelRtVariableEnum(StrEnum):
    """ChannelRtVariableEnum."""
    nudge = "nudge"
    qBtmVertRunoff = "qBtmVertRunoff"
    qBucket = "qBucket"
    qSfcLatRunoff = "qSfcLatRunoff"
    streamflow = "streamflow"
    velocity = "velocity"


class ChannelRtNoDAVariableEnum(StrEnum):
    """ChannelRtNoDAVariableEnum."""
    nudge = "nudge"
    qBucket = "qBucket"
    qSfcLatRunoff = "qSfcLatRunoff"
    streamflow = "streamflow"
    velocity = "velocity"


class ChannelRtLongVariableEnum(StrEnum):
    """ChannelRtLongVariableEnum."""
    nudge = "nudge"
    streamflow = "streamflow"
    velocity = "velocity"


class TerrainRtVariableEnum(StrEnum):
    """TerrainRtVariableEnum."""
    sfcheadsubrt = "sfcheadsubrt"
    zwattablrt = "zwattablrt"


class ReservoirVariableEnum(StrEnum):
    """ReservoirVariableEnum."""
    inflow = "inflow"
    outflow = "outflow"
    reservoir_assimiated_value = "reservoir_assimiated_value"
    water_sfc_elev = "water_sfc_elev"


# OUTPUT ENUMS: Potential output names for each configuration
class ShortAndAnalysisOutputEnum(StrEnum):
    """ShortAndAnalysisOutputEnum."""
    channel_rt = "channel_rt"
    terrain_rt = "terrain_rt"
    reservoir = "reservoir"


class Medium1OutputEnum(StrEnum):
    """Medium1OutputEnum."""
    channel_rt_1 = "channel_rt_1"
    terrain_rt_1 = "terrain_rt_1"
    reservoir_1 = "reservoir_1"


class Medium2OutputEnum(StrEnum):
    """Medium2OutputEnum."""
    channel_rt_2 = "channel_rt_2"
    terrain_rt_2 = "terrain_rt_2"
    reservoir_2 = "reservoir_2"


class Medium3OutputEnum(StrEnum):
    """Medium3OutputEnum."""
    channel_rt_3 = "channel_rt_3"
    terrain_rt_3 = "terrain_rt_3"
    reservoir_3 = "reservoir_3"


class Medium4OutputEnum(StrEnum):
    """Medium4OutputEnum."""
    channel_rt_4 = "channel_rt_4"
    terrain_rt_4 = "terrain_rt_4"
    reservoir_4 = "reservoir_4"


class Medium5OutputEnum(StrEnum):
    """Medium5OutputEnum."""
    channel_rt_5 = "channel_rt_5"
    terrain_rt_5 = "terrain_rt_5"
    reservoir_5 = "reservoir_5"


class Medium6OutputEnum(StrEnum):
    """Medium6OutputEnum."""
    channel_rt_6 = "channel_rt_6"
    terrain_rt_6 = "terrain_rt_6"
    reservoir_6 = "reservoir_6"


class Medium7OutputEnum(StrEnum):
    """Medium7OutputEnum."""
    channel_rt_7 = "channel_rt_7"
    terrain_rt_7 = "terrain_rt_7"
    reservoir_7 = "reservoir_7"


class MediumNoDAEnum(StrEnum):
    """MediumNoDAEnum."""
    channel_rt = "channel_rt"


class Long1OutputEnum(StrEnum):
    """Long1OutputEnum."""
    channel_rt_1 = "channel_rt_1"
    reservoir_1 = "reservoir_1"


class Long2OutputEnum(StrEnum):
    """Long2OutputEnum."""
    channel_rt_2 = "channel_rt_2"
    reservoir_2 = "reservoir_2"


class Long3OutputEnum(StrEnum):
    """Long3OutputEnum."""
    channel_rt_3 = "channel_rt_3"
    reservoir_3 = "reservoir_3"


class Long4OutputEnum(StrEnum):
    """Long4OutputEnum."""
    channel_rt_4 = "channel_rt_4"
    reservoir_4 = "reservoir_4"


# POINT OUTPUT TYPE MODELS (needed for each configuration enum)
class ShortAndAnalysis(BaseModel):
    """ShortAndAnalysis."""
    output_type: ShortAndAnalysisOutputEnum
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class ShortAndAnalysisNoDA(BaseModel):
    """ShortAndAnalysisNoDA."""
    output_type: ShortAndAnalysisOutputEnum
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


class MediumRangeMem1(BaseModel):
    """MediumRangeMem1."""
    output_type: Medium1OutputEnum
    channel_rt_1: Optional[ChannelRtVariableEnum] = None
    terrain_rt_1: Optional[TerrainRtVariableEnum] = None
    reservoir_1: Optional[ReservoirVariableEnum] = None


class MediumRangeMem2(BaseModel):
    """MediumRangeMem2."""
    output_type: Medium2OutputEnum
    channel_rt_2: Optional[ChannelRtVariableEnum] = None
    terrain_rt_2: Optional[TerrainRtVariableEnum] = None
    reservoir_2: Optional[ReservoirVariableEnum] = None


class MediumRangeMem3(BaseModel):
    """MediumRangeMem3."""
    output_type: Medium3OutputEnum
    channel_rt_3: Optional[ChannelRtVariableEnum] = None
    terrain_rt_3: Optional[TerrainRtVariableEnum] = None
    reservoir_3: Optional[ReservoirVariableEnum] = None


class MediumRangeMem4(BaseModel):
    """MediumRangeMem4."""
    output_type: Medium4OutputEnum
    channel_rt_4: Optional[ChannelRtVariableEnum] = None
    terrain_rt_4: Optional[TerrainRtVariableEnum] = None
    reservoir_4: Optional[ReservoirVariableEnum] = None


class MediumRangeMem5(BaseModel):
    """MediumRangeMem5."""
    output_type: Medium5OutputEnum
    channel_rt_5: Optional[ChannelRtVariableEnum] = None
    terrain_rt_5: Optional[TerrainRtVariableEnum] = None
    reservoir_5: Optional[ReservoirVariableEnum] = None


class MediumRangeMem6(BaseModel):
    """MediumRangeMem6."""
    output_type: Medium6OutputEnum
    channel_rt_6: Optional[ChannelRtVariableEnum] = None
    terrain_rt_6: Optional[TerrainRtVariableEnum] = None
    reservoir_6: Optional[ReservoirVariableEnum] = None


class MediumRangeMem7(BaseModel):
    """MediumRangeMem7."""
    output_type: Medium7OutputEnum
    channel_rt_7: Optional[ChannelRtVariableEnum] = None
    terrain_rt_7: Optional[TerrainRtVariableEnum] = None
    reservoir_7: Optional[ReservoirVariableEnum] = None


class MediumRangeNoDA(BaseModel):
    """MediumRangeNoDA."""
    output_type: MediumNoDAEnum
    channel_rt: Optional[ChannelRtNoDAVariableEnum] = None


class LongRangeMem1(BaseModel):
    """LongRangeMem1."""
    output_type: Long1OutputEnum
    channel_rt_1: Optional[ChannelRtLongVariableEnum] = None
    reservoir_1: Optional[ReservoirVariableEnum] = None


class LongRangeMem2(BaseModel):
    """LongRangeMem2."""
    output_type: Long2OutputEnum
    channel_rt_2: Optional[ChannelRtLongVariableEnum] = None
    reservoir_2: Optional[ReservoirVariableEnum] = None


class LongRangeMem3(BaseModel):
    """LongRangeMem3."""
    output_type: Long3OutputEnum
    channel_rt_3: Optional[ChannelRtLongVariableEnum] = None
    reservoir_3: Optional[ReservoirVariableEnum] = None


class LongRangeMem4(BaseModel):
    """LongRangeMem4."""
    output_type: Long4OutputEnum
    channel_rt_4: Optional[ChannelRtLongVariableEnum] = None
    reservoir_4: Optional[ReservoirVariableEnum] = None


# POINT CONFIGURATION ENUM: Potential configuration names
class ConfigurationsEnum(StrEnum):
    """ConfigurationsEnum."""
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
    """NWM v2.2 PointConfigurationModel."""
    configuration: ConfigurationsEnum = None
    analysis_assim: Optional[ShortAndAnalysis] = None
    analysis_assim_no_da: Optional[ShortAndAnalysisNoDA] = None
    analysis_assim_extend: Optional[ShortAndAnalysis] = None
    analysis_assim_extend_no_da: Optional[ShortAndAnalysisNoDA] = None
    analysis_assim_long: Optional[ShortAndAnalysis] = None
    analysis_assim_long_no_da: Optional[ShortAndAnalysisNoDA] = None
    analysis_assim_hawaii: Optional[ShortAndAnalysis] = None
    analysis_assim_hawaii_no_da: Optional[ShortAndAnalysisNoDA] = None
    analysis_assim_puertorico: Optional[ShortAndAnalysis] = None
    analysis_assim_puertorico_no_da: Optional[ShortAndAnalysisNoDA] = None
    short_range: Optional[ShortAndAnalysis] = None
    short_range_hawaii: Optional[ShortAndAnalysis] = None
    short_range_puertorico: Optional[ShortAndAnalysis] = None
    short_range_hawaii_no_da: Optional[ShortAndAnalysisNoDA] = None
    short_range_puertorico_no_da: Optional[ShortAndAnalysisNoDA] = None
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
    configuration = "medium_range_mem1"
    output_type = "channel_rt_1"
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
    cm = PointConfigurationModel.model_validate(vars)

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    var_name = getattr(forecast_obj, out_type).name

    pass
