"""Module describing NWM v2.0 point data configuration variables."""
from teehr.models.str_enum import StrEnum

from typing import Optional

from pydantic import BaseModel

from teehr.models.fetching.nwm22_point import (
    ShortAndAnalysis,
    LongRangeMem1,
    LongRangeMem2,
    LongRangeMem3,
    LongRangeMem4,
    ChannelRtVariableEnum,
    TerrainRtVariableEnum,
    ReservoirVariableEnum
)


class MediumOutputEnum(StrEnum):
    """MediumOutputEnum."""

    channel_rt = "channel_rt"
    terrain_rt = "terrain_rt"
    reservoir = "reservoir"


class MediumRange(BaseModel):
    """MediumRange."""

    output_type: MediumOutputEnum
    channel_rt: Optional[ChannelRtVariableEnum] = None
    terrain_rt: Optional[TerrainRtVariableEnum] = None
    reservoir: Optional[ReservoirVariableEnum] = None


# POINT CONFIGURATION ENUM: Potential configuration names
class ConfigurationsEnum(StrEnum):
    """ConfigurationsEnum."""

    analysis_assim = "analysis_assim"
    short_range = "short_range"
    medium_range = "medium_range"
    long_range_mem1 = "long_range_mem1"
    long_range_mem2 = "long_range_mem2"
    long_range_mem3 = "long_range_mem3"
    long_range_mem4 = "long_range_mem4"


# POINT CONFIGURATION MODEL
class PointConfigurationModel(BaseModel):
    """NWM v1.2 PointConfigurationModel."""

    configuration: ConfigurationsEnum = None
    analysis_assim: Optional[ShortAndAnalysis] = None
    short_range: Optional[ShortAndAnalysis] = None
    medium_range: Optional[MediumRange] = None
    long_range_mem1: Optional[LongRangeMem1] = None
    long_range_mem2: Optional[LongRangeMem2] = None
    long_range_mem3: Optional[LongRangeMem3] = None
    long_range_mem4: Optional[LongRangeMem4] = None


if __name__ == "__main__":
    # So for example:
    configuration = "medium_range"
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
    cm = PointConfigurationModel.model_validate(vars)

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    var_name = getattr(forecast_obj, out_type).name

    pass
