"""Module describing NWM v2.0 point data configuration variables."""
from teehr.models.str_enum import StrEnum

from typing import Optional

from pydantic import BaseModel


from teehr.models.fetching.nwm22_point import (
    ShortAndAnalysis,
    MediumRangeMem1,
    MediumRangeMem2,
    MediumRangeMem3,
    MediumRangeMem4,
    MediumRangeMem5,
    MediumRangeMem6,
    MediumRangeMem7,
    LongRangeMem1,
    LongRangeMem2,
    LongRangeMem3,
    LongRangeMem4
)


# POINT CONFIGURATION ENUM: Potential configuration names
class ConfigurationsEnum(StrEnum):
    """ConfigurationsEnum."""

    analysis_assim = "analysis_assim"
    analysis_assim_extend = "analysis_assim_extend"
    analysis_assim_long = "analysis_assim_long"
    analysis_assim_hawaii = "analysis_assim_hawaii"
    short_range = "short_range"
    short_range_hawaii = "short_range_hawaii"
    medium_range_mem1 = "medium_range_mem1"
    medium_range_mem2 = "medium_range_mem2"
    medium_range_mem3 = "medium_range_mem3"
    medium_range_mem4 = "medium_range_mem4"
    medium_range_mem5 = "medium_range_mem5"
    medium_range_mem6 = "medium_range_mem6"
    medium_range_mem7 = "medium_range_mem7"
    long_range_mem1 = "long_range_mem1"
    long_range_mem2 = "long_range_mem2"
    long_range_mem3 = "long_range_mem3"
    long_range_mem4 = "long_range_mem4"


# POINT CONFIGURATION MODEL
class PointConfigurationModel(BaseModel):
    """NWM v2.0 PointConfigurationModel."""

    configuration: ConfigurationsEnum = None
    analysis_assim: Optional[ShortAndAnalysis] = None
    analysis_assim_extend: Optional[ShortAndAnalysis] = None
    analysis_assim_long: Optional[ShortAndAnalysis] = None
    analysis_assim_hawaii: Optional[ShortAndAnalysis] = None
    short_range: Optional[ShortAndAnalysis] = None
    short_range_hawaii: Optional[ShortAndAnalysis] = None
    medium_range_mem1: Optional[MediumRangeMem1] = None
    medium_range_mem2: Optional[MediumRangeMem2] = None
    medium_range_mem3: Optional[MediumRangeMem3] = None
    medium_range_mem4: Optional[MediumRangeMem4] = None
    medium_range_mem5: Optional[MediumRangeMem5] = None
    medium_range_mem6: Optional[MediumRangeMem6] = None
    medium_range_mem7: Optional[MediumRangeMem7] = None
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
