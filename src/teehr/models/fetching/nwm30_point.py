"""Module describing NWM v3.0 point configuration variables."""
from teehr.models.str_enum import StrEnum

from typing import Optional

from pydantic import BaseModel

from teehr.models.fetching.nwm22_point import (
    ShortAndAnalysis,
    ShortAndAnalysisNoDA,
    MediumRangeMem1,
    MediumRangeMem2,
    MediumRangeMem3,
    MediumRangeMem4,
    MediumRangeMem5,
    MediumRangeMem6,
    MediumRangeMem7,
    MediumRangeNoDA,
    LongRangeMem1,
    LongRangeMem2,
    LongRangeMem3,
    LongRangeMem4
)


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
    """NWM v3.0 PointConfigurationModel."""
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
    analysis_assim_alaska: Optional[ShortAndAnalysis] = None
    analysis_assim_alaska_no_da: Optional[ShortAndAnalysisNoDA] = None
    analysis_assim_extend_alaska: Optional[ShortAndAnalysis] = None
    analysis_assim_extend_alaska_no_da: Optional[ShortAndAnalysisNoDA] = None
    short_range: Optional[ShortAndAnalysis] = None
    short_range_hawaii: Optional[ShortAndAnalysis] = None
    short_range_puertorico: Optional[ShortAndAnalysis] = None
    short_range_hawaii_no_da: Optional[ShortAndAnalysisNoDA] = None
    short_range_puertorico_no_da: Optional[ShortAndAnalysisNoDA] = None
    short_range_alaska: Optional[ShortAndAnalysis] = None
    medium_range_mem1: Optional[MediumRangeMem1] = None
    medium_range_mem2: Optional[MediumRangeMem2] = None
    medium_range_mem3: Optional[MediumRangeMem3] = None
    medium_range_mem4: Optional[MediumRangeMem4] = None
    medium_range_mem5: Optional[MediumRangeMem5] = None
    medium_range_mem6: Optional[MediumRangeMem6] = None
    medium_range_mem7: Optional[MediumRangeMem7] = None
    medium_range_no_da: Optional[MediumRangeNoDA] = None
    medium_range_alaska_mem1: Optional[MediumRangeMem1] = None
    medium_range_alaska_mem2: Optional[MediumRangeMem2] = None
    medium_range_alaska_mem3: Optional[MediumRangeMem3] = None
    medium_range_alaska_mem4: Optional[MediumRangeMem4] = None
    medium_range_alaska_mem5: Optional[MediumRangeMem5] = None
    medium_range_alaska_mem6: Optional[MediumRangeMem6] = None
    medium_range_alaska_no_da: Optional[ShortAndAnalysisNoDA] = None
    medium_range_blend: Optional[ShortAndAnalysis] = None
    medium_range_blend_alaska: Optional[ShortAndAnalysis] = None
    long_range_mem1: Optional[LongRangeMem1] = None
    long_range_mem2: Optional[LongRangeMem2] = None
    long_range_mem3: Optional[LongRangeMem3] = None
    long_range_mem4: Optional[LongRangeMem4] = None


if __name__ == "__main__":
    # So for example:
    configuration = "short_range_alaska"
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
