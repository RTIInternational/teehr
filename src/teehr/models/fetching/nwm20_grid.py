"""Module describing NWM v2.0 grid configuration variables."""
from teehr.models.str_enum import StrEnum

from typing import Optional

from pydantic import BaseModel

from teehr.models.fetching.nwm22_grid import (
    Analysis,
    ShortRange,
    MediumRange,
    LongRange,
    Forcing,
)


# CONFIGURATIONS ENUM
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
    forcing_medium_range = "forcing_medium_range"
    forcing_short_range = "forcing_short_range"
    forcing_short_range_hawaii = "forcing_short_range_hawaii"
    forcing_analysis_assim = "forcing_analysis_assim"
    forcing_analysis_assim_extend = "forcing_analysis_assim_extend"
    forcing_analysis_assim_hawaii = "forcing_analysis_assim_hawaii"


# CONFIGURATION MODEL
class GridConfigurationModel(BaseModel):
    """NWM v2.0 GridConfigurationModel."""

    configuration: ConfigurationsEnum
    analysis_assim: Optional[Analysis] = None
    analysis_assim_extend: Optional[Analysis] = None
    analysis_assim_long: Optional[Analysis] = None
    analysis_assim_hawaii: Optional[Analysis] = None
    short_range: Optional[ShortRange] = None
    short_range_hawaii: Optional[ShortRange] = None
    medium_range_mem1: Optional[MediumRange] = None
    medium_range_mem2: Optional[MediumRange] = None
    medium_range_mem3: Optional[MediumRange] = None
    medium_range_mem4: Optional[MediumRange] = None
    medium_range_mem5: Optional[MediumRange] = None
    medium_range_mem6: Optional[MediumRange] = None
    medium_range_mem7: Optional[MediumRange] = None
    long_range_mem1: Optional[LongRange] = None
    long_range_mem2: Optional[LongRange] = None
    long_range_mem3: Optional[LongRange] = None
    long_range_mem4: Optional[LongRange] = None
    forcing_medium_range: Optional[Forcing] = None
    forcing_short_range: Optional[Forcing] = None
    forcing_short_range_hawaii: Optional[Forcing] = None
    forcing_analysis_assim: Optional[Forcing] = None
    forcing_analysis_assim_extend: Optional[Forcing] = None
    forcing_analysis_assim_hawaii: Optional[Forcing] = None


if __name__ == "__main__":
    # So for example:
    configuration = "forcing_medium_range"
    output_type = "forcing"
    variable_name = "RAINRATE"

    # Assemble input parameters
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }
    cm = GridConfigurationModel.model_validate(vars)

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    var_name = getattr(forecast_obj, out_type).name
