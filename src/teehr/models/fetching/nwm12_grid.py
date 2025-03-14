"""Module describing NWM v2.0 grid configuration variables."""
from teehr.models.str_enum import StrEnum

from typing import Optional

from pydantic import BaseModel

from teehr.models.fetching.nwm22_grid import (
    Analysis,
    ShortRange,
    LongRange,
    Forcing,
    LandMediumVariablesEnum
)


class MediumOutputEnum(StrEnum):
    """MediumOutputEnum."""

    land = "land"


class MediumRange(BaseModel):
    """MediumRange."""

    output_type: MediumOutputEnum
    land: Optional[LandMediumVariablesEnum] = None


# CONFIGURATIONS ENUM
class ConfigurationsEnum(StrEnum):
    """ConfigurationsEnum."""

    analysis_assim = "analysis_assim"
    short_range = "short_range"
    medium_range = "medium_range"
    long_range_mem1 = "long_range_mem1"
    long_range_mem2 = "long_range_mem2"
    long_range_mem3 = "long_range_mem3"
    long_range_mem4 = "long_range_mem4"
    forcing_medium_range = "forcing_medium_range"
    forcing_short_range = "forcing_short_range"
    forcing_analysis_assim = "forcing_analysis_assim"


# CONFIGURATION MODEL
class GridConfigurationModel(BaseModel):
    """NWM v1.2 GridConfigurationModel."""

    configuration: ConfigurationsEnum
    analysis_assim: Optional[Analysis] = None
    short_range: Optional[ShortRange] = None
    medium_range: Optional[MediumRange] = None
    long_range_mem1: Optional[LongRange] = None
    long_range_mem2: Optional[LongRange] = None
    long_range_mem3: Optional[LongRange] = None
    long_range_mem4: Optional[LongRange] = None
    forcing_medium_range: Optional[Forcing] = None
    forcing_short_range: Optional[Forcing] = None
    forcing_analysis_assim: Optional[Forcing] = None


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
