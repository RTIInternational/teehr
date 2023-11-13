from enum import Enum
from typing import Optional

from pydantic import BaseModel

from teehr.models.loading.nwm22_grid import (
    Analysis,
    ShortRange,
    MediumRange,
    LongRange,
    Forcing,
)


# CONFIGURATIONS ENUM: All possible configuration
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
    long_range_mem1 = "long_range_mem1"
    long_range_mem2 = "long_range_mem2"
    long_range_mem3 = "long_range_mem3"
    long_range_mem4 = "long_range_mem4"
    forcing_medium_range = "forcing_medium_range"
    forcing_medium_range_blend = "forcing_medium_range_blend"
    forcing_medium_range_alaska = "forcing_medium_range_alaska"
    forcing_medium_range_blend_alaska = "forcing_medium_range_blend_alaska"
    forcing_short_range = "forcing_short_range"
    forcing_short_range_hawaii = "forcing_short_range_hawaii"
    forcing_short_range_puertorico = "forcing_short_range_puertorico"
    forcing_short_range_alaska = "forcing_short_range_alaska"
    forcing_analysis_assim = "forcing_analysis_assim"
    forcing_analysis_assim_extend = "forcing_analysis_assim_extend"
    forcing_analysis_assim_hawaii = "forcing_analysis_assim_hawaii"
    forcing_analysis_assim_puertorico = "forcing_analysis_assim_puertorico"
    forcing_analysis_assim_alaska = "forcing_analysis_assim_alaska"


# CONFIGURATION MODEL
class GridConfigurationModel(BaseModel):
    configuration: ConfigurationsEnum
    analysis_assim: Optional[Analysis]
    analysis_assim_no_da: Optional[Analysis]
    analysis_assim_extend: Optional[Analysis]
    analysis_assim_extend_no_da: Optional[Analysis]
    analysis_assim_long: Optional[Analysis]
    analysis_assim_long_no_da: Optional[Analysis]
    analysis_assim_hawaii: Optional[Analysis]
    analysis_assim_hawaii_no_da: Optional[Analysis]
    analysis_assim_puertorico: Optional[Analysis]
    analysis_assim_puertorico_no_da: Optional[Analysis]
    short_range: Optional[ShortRange]
    short_range_hawaii: Optional[ShortRange]
    short_range_puertorico: Optional[ShortRange]
    short_range_hawaii_no_da: Optional[ShortRange]
    short_range_puertorico_no_da: Optional[ShortRange]
    medium_range_mem1: Optional[MediumRange]
    medium_range_mem2: Optional[MediumRange]
    medium_range_mem3: Optional[MediumRange]
    medium_range_mem4: Optional[MediumRange]
    medium_range_mem5: Optional[MediumRange]
    medium_range_mem6: Optional[MediumRange]
    medium_range_mem7: Optional[MediumRange]
    medium_range_no_da: Optional[MediumRange]
    long_range_mem1: Optional[LongRange]
    long_range_mem2: Optional[LongRange]
    long_range_mem3: Optional[LongRange]
    long_range_mem4: Optional[LongRange]
    forcing_medium_range: Optional[Forcing]
    forcing_medium_range_blend: Optional[Forcing]
    forcing_medium_range_alaska: Optional[Forcing]
    forcing_medium_range_blend_alaska: Optional[Forcing]
    forcing_short_range: Optional[Forcing]
    forcing_short_range_hawaii: Optional[Forcing]
    forcing_short_range_puertorico: Optional[Forcing]
    forcing_short_range_alaska: Optional[Forcing]
    forcing_analysis_assim: Optional[Forcing]
    forcing_analysis_assim_extend: Optional[Forcing]
    forcing_analysis_assim_hawaii: Optional[Forcing]
    forcing_analysis_assim_puertorico: Optional[Forcing]
    forcing_analysis_assim_alaska: Optional[Forcing]


if __name__ == "__main__":
    # So for example:
    configuration = "forcing_medium_range_blend_alaska"
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

    # import sys
    # Check input parameters
    # try:
    cm = GridConfigurationModel.parse_obj(vars)
    # except ValidationError as e:
    #     print(e.errors()[0]["msg"])
    #     sys.exit()

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    # out_type = getattr(cm, config).output_type.name
    var_name = getattr(forecast_obj, out_type).name

    pass
