from enum import Enum
from typing import Optional

from pydantic import BaseModel


# GRID DATA VARIABLES ENUMS
class VariableNamesEnum(str, Enum):
    """All possible variable names for grid-based data"""

    # Forcing
    U2D = "U2D"
    V2D = "V2D"
    T2D = "T2D"
    Q2D = "Q2D"
    LWDOWN = "LWDOWN"
    SWDOWN = "SWDOWN"
    RAINRATE = "RAINRATE"
    PSFC = "PSFC"
    # Land
    ACCECAN = "ACCECAN"
    ACCEDIR = "ACCEDIR"
    ACCET = "ACCET"
    ACCETRAN = "ACCETRAN"
    ACSNOM = "ACSNOM"
    CANWAT = "CANWAT"
    EDIR = "EDIR"
    FIRA = "FIRA"
    FSA = "FSA"
    FSNO = "FSNO"
    GRDFLX = "GRDFLX"
    HFX = "HFX"
    ISNOW = "ISNOW"
    LH = "LH"
    QRAIN = "QRAIN"
    QSNOW = "QSNOW"
    SFCRNOFF = "SFCRNOFF"
    SNEQV = "SNEQV"
    SNLIQ = "SNLIQ"
    SNOWH = "SNOWH"
    SNOWT_AVG = "SNOWT_AVG"
    SOIL_M = "SOIL_M"
    SOIL_T = "SOIL_T"
    SOILICE = "SOILICE"
    SOILSAT_TOP = "SOILSAT_TOP"
    SOILSAT = "SOILSAT"
    TRAD = "TRAD"
    UGDRNOFF = "UGDRNOFF"


class LandAssimVariablesEnum(str, Enum):
    ACCET = "ACCET"
    ACSNOM = "ACSNOM"
    EDIR = "EDIR"
    FSNO = "FSNO"
    ISNOW = "ISNOW"
    QRAIN = "QRAIN"
    QSNOW = "QSNOW"
    SNEQV = "SNEQV"
    SNLIQ = "SNLIQ"
    SNOWH = "SNOWH"
    SNOWT_AVG = "SNOWT_AVG"
    SOILICE = "SOILICE"
    SOILSAT_TOP = "SOILSAT_TOP"
    SOIL_M = "SOIL_M"
    SOIL_T = "SOIL_T"


class LandShortVariablesEnum(str, Enum):
    ACCET = "ACCET"
    SNOWT_AVG = "SNOWT_AVG"
    SOILSAT_TOP = "SOILSAT_TOP"
    FSNO = "FSNO"
    SNOWH = "SNOWH"
    SNEQV = "SNEQV"


class LandMediumVariablesEnum(str, Enum):
    FSA = "FSA"
    FIRA = "FIRA"
    GRDFLX = "GRDFLX"
    HFX = "HFX"
    LH = "LH"
    UGDRNOFF = "UGDRNOFF"
    ACCECAN = "ACCECAN"
    ACCEDIR = "ACCEDIR"
    ACCETRAN = "ACCETRAN"
    TRAD = "TRAD"
    SNLIQ = "SNLIQ"
    SOIL_T = "SOIL_T"
    SOIL_M = "SOIL_M"
    SNOWH = "SNOWH"
    SNEQV = "SNEQV"
    ISNOW = "ISNOW"
    FSNO = "FSNO"
    ACSNOM = "ACSNOM"
    ACCET = "ACCET"
    CANWAT = "CANWAT"
    SOILICE = "SOILICE"
    SOILSAT_TOP = "SOILSAT_TOP"
    SNOWT_AVG = "SNOWT_AVG"


class LandLongVariablesEnum(str, Enum):
    UGDRNOFF = "UGDRNOFF"
    SFCRNOFF = "SFCRNOFF"
    SNEQV = "SNEQV"
    ACSNOM = "ACSNOM"
    ACCET = "ACCET"
    CANWAT = "CANWAT"
    SOILSAT_TOP = "SOILSAT_TOP"
    SOILSAT = "SOILSAT"


class ForcingVariablesEnum(str, Enum):
    U2D = "U2D"
    V2D = "V2D"
    T2D = "T2D"
    Q2D = "Q2D"
    LWDOWN = "LWDOWN"
    SWDOWN = "SWDOWN"
    RAINRATE = "RAINRATE"
    PSFC = "PSFC"


# GRID DATA TYPES ENUM
class OutputTypesEnum(str, Enum):
    """All possible output types for grid-based data"""

    land = "land"
    land1 = "land1"
    land2 = "land2"
    land3 = "land3"
    land4 = "land4"
    land5 = "land5"
    land6 = "land6"
    land7 = "land7"
    forcing = "forcing"


# GRID OUTPUT TYPE MODELS
class Analysis(BaseModel):
    land: Optional[LandAssimVariablesEnum]


class ShortRange(BaseModel):
    land: Optional[LandShortVariablesEnum]


class MediumRange(BaseModel):
    land_1: Optional[LandMediumVariablesEnum]
    land_2: Optional[LandMediumVariablesEnum]
    land_3: Optional[LandMediumVariablesEnum]
    land_4: Optional[LandMediumVariablesEnum]
    land_5: Optional[LandMediumVariablesEnum]
    land_6: Optional[LandMediumVariablesEnum]
    land_7: Optional[LandMediumVariablesEnum]


class LongRange(BaseModel):
    land_1: Optional[LandLongVariablesEnum]
    land_2: Optional[LandLongVariablesEnum]
    land_3: Optional[LandLongVariablesEnum]
    land_4: Optional[LandLongVariablesEnum]


class Forcing(BaseModel):
    forcing: Optional[ForcingVariablesEnum]


# GRID CONFIGURATIONS ENUM
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
    forcing_short_range = "forcing_short_range"
    forcing_short_range_hawaii = "forcing_short_range_hawaii"
    forcing_short_range_puertorico = "forcing_short_range_puertorico"
    forcing_analysis_assim = "forcing_analysis_assim"
    forcing_analysis_assim_extend = "forcing_analysis_assim_extend"
    forcing_analysis_assim_hawaii = "forcing_analysis_assim_hawaii"
    forcing_analysis_assim_puertorico = "forcing_analysis_assim_puertorico"


# GRID CONFIGURATION MODEL
class GridConfigurationModel(BaseModel):
    configuration: ConfigurationsEnum  # configuration is OK?
    output_type: OutputTypesEnum  # output_type is OK?
    variable_name: VariableNamesEnum  # variable_name is OK?
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
    forcing_short_range: Optional[Forcing]
    forcing_short_range_hawaii: Optional[Forcing]
    forcing_short_range_puertorico: Optional[Forcing]
    forcing_analysis_assim: Optional[Forcing]
    forcing_analysis_assim_extend: Optional[Forcing]
    forcing_analysis_assim_hawaii: Optional[Forcing]
    forcing_analysis_assim_puertorico: Optional[Forcing]


if __name__ == "__main__":
    # So for example:
    configuration = "short_range"  # Values in ConfigurationsEnum
    output_type = "forcing"  # Values in OutputTypesEnum
    variable_name = "RAINRATE"  # Values in VariableNamesEnum

    # Assemble input parameters
    vars = {
        "configuration": configuration,
        "output_type": output_type,
        "variable_name": variable_name,
        configuration: {
            output_type: variable_name,
        },
    }

    # import sys

    # Check input parameters
    # try:
    cm = GridConfigurationModel.model_validate(vars)
    # except ValidationError as e:
    #     print(e.errors()[0]["msg"])
    #     sys.exit()

    cm.configuration.name
    cm.output_type.name
    cm.variable_name.name

    pass
