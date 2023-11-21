from enum import Enum
# from pydantic import BaseModel


class ChunkByEnum(str, Enum):
    day = "day"
    location_id = "location_id"


class SupportedNWMVersionsEnum(str, Enum):
    nwm20 = "nwm20"
    nwm21 = "nwm21"


# class NWM20RetroConfig(BaseModel):
#     URL = 's3://noaa-nwm-retro-v2-zarr-pds'
#     MIN_DATE = datetime(1993, 1, 1)
#     MAX_DATE = datetime(2018, 12, 31, 23)


# class NWM21RetroConfig(BaseModel):
#     URL = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr/"
#     MIN_DATE = pd.Timestamp(1979, 1, 1)
#     MAX_DATE = pd.Timestamp(2020, 12, 31, 23)