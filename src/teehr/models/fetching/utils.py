"""Module for NWM fetching models."""
from teehr.models.str_enum import StrEnum


# Timeseries models.
class TimeseriesTypeEnum(StrEnum):
    """Timeseries types enum."""

    primary = "primary"
    secondary = "secondary"


# Retrospective models.
class ChannelRtRetroVariableEnum(StrEnum):
    """ChannelRtRetro variables enum."""

    nudge = "nudge"
    qBtmVertRunoff = "qBtmVertRunoff"
    qBucket = "qBucket"
    qSfcLatRunoff = "qSfcLatRunoff"
    streamflow = "streamflow"
    velocity = "velocity"


class USGSServiceEnum(StrEnum):
    """USGSServiceEnum for fetching USGS data."""

    dv = "dv"
    iv = "iv"


class USGSChunkByEnum(StrEnum):
    """USGSChunkByEnum for fetching USGS data."""

    day = "day"
    location_id = "location_id"
    week = "week"
    month = "month"
    year = "year"


class NWMChunkByEnum(StrEnum):
    """NWMChunkByEnum for fetching NWM data."""

    week = "week"
    month = "month"
    year = "year"


class SupportedNWMRetroVersionsEnum(StrEnum):
    """SupportedNWMRetroVersionsEnum."""

    nwm20 = "nwm20"
    nwm21 = "nwm21"
    nwm30 = "nwm30"


class SupportedNWMOperationalVersionsEnum(StrEnum):
    """SupportedNWMOperationalVersionsEnum."""

    nwm22 = "nwm22"
    nwm30 = "nwm30"


class SupportedNWMDataSourcesEnum(StrEnum):
    """SupportedNWMDataSourcesEnum."""

    GCS = "GCS"
    NOMADS = "NOMADS"
    DSTOR = "DSTOR"


class SupportedKerchunkMethod(StrEnum):
    """SupportedKerchunkMethod."""

    local = "local"
    remote = "remote"
    auto = "auto"


class SupportedNWMRetroDomainsEnum(StrEnum):
    """SupportedNWMRetroDomainsEnum."""

    CONUS = "CONUS"
    Alaska = "Alaska"
    PR = "PR"
    Hawaii = "Hawaii"

# class NWM20RetroConfig(BaseModel):
#     URL = 's3://noaa-nwm-retro-v2-zarr-pds'
#     MIN_DATE = datetime(1993, 1, 1)
#     MAX_DATE = datetime(2018, 12, 31, 23)


# class NWM21RetroConfig(BaseModel):
#     URL = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr/"
#     MIN_DATE = pd.Timestamp(1979, 1, 1)
#     MAX_DATE = pd.Timestamp(2020, 12, 31, 23)
