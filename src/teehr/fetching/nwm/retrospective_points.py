"""A module for fetching retrospective NWM point data (i.e., streamflow).

The function ``nwm_retro_to_parquet()`` can be used to fetch and format three
different versions of retrospective NWM data (v2.0, v2.1, and v3.0) for
specified variables, locations, and date ranges.

Each version of the NWM data is hosted on `AWS S3
<https://registry.opendata.aws/nwm-archive/>`__ and is stored in Zarr format.
Several options are included for fetching the data in chunks, including by
week, month, or year, which can be specified using the ``chunk_by`` argument.

Care must be taken when choosing a ``chunk_by`` value to minimize the amount
of data transferred over the network.

The Zarr stores for each version of the NWM data have the same internal
chunking scheme: {"time": 672, "feature_id": 30000}, which defines the minimum
amount of data that can be accessed at once (a single chunk). This means that
if you specify ``chunk_by='week'``, the entire chunk
(672 hours x 30000 locations) will be fetched for each week (128 hours)
falling within that chunk, resulting in redundant data transfer.

While it is currently not possible to avoid all redundant data transfer,
it can be minimized by selecting the largest chunk_by size that can fit into
your computer's memory.

.. note::
   It is recommended to set the ``chunk_by`` parameter to the largest time
   period ('week', 'month', or 'year') that will fit into your systems memory
   given the number of locations being fetched.
"""
import pandas as pd
import xarray as xr
import fsspec
from pydantic import validate_call
import numpy as np

from datetime import datetime

from pathlib import Path
from typing import Union, List, Optional, Dict
import logging

from teehr.fetching.const import (
    VALUE,
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME
)
from teehr.models.fetching.utils import (
    NWMChunkByEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum,
    ChannelRtRetroVariableEnum
)
from teehr.fetching.utils import (
    write_parquet_file,
    get_period_start_end_times,
    create_periods_based_on_chunksize,
    format_timeseries_data_types
)

NWM20_MIN_DATE = datetime(1993, 1, 1)
NWM20_MAX_DATE = datetime(2018, 12, 31, 23)
NWM21_MIN_DATE = pd.Timestamp(1979, 1, 1)
NWM21_MAX_DATE = pd.Timestamp(2020, 12, 31, 23)
NWM30_MIN_DATE = pd.Timestamp(1979, 2, 1, 1)
NWM30_MAX_DATE = pd.Timestamp(2023, 1, 31, 23)

logger = logging.getLogger(__name__)


def validate_start_end_date(
    nwm_version: SupportedNWMRetroVersionsEnum,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime]
):
    """Validate the start and end dates by NWM version."""
    if nwm_version == SupportedNWMRetroVersionsEnum.nwm20:
        if end_date <= start_date:
            raise ValueError("start_date must be before end_date")

        if start_date < NWM21_MIN_DATE:
            raise ValueError(
                f"start_date must be on or after {NWM20_MIN_DATE}"
            )

        if end_date > NWM21_MAX_DATE:
            raise ValueError(f"end_date must be on or before {NWM20_MAX_DATE}")

    elif nwm_version == SupportedNWMRetroVersionsEnum.nwm21:
        if end_date <= start_date:
            raise ValueError("start_date must be before end_date")

        if start_date < NWM21_MIN_DATE:
            raise ValueError(
                f"start_date must be on or after {NWM21_MIN_DATE}"
            )

        if end_date > NWM21_MAX_DATE:
            raise ValueError(f"end_date must be on or before {NWM21_MAX_DATE}")
    elif nwm_version == SupportedNWMRetroVersionsEnum.nwm30:
        if end_date <= start_date:
            raise ValueError("start_date must be before end_date")

        if start_date < NWM30_MIN_DATE:
            raise ValueError(
                f"start_date must be on or after {NWM30_MIN_DATE}"
            )

        if end_date > NWM30_MAX_DATE:
            raise ValueError(f"end_date must be on or before {NWM30_MAX_DATE}")
    else:
        raise ValueError(f"unsupported NWM version {nwm_version}")


def da_to_df(
        nwm_version: SupportedNWMRetroVersionsEnum,
        da: xr.DataArray,
        variable_mapper: Dict[str, Dict[str, str]]
) -> pd.DataFrame:
    """Format NWM retrospective data to TEEHR format."""
    logger.debug("Converting DataArray to a formatted DataFrame.")
    df = da.to_dataframe()
    df.reset_index(inplace=True)

    if not variable_mapper:
        df[UNIT_NAME] = da.units
        df[VARIABLE_NAME] = da.name.value
    else:
        # TODO: Make sure the mapping values are valid?
        df[UNIT_NAME] = variable_mapper[UNIT_NAME].get(da.units, da.units)
        df[VARIABLE_NAME] = variable_mapper[VARIABLE_NAME].get(
            da.name, da.name
        )
    df[CONFIGURATION_NAME] = f"{nwm_version}_retrospective"
    df[REFERENCE_TIME] = np.nan
    df.rename(
        columns={
            "time": VALUE_TIME,
            "feature_id": LOCATION_ID,
            da.name: VALUE,
        },
        inplace=True,
    )
    df.drop(columns=["latitude", "longitude"], inplace=True)

    df[LOCATION_ID] = f"{nwm_version}-" + df[LOCATION_ID].astype(str)

    if (nwm_version == "nwm21") or (nwm_version == "nwm30"):
        df.drop(columns=["elevation", "gage_id", "order"], inplace=True)

    df = format_timeseries_data_types(df)

    return df


def datetime_to_date(dt: datetime) -> datetime:
    """Convert datetime to date only."""
    dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


def format_grouped_filename(ds_i: xr.Dataset) -> str:
    """Format the output filename based on min and max datetime."""
    min_year = ds_i.time.min().dt.year
    min_month = ds_i.time.min().dt.month
    min_day = ds_i.time.min().dt.day

    max_year = ds_i.time.max().dt.year
    max_month = ds_i.time.max().dt.month
    max_day = ds_i.time.max().dt.day

    min_time = f"{min_year.values}{min_month.values:02d}{min_day.values:02d}"
    max_time = f"{max_year.values}{max_month.values:02d}{max_day.values:02d}"

    if min_time == max_time:
        return f"{min_time}.parquet"
    else:
        return f"{min_time}_{max_time}.parquet"


@validate_call(config=dict(arbitrary_types_allowed=True))
def nwm_retro_to_parquet(
    nwm_version: SupportedNWMRetroVersionsEnum,
    variable_name: ChannelRtRetroVariableEnum,
    location_ids: List[int],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[NWMChunkByEnum, None] = None,
    overwrite_output: Optional[bool] = False,
    domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
    variable_mapper: Dict[str, Dict[str, str]] = None,
):
    """Fetch NWM retrospective at NWM COMIDs and store as Parquet file.

    All dates and times within the files and in the file names are in UTC.

    Parameters
    ----------
    nwm_version : SupportedNWMRetroVersionsEnum
        NWM retrospective version to fetch.
        Currently `nwm20`, `nwm21`, and `nwm30` supported.
    variable_name : str
        Name of the NWM data variable to download.
        (e.g., "streamflow", "velocity", ...).
    location_ids : Iterable[int],
        NWM feature_ids to fetch.
    start_date : Union[str, datetime, pd.Timestamp]
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
        Rounds down to beginning of day.
    end_date : Union[str, datetime, pd.Timestamp],
        Last date to fetch.  Rounds up to end of day.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY.
    output_parquet_dir : Union[str, Path],
        Directory where output will be saved.
    chunk_by : Union[NWMChunkByEnum, None] = None,
        If None (default) saves all timeseries to a single file, otherwise
        the data is processed using the specified parameter.
        Can be: 'week', 'month', or 'year'.
    overwrite_output : bool = False,
        Whether output should overwrite files if they exist.  Default is False.
    domain : str = "CONUS"
        Geographical domain when NWM version is v3.0.
        Acceptable values are "Alaska", "CONUS" (default), "Hawaii", and "PR".
        Only used when NWM version equals `nwm30`.
    variable_mapper : Dict[str, Dict[str, str]]
        Dictionary mapping NWM variable and unit names to TEEHR variable
        and unit names.


    Examples
    --------
    Here we fetch and format retrospective NWM v2.0 streamflow data
    for two locations.

    Import the module.

    >>> import teehr.fetching.nwm.retrospective_points as nwm_retro

    Specify the input variables.

    >>> NWM_VERSION = "nwm20"
    >>> VARIABLE_NAME = "streamflow"
    >>> START_DATE = datetime(2000, 1, 1)
    >>> END_DATE = datetime(2000, 1, 2, 23)
    >>> LOCATION_IDS = [7086109, 7040481]
    >>> OUTPUT_ROOT = Path(Path().home(), "temp")
    >>> OUTPUT_DIR = Path(OUTPUT_ROOT, "nwm20_retrospective")

    Fetch and format the data, writing to the specified directory.

    >>> nwm_retro.nwm_retro_to_parquet(
    >>>     nwm_version=NWM_VERSION,
    >>>     variable_name=VARIABLE_NAME,
    >>>     start_date=START_DATE,
    >>>     end_date=END_DATE,
    >>>     location_ids=LOCATION_IDS,
    >>>     output_parquet_dir=OUTPUT_DIR
    >>> )
    """
    logger.info(
        f"Fetching NWM retrospective point data, version: {nwm_version}."
    )

    if nwm_version == SupportedNWMRetroVersionsEnum.nwm20:
        s3_zarr_url = "s3://noaa-nwm-retro-v2-zarr-pds"
    elif nwm_version == SupportedNWMRetroVersionsEnum.nwm21:
        s3_zarr_url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr/"
    elif nwm_version == SupportedNWMRetroVersionsEnum.nwm30:
        s3_zarr_url = (
            f"s3://noaa-nwm-retrospective-3-0-pds/{domain}/zarr/chrtout.zarr"
        )
    else:
        raise ValueError(f"unsupported NWM version {nwm_version}")

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    validate_start_end_date(nwm_version, start_date, end_date)

    # Include the entirety of the specified end day
    end_date = end_date.to_period(freq="D").end_time

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    da = xr.open_zarr(
        fsspec.get_mapper(s3_zarr_url, anon=True), consolidated=True
    )[variable_name].sel(
        feature_id=location_ids, time=slice(start_date, end_date)
    )

    # Chunk data by time
    logger.debug("Chunking data by time.")
    periods = create_periods_based_on_chunksize(
        start_date=start_date,
        end_date=end_date,
        chunk_by=chunk_by
    )

    for period in periods:

        if period is not None:
            dts = get_period_start_end_times(
                period=period,
                start_date=start_date,
                end_date=end_date
            )
        else:
            dts = {"start_dt": start_date, "end_dt": end_date}

        da_i = da.sel(time=slice(dts["start_dt"], dts["end_dt"]))

        logger.debug(
            f"Fetching point data for {dts['start_dt']} to {dts['end_dt']}."
        )

        df = da_to_df(nwm_version, da_i, variable_mapper)
        output_filename = format_grouped_filename(da_i)
        output_filepath = Path(
            output_parquet_dir, output_filename
        )
        write_parquet_file(output_filepath, overwrite_output, df)
