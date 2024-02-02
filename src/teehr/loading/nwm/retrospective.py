"""Module for fetchning and processing retrospective NWM point data."""
import pandas as pd
import xarray as xr
import fsspec

# import numpy as np
from datetime import datetime, timedelta

from pathlib import Path
from typing import Union, Iterable, Optional

from teehr.loading.nwm.const import NWM22_UNIT_LOOKUP
from teehr.models.loading.utils import (
    ChunkByEnum,
    SupportedNWMRetroVersionsEnum
)
from teehr.loading.nwm.utils import write_parquet_file

NWM20_MIN_DATE = datetime(1993, 1, 1)
NWM20_MAX_DATE = datetime(2018, 12, 31, 23)
NWM21_MIN_DATE = pd.Timestamp(1979, 1, 1)
NWM21_MAX_DATE = pd.Timestamp(2020, 12, 31, 23)


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
    else:
        raise ValueError(f"unsupported NWM version {nwm_version}")


def da_to_df(
        nwm_version: SupportedNWMRetroVersionsEnum,
        da: xr.DataArray
) -> pd.DataFrame:
    """Format NWM retrospective data to TEEHR format."""
    df = da.to_dataframe()
    df.reset_index(inplace=True)
    df["measurement_unit"] = NWM22_UNIT_LOOKUP.get(da.units, da.units)
    df["variable_name"] = da.name
    df["configuration"] = f"{nwm_version}_retrospective"
    df["reference_time"] = df["time"]
    df.rename(
        columns={
            "time": "value_time",
            "feature_id": "location_id",
            da.name: "value",
        },
        inplace=True,
    )
    df.drop(columns=["latitude", "longitude"], inplace=True)

    df["location_id"] = f"{nwm_version}-" + df["location_id"].astype(str)
    df["location_id"] = df["location_id"].astype(str).astype("category")
    df["measurement_unit"] = df["measurement_unit"].astype("category")
    df["variable_name"] = df["variable_name"].astype("category")
    df["configuration"] = df["configuration"].astype("category")

    return df


def datetime_to_date(dt: datetime) -> datetime:
    """Convert datetime to date only."""
    dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


def nwm_retro_to_parquet(
    nwm_version: SupportedNWMRetroVersionsEnum,
    variable_name: str,
    location_ids: Iterable[int],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[ChunkByEnum, None] = None,
    overwrite_output: Optional[bool] = False,
):
    """Fetch NWM retrospective at NWM COMIDs and store as Parquet file.

    Parameters
    ----------
    nwm_version : SupportedNWMRetroVersionsEnum
        NWM retrospective version to fetch.
        Currently `nwm20` and `nwm21` supported.
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
        Last date to fetch.  Rounds up to end of day
        Str formats can include YYYY-MM-DD or MM/DD/YYYY.
    output_parquet_dir : Union[str, Path],
        Directory where output will be saved.
    chunk_by : Union[ChunkByEnum, None] = None,
        If None (default) saves all timeseries to a single file.
    overwrite_output : bool = False,
        Whether output should overwrite files if they exist.  Default is False.

    Examples
    --------
    Here we fetch and format retrospective NWM v2.0 streamflow data
    for two locations.

    Import the module.

    >>> import teehr.loading.nwm.retrospective as nwm_retro

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
    if nwm_version == SupportedNWMRetroVersionsEnum.nwm20:
        s3_zarr_url = 's3://noaa-nwm-retro-v2-zarr-pds'
    elif nwm_version == SupportedNWMRetroVersionsEnum.nwm21:
        s3_zarr_url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr/"
    else:
        raise ValueError(f"unsupported NWM version {nwm_version}")

    start_date = datetime_to_date(pd.Timestamp(start_date))
    end_date = (
        datetime_to_date(pd.Timestamp(end_date)) +
        timedelta(days=1) -
        timedelta(minutes=1)
    )
    # start_date = datetime_to_date(start_date)
    # end_date = datetime_to_date(end_date) + timedelta(days=1)

    validate_start_end_date(nwm_version, start_date, end_date)

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    ds = xr.open_zarr(
        fsspec.get_mapper(s3_zarr_url, anon=True), consolidated=True
    )

    # Fetch all at once
    if chunk_by is None:
        da = ds[variable_name].sel(
            feature_id=location_ids, time=slice(start_date, end_date)
        )
        df = da_to_df(nwm_version, da)
        output_filepath = Path(
            output_parquet_dir, f"{nwm_version}_retrospective.parquet"
        )
        write_parquet_file(output_filepath, overwrite_output, df)

    if chunk_by == "day":
        # Determine number of days to fetch
        period_length = timedelta(days=1)
        # start_date = datetime_to_date(start_date)
        # end_date = datetime_to_date(end_date) + timedelta(days=1)
        period = end_date - start_date
        if period < period_length:
            period = period_length
        number_of_days = period.days

        # Fetch data in daily batches
        for day in range(number_of_days):
            # Setup start and end date for fetch
            start_dt = start_date + period_length * day
            end_dt = (
                start_date + period_length * (day + 1) - timedelta(minutes=1)
            )
            da = ds[variable_name].sel(
                feature_id=location_ids, time=slice(start_dt, end_dt)
            )
            df = da_to_df(nwm_version, da)
            output_filepath = Path(
                output_parquet_dir, f"{start_dt.strftime('%Y-%m-%d')}.parquet"
            )
            write_parquet_file(output_filepath, overwrite_output, df)

    # fetch data by site
    if chunk_by == "location_id":

        for location_id in location_ids:
            da = ds[variable_name].sel(
                feature_id=location_id, time=slice(start_date, end_date)
            )
            df = da_to_df(nwm_version, da)
            output_filepath = Path(
                output_parquet_dir, f"{location_id}.parquet"
            )
            write_parquet_file(output_filepath, overwrite_output, df)


# if __name__ == "__main__":
#     # Examples

#     LOCATION_IDS = [7086109, 7040481]

#     nwm_retro_to_parquet(
#         nwm_version="nwm20",
#         variable_name="streamflow",
#         start_date="2000-01-01",
#         end_date="2000-01-02",
#         location_ids=LOCATION_IDS,
#         output_parquet_dir=Path(Path().home(), "temp", "nwm20_retrospective"),
#     )

#     nwm_retro_to_parquet(
#         nwm_version="nwm20",
#         variable_name="streamflow",
#         start_date=datetime(2000, 1, 1),
#         end_date=datetime(2000, 1, 2),
#         location_ids=LOCATION_IDS,
#         output_parquet_dir=Path(Path().home(), "temp", "nwm20_retrospective"),
#         chunk_by="day",
#     )

#     nwm_retro_to_parquet(
#         nwm_version="nwm20",
#         variable_name="streamflow",
#         start_date=datetime(2000, 1, 1),
#         end_date=datetime(2000, 1, 2),
#         location_ids=LOCATION_IDS,
#         output_parquet_dir=Path(Path().home(), "temp", "nwm20_retrospective"),
#         chunk_by="location_id",
#     )

#     nwm_retro_to_parquet(
#         nwm_version="nwm21",
#         variable_name="streamflow",
#         start_date="2000-01-01",
#         end_date="2000-01-02",
#         location_ids=LOCATION_IDS,
#         output_parquet_dir=Path(Path().home(), "temp", "nwm21_retrospective"),
#     )

#     nwm_retro_to_parquet(
#         nwm_version="nwm21",
#         variable_name="streamflow",
#         start_date=datetime(2000, 1, 1),
#         end_date=datetime(2000, 1, 2),
#         location_ids=LOCATION_IDS,
#         output_parquet_dir=Path(Path().home(), "temp", "nwm21_retrospective"),
#         chunk_by="day",
#     )

#     nwm_retro_to_parquet(
#         nwm_version="nwm21",
#         variable_name="streamflow",
#         start_date=datetime(2000, 1, 1),
#         end_date=datetime(2000, 1, 2),
#         location_ids=LOCATION_IDS,
#         output_parquet_dir=Path(Path().home(), "temp", "nwm21_retrospective"),
#         chunk_by="location_id",
#     )
#     pass
