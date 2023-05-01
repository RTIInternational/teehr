import pandas as pd
import xarray as xr
import fsspec
# import numpy as np
from datetime import datetime, timedelta

from pathlib import Path
from typing import Union, Iterable

from teehr.loading.const_nwm import (
    NWM22_UNIT_LOOKUP
)
from teehr.models.loading import (
    ChunkByEnum
)

# URL = 's3://noaa-nwm-retro-v2-zarr-pds'
# MIN_DATE = datetime(1993, 1, 1)
# MAX_DATE = datetime(2018, 12, 31, 23)

URL = 's3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr/'
MIN_DATE = pd.Timestamp(1979, 1, 1)
MAX_DATE = pd.Timestamp(2020, 12, 31, 23)


def _datetime_to_date(dt: datetime) -> datetime:
    """Convert datetime to date only"""
    dt.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )
    return dt


def _da_to_df(da: xr.DataArray) -> pd.DataFrame:
    """Format NWM retrospective data to TEEHR format."""

    df = da.to_dataframe()
    df.reset_index(inplace=True)
    df["measurement_unit"] = NWM22_UNIT_LOOKUP.get(da.units, da.units)
    df["variable_name"] = da.name
    df["configuration"] = "nwm22_retrospective"
    df["reference_time"] = df["time"]
    df.rename(
        columns={
            "time": "value_time",
            "feature_id": "location_id",
            da.name: "value"
        },
        inplace=True
    )
    df.drop(columns=["latitude", "longitude"], inplace=True)

    df["location_id"] = "nwm22-" + df["location_id"].astype(str)
    df["location_id"] = df["location_id"].astype(str).astype("category")
    df["measurement_unit"] = df["measurement_unit"].astype("category")
    df["variable_name"] = df["variable_name"].astype("category")
    df["configuration"] = df["configuration"].astype("category")

    return df


def validate_start_end_date(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
):
    if end_date <= start_date:
        raise ValueError("start_date must be before end_date")

    if start_date < MIN_DATE:
        raise ValueError(f"start_date must be on or after {MIN_DATE}")

    if end_date > MAX_DATE:
        raise ValueError(f"end_date must be on or before {MAX_DATE}")


def nwm_retro_to_parquet(
    variable_name: str,
    location_ids: Iterable[int],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[ChunkByEnum, None] = None,
):
    """Fetch NWM retrospective at NWM COMIDs and store as Parquet.

    Parameters
    ----------

    Returns
    -------

    """

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    validate_start_end_date(start_date, end_date)

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    ds = xr.open_zarr(fsspec.get_mapper(URL, anon=True), consolidated=True)

    # Fetch all at once
    if chunk_by is None:
        da = ds[variable_name].sel(
            feature_id=location_ids,
            time=slice(start_date, end_date)
        )
        df = _da_to_df(da)
        output_filepath = Path(
            output_parquet_dir,
            "nwm22_retrospective.parquet"
        )
        df.to_parquet(output_filepath)
        # output_filepath = Path(
        #     output_parquet_dir,
        #     "nwm22_retrospective.csv"
        # )
        # df.to_csv(output_filepath)
        # print(df)

    if chunk_by == "day":
        # Determine number of days to fetch
        period_length = timedelta(days=1)
        start_date = _datetime_to_date(start_date)
        end_date = _datetime_to_date(end_date)
        period = end_date - start_date
        if period < period_length:
            period = period_length
        number_of_days = period.days

        # Fetch data in daily batches
        for day in range(number_of_days):

            # Setup start and end date for fetch
            start_dt = (start_date + period_length * day)
            end_dt = (
                start_date
                + period_length * (day + 1)
                - timedelta(minutes=1)
            )
            da = ds[variable_name].sel(
                feature_id=location_ids,
                time=slice(start_dt, end_dt)
            )
            df = _da_to_df(da)
            output_filepath = Path(
                output_parquet_dir,
                f"{start_dt.strftime('%Y-%m-%d')}.parquet"
            )
            df.to_parquet(output_filepath)
            # output_filepath = Path(
            #     output_parquet_dir,
            #     f"{start_dt.strftime('%Y-%m-%d')}.csv"
            # )
            # df.to_csv(output_filepath)
            # print(df)

    # fetch data by site
    if chunk_by == "location_id":
        for location_id in location_ids:
            da = ds[variable_name].sel(
                feature_id=location_id,
                time=slice(start_date, end_date)
            )
            df = _da_to_df(da)
            output_filepath = Path(
                output_parquet_dir,
                f"{location_id}.parquet"
            )
            df.to_parquet(output_filepath)
            # output_filepath = Path(
            #     output_parquet_dir,
            #     f"{location_id}.csv"
            # )
            # df.to_csv(output_filepath)
            # print(df)


if __name__ == "__main__":

    # Examples

    LOCATION_IDS = [7086109, 7040481]

    nwm_retro_to_parquet(
        variable_name='streamflow',
        start_date="2000-01-01",
        end_date="2000-01-02 23:00",
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm22_retrospective")
    )

    nwm_retro_to_parquet(
        variable_name='streamflow',
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 3),
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm22_retrospective"),
        chunk_by="day",
    )

    nwm_retro_to_parquet(
        variable_name='streamflow',
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2, 23),
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm22_retrospective"),
        chunk_by="location_id",
    )
    pass
