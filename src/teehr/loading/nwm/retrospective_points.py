import pandas as pd
import xarray as xr
import fsspec
from pydantic import validate_call

from datetime import datetime, timedelta

from pathlib import Path
from typing import Union, List, Optional

from teehr.loading.nwm.const import NWM22_UNIT_LOOKUP
from teehr.models.loading.utils import (
    ChunkByEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum
)
from teehr.loading.nwm.utils import write_parquet_file

NWM20_MIN_DATE = datetime(1993, 1, 1)
NWM20_MAX_DATE = datetime(2018, 12, 31, 23)
NWM21_MIN_DATE = pd.Timestamp(1979, 1, 1)
NWM21_MAX_DATE = pd.Timestamp(2020, 12, 31, 23)
NWM30_MIN_DATE = pd.Timestamp(1979, 2, 1, 1)
NWM30_MAX_DATE = pd.Timestamp(2023, 1, 31, 23)


def validate_start_end_date(
    nwm_version: SupportedNWMRetroVersionsEnum,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime]
):
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

    if nwm_version == "nwm21":
        df.drop(columns=["elevation", "gage_id", "order"], inplace=True)

    return df


def datetime_to_date(dt: datetime) -> datetime:
    """Convert datetime to date only"""
    dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


def format_grouped_filename(ds_i: xr.Dataset) -> str:
    """Formats the output filename based on min and max
    datetime in the dataset."""
    min_year = ds_i.time.min().dt.year
    min_month = ds_i.time.min().dt.month
    min_day = ds_i.time.min().dt.day

    max_year = ds_i.time.max().dt.year
    max_month = ds_i.time.max().dt.month
    max_day = ds_i.time.max().dt.day

    min_time = f"{min_year.values}{min_month.values:02d}{min_day.values:02d}Z"
    max_time = f"{max_year.values}{max_month.values:02d}{max_day.values:02d}Z"

    if min_time == max_time:
        return f"{min_time}.parquet"
    else:
        return f"{min_time}_{max_time}.parquet"


@validate_call(config=dict(arbitrary_types_allowed=True))
def nwm_retro_to_parquet(
    nwm_version: SupportedNWMRetroVersionsEnum,
    variable_name: str,
    location_ids: List[int],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[ChunkByEnum, None] = None,
    overwrite_output: Optional[bool] = False,
    domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS"
):
    """Fetch NWM retrospective at NWM COMIDs and store as Parquet file.

    Parameters
    ----------
    nwm_version: SupportedNWMRetroVersionsEnum
        NWM retrospective version to fetch.
        Currently `nwm20`, `nwm21`, and `nwm30` supported
    variable_name: str
        Name of the NWM data variable to download.
        (e.g., "streamflow", "velocity", ...)
    location_ids: Iterable[int],
        NWM feature_ids to fetch
    start_date: Union[str, datetime, pd.Timestamp]
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
        Rounds down to beginning of day
    end_date: Union[str, datetime, pd.Timestamp],
        Last date to fetch.  Rounds up to end of day
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
    output_parquet_dir: Union[str, Path],
        Directory where output will be saved.
    chunk_by: Union[ChunkByEnum, None] = None,
        If None (default) saves all timeseries to a single file, otherwise
        the data is processed using the specified parameter.
        Can be: 'location_id', 'day', 'week', 'month', or 'year'
    overwrite_output: bool = False,
        Whether output should overwrite files if they exist.  Default is False.
    domain: str = "CONUS"
        Geographical domain when NWM version is v3.0.
        Acceptable values are "Alaska", "CONUS" (default), "Hawaii", and "PR".
        Only used when NWM version equals `nwm30`

    Returns
    -------
    None - saves file to specified path

    """

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

    start_date = datetime_to_date(pd.Timestamp(start_date))
    end_date = (
        datetime_to_date(pd.Timestamp(end_date)) +
        timedelta(days=1) -
        timedelta(minutes=1)
    )

    validate_start_end_date(nwm_version, start_date, end_date)

    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    ds = xr.open_zarr(
        fsspec.get_mapper(s3_zarr_url, anon=True), consolidated=True
    ).sel(feature_id=location_ids, time=slice(start_date, end_date))

    # Fetch all at once
    if chunk_by is None:

        da = ds[variable_name]
        df = da_to_df(nwm_version, da)
        min_time = df.value_time.min().strftime("%Y%m%d%HZ")
        max_time = df.value_time.max().strftime("%Y%m%d%HZ")
        output_filepath = Path(
            output_parquet_dir, f"{min_time}_{max_time}.parquet"
        )
        write_parquet_file(output_filepath, overwrite_output, df)
        return

    # Fetch data by site
    if chunk_by == "location_id":
        for location_id in location_ids:

            da = ds[variable_name].sel(feature_id=location_id)
            df = da_to_df(nwm_version, da)
            min_time = df.value_time.min().strftime("%Y%m%d%HZ")
            max_time = df.value_time.max().strftime("%Y%m%d%HZ")
            output_filepath = Path(
                output_parquet_dir,
                f"{location_id}_{min_time}_{max_time}.parquet"
            )
            write_parquet_file(output_filepath, overwrite_output, df)
        return

    # Group dataset by day, week, month, or year
    if chunk_by == "day":
        gps = ds.groupby("time.day")

    if chunk_by == "week":
        # Calendar week: Monday to Sunday
        gps = ds.groupby(ds.time.dt.isocalendar().week)

    if chunk_by == "month":
        # Calendar month
        gps = ds.groupby("time.month")

    if chunk_by == "year":
        # Calendar year
        gps = ds.groupby("time.year")

    # Process the data by selected chunk
    for _, ds_i in gps:
        df = da_to_df(nwm_version, ds_i[variable_name])
        output_filename = format_grouped_filename(ds_i)
        output_filepath = Path(
            output_parquet_dir, output_filename
        )
        write_parquet_file(output_filepath, overwrite_output, df)


if __name__ == "__main__":
    # Examples

    LOCATION_IDS = [7086109, 7040481]

    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date="2000-01-01",
        end_date="2000-01-02",
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm20_retrospective"),
    )

    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm20_retrospective"),
        chunk_by="day",
    )

    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm20_retrospective"),
        chunk_by="location_id",
    )

    nwm_retro_to_parquet(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date="2000-01-01",
        end_date="2000-01-02",
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm21_retrospective"),
    )

    nwm_retro_to_parquet(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm21_retrospective"),
        chunk_by="day",
    )

    nwm_retro_to_parquet(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=Path(Path().home(), "temp", "nwm21_retrospective"),
        chunk_by="location_id",
    )
    pass
