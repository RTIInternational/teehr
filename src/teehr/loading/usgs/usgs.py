import pandas as pd

from typing import List, Union, Optional
from pathlib import Path
from datetime import datetime, timedelta
from hydrotools.nwis_client.iv import IVDataService
from teehr.models.loading.utils import ChunkByEnum
from pydantic import validate_call, ConfigDict
from teehr.loading.nwm.utils import write_parquet_file

DATETIME_STR_FMT = "%Y-%m-%dT%H:%M:00+0000"


def _filter_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out data not reported on the hour."""
    df.set_index("value_time", inplace=True)
    df2 = df[
        df.index.hour.isin(range(0, 24))
        & (df.index.minute == 0)
        & (df.index.second == 0)
    ]
    df2.reset_index(level=0, allow_duplicates=True, inplace=True)
    return df2


def _filter_no_data(df: pd.DataFrame, no_data_value=-999) -> pd.DataFrame:
    """Filter out no data values."""

    df2 = df[df["value"] != no_data_value]
    return df2


def _convert_to_si_units(df: pd.DataFrame) -> pd.DataFrame:
    """Convert streamflow values from english to metric."""

    df["value"] = df["value"] * 0.3048**3
    df["measurement_unit"] = "m3/s"
    return df


def _datetime_to_date(dt: datetime) -> datetime:
    """Convert datetime to date only"""
    dt.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )
    return dt


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format HydroTools dataframe columns to TEEHR data model."""

    df.rename(columns={"usgs_site_code": "location_id"}, inplace=True)
    df["location_id"] = "usgs-" + df["location_id"].astype(str)
    df["configuration"] = "usgs_gage_data"
    df["reference_time"] = df["value_time"]
    return df[[
        "location_id",
        "reference_time",
        "value_time",
        "value",
        "variable_name",
        "measurement_unit",
        "configuration"
    ]]


def _fetch_usgs(
    sites: List[str],
    start_date: datetime,
    end_date: datetime,
    filter_to_hourly: bool = True,
    filter_no_data: bool = True,
    convert_to_si: bool = True
) -> pd.DataFrame:
    """Fetch USGS gage data and format to TEEHR format."""

    start_dt_str = start_date.strftime(DATETIME_STR_FMT)
    end_dt_str = (
        end_date
        - timedelta(minutes=1)
    ).strftime(DATETIME_STR_FMT)

    # Retrieve data
    service = IVDataService(
        value_time_label="value_time",
        enable_cache=False
    )
    usgs_df = service.get(
        sites=sites,
        startDT=start_dt_str,
        endDT=end_dt_str
    )

    if filter_to_hourly is True:
        usgs_df = _filter_to_hourly(usgs_df)
    if filter_no_data is True:
        usgs_df = _filter_no_data(usgs_df)
    if convert_to_si is True:
        usgs_df = _convert_to_si_units(usgs_df)
    usgs_df = _format_df(usgs_df)

    # Return the data
    return usgs_df


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def usgs_to_parquet(
    sites: List[str],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[ChunkByEnum, None] = None,
    filter_to_hourly: bool = True,
    filter_no_data: bool = True,
    convert_to_si: bool = True,
    overwrite_output: Optional[bool] = False,
):
    """Fetch USGS gage data and save as a Parquet file.

    Parameters
    ----------
    sites : List[str]
        List of USGS gages sites to fetch.
        Must be string to preserve the leading 0.
    start_date : datetime
        Start time of data to fetch.
    end_date : datetime
        End time of data to fetch. Note, since startDt is inclusive for the
        USGS service, we subtract 1 minute from this time so we don't get
        overlap between consecutive calls.
    output_parquet_dir : Union[str, Path]
        Path of directory where parquet files will be saved.
    chunk_by : Union[str, None], default = None
        How to "chunk" the fetching and storing of the data.
        Valid options = ["day", "site", None]
    filter_to_hourly: bool = True
        Return only values that fall on the hour (i.e. drop 15 minute data).
    filter_no_data: bool = True
        Filter out -999 values
    convert_to_si: bool = True
        Multiplies values by 0.3048**3 and sets `measurement_units` to `m3/s`
    overwrite_output: bool
        Flag specifying whether or not to overwrite output files if they
        already exist.  True = overwrite; False = fail
    """

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    # Check if output_parquet_dir is an existing dir
    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # Fetch all at once
    if chunk_by is None:
        usgs_df = _fetch_usgs(
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si
        )
        if len(usgs_df) > 0:
            output_filepath = Path(output_parquet_dir, "usgs.parquet")
            write_parquet_file(
                filepath=output_filepath,
                overwrite_output=overwrite_output,
                data=usgs_df
            )
            # output_filepath = Path(usgs.output_parquet_dir, "usgs.csv")
            # usgs_df.to_csv(output_filepath)
            # print(usgs_df)

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
            usgs_df = _fetch_usgs(
                sites=sites,
                start_date=start_dt,
                end_date=end_dt,
                filter_to_hourly=filter_to_hourly,
                filter_no_data=filter_no_data,
                convert_to_si=convert_to_si
            )
            if len(usgs_df) > 0:
                output_filepath = Path(
                    output_parquet_dir,
                    f"{start_dt.strftime('%Y-%m-%d')}.parquet"
                )
                write_parquet_file(
                    filepath=output_filepath,
                    overwrite_output=overwrite_output,
                    data=usgs_df
                )
                # output_filepath = Path(
                #     usgs.output_parquet_dir,
                #     f"{start_dt.strftime('%Y-%m-%d')}.csv"
                # )
                # usgs_df.to_csv(output_filepath)
                # print(usgs_df)

    if chunk_by == "location_id":
        for site in sites:
            usgs_df = _fetch_usgs(
                sites=[site],
                start_date=start_date,
                end_date=end_date,
                filter_to_hourly=filter_to_hourly,
                filter_no_data=filter_no_data,
                convert_to_si=convert_to_si
            )
            if len(usgs_df) > 0:
                output_filepath = Path(
                    output_parquet_dir,
                    f"{site}.parquet"
                )
                write_parquet_file(
                    filepath=output_filepath,
                    overwrite_output=overwrite_output,
                    data=usgs_df
                )
                # output_filepath = Path(
                #     usgs.output_parquet_dir,
                #     f"{site}.csv"
                # )
                # usgs_df.to_csv(output_filepath)
                # print(usgs_df)


if __name__ == "__main__":
    # Examples
    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=Path(Path().home(), "temp", "usgs"),
        chunk_by="location_id",
        overwrite_output=True
    )

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=Path(Path().home(), "temp", "usgs"),
        chunk_by="day",
        overwrite_output=True
    )

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=Path(Path().home(), "temp", "usgs"),
        overwrite_output=True
    )
    pass
