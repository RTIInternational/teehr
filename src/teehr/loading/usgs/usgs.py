"""Module for loading and processing USGS streamflow data.

The function ``usgs_to_parquet`` fetches USGS streamflow data and saves it to
parquet files following the TEEHR data model.  The OWP tool ``HydroTools`` is
used to fetch the data from the USGS API.  Several ``chunk_by`` options are
included to allow for the data to be fetched by location_id, day, week, month,
and year (or None).

Note that the USGS API is called for each unique value in the specified
chunk_by option.  For example, if chunk_by is set to "location_id", the USGS
API will be called for each unique location_id in the provided list.

.. note::
   Care should be taken to select the appropriate chunk_by option to based on
   the number of locations and the time period of interest, to avoid excessive
   API calls and produce more efficient queries.
"""
import pandas as pd

from typing import List, Union, Optional
from pathlib import Path
from datetime import datetime, timedelta
# from hydrotools.nwis_client.iv import IVDataService
import dataretrieval.nwis as nwis
from teehr.models.loading.utils import USGSChunkByEnum
from pydantic import validate_call, ConfigDict
from teehr.loading.utils import (
    write_parquet_file,
    get_period_start_end_times,
    create_periods_based_on_chunksize,
    format_timeseries_data_types
)
from teehr.loading.const import USGS_NODATA_VALUES

DATETIME_STR_FMT = "%Y-%m-%dT%H:%M:00+0000"
DAYLIGHT_SAVINGS_PAD = timedelta(hours=2)


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


def _filter_no_data(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out no data values."""
    df2 = df[~df["value"].isin(USGS_NODATA_VALUES)]
    df2.dropna(subset=["value"], inplace=True)
    return df2


def _convert_to_si_units(df: pd.DataFrame) -> pd.DataFrame:
    """Convert streamflow values from english to metric."""
    df["value"] = df["value"] * 0.3048**3
    df["measurement_unit"] = "m3/s"
    return df


# def _datetime_to_date(dt: datetime) -> datetime:
#     """Convert datetime to date only."""
#     dt.replace(
#         hour=0,
#         minute=0,
#         second=0,
#         microsecond=0
#     )
#     return dt


# def _format_df_data_types(df: pd.DataFrame) -> pd.DataFrame:
#     """Convert field types to TEEHR data model.

#     Notes
#     -----
#     dataretrieval attempts to return values in UTC, here we explicitly convert
#     to be sure. We also drop timezone information and convert to datetime64[ms].

#     The fields types are specified in the TIMESERIES_DATA_TYPES dictionary.
#     """
#     # Convert to UTC if not already in UTC.
#     if df["value_time"].dt.tz is not None:
#         df["value_time"] = df["value_time"].dt.tz_convert("UTC")
#     if df["reference_time"].dt.tz is not None:
#         df["reference_time"] = df["reference_time"].dt.tz_convert("UTC")
#     # Drop timezone information.
#     df["value_time"] = df["value_time"].dt.tz_localize(None)
#     df["reference_time"] = df["reference_time"].dt.tz_localize(None)
#     # Convert to datetime64[ms].
#     df["value_time"] = df["value_time"].astype(TIMESERIES_DATA_TYPES["value_time"])  # noqa
#     df["reference_time"] = df["reference_time"].astype(TIMESERIES_DATA_TYPES["reference_time"])  # noqa
#     # Convert remaining fields.
#     df["value"] = df["value"].astype(TIMESERIES_DATA_TYPES["value"])
#     df["measurement_unit"] = df["measurement_unit"].astype(TIMESERIES_DATA_TYPES["measurement_unit"])  # noqa
#     df["variable_name"] = df["variable_name"].astype(TIMESERIES_DATA_TYPES["variable_name"])  # noqa
#     df["configuration"] = df["configuration"].astype(TIMESERIES_DATA_TYPES["configuration"])  # noqa
#     df["location_id"] = df["location_id"].astype(TIMESERIES_DATA_TYPES["location_id"])  # noqa

#     return df


def _format_df_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Format dataretrieval dataframe columns to TEEHR data model."""
    df.reset_index(inplace=True)
    df.rename(
        columns={
            "site_no": "location_id",
            "00060": "value",
            "datetime": "value_time"
        },
        inplace=True
    )
    df["location_id"] = "usgs-" + df["location_id"].astype(str)
    df["configuration"] = "usgs_gage_data"
    df["reference_time"] = df["value_time"]
    df["measurement_unit"] = "ft3/s"
    df["variable_name"] = "streamflow"
    return df[[
        "location_id",
        "reference_time",
        "value_time",
        "value",
        "variable_name",
        "measurement_unit",
        "configuration"
    ]]


def _format_ht_df(df: pd.DataFrame) -> pd.DataFrame:
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


def _fetch_usgs_streamflow(
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

    # # Retrieve data --> HydroTools
    # service = IVDataService(
    #     value_time_label="value_time",
    #     enable_cache=False
    # )
    # usgs_ht_df = service.get(
    #     sites=sites,
    #     startDT=start_dt_str,
    #     endDT=end_dt_str
    # )

    # Retrieve data --> dataretrieval
    usgs_df = nwis.get_record(
        sites=sites,
        service="iv",  # Instantaneous values
        start=start_dt_str,
        end=end_dt_str
    )

    # NOTE: Can use get_iv to include station metadata.
    # df, meta = nwis.get_iv(sites=sites, start=start_dt_str, end=end_dt_str)

    usgs_df = _format_df_column_names(usgs_df)

    usgs_df = format_timeseries_data_types(usgs_df)

    if filter_to_hourly is True:
        usgs_df = _filter_to_hourly(usgs_df)
    if filter_no_data is True:
        usgs_df = _filter_no_data(usgs_df)
    if convert_to_si is True:
        usgs_df = _convert_to_si_units(usgs_df)

    # # HydroTools data
    # if filter_to_hourly is True:
    #     usgs_ht_df = _filter_to_hourly(usgs_ht_df)
    # if filter_no_data is True:
    #     usgs_ht_df = _filter_no_data(usgs_ht_df)
    # if convert_to_si is True:
    #     usgs_ht_df = _convert_to_si_units(usgs_ht_df)
    # usgs_ht_df = _format_ht_df(usgs_ht_df)

    # Return the data
    return usgs_df


def _format_output_filename(chunk_by: str, start_dt, end_dt) -> str:
    """Format the output filename based on min and max datetime."""
    if chunk_by == "day":
        return f"{start_dt.strftime('%Y-%m-%d')}.parquet"
    else:
        start = start_dt.strftime('%Y-%m-%d')
        end = end_dt.strftime('%Y-%m-%d')
        return f"{start}_{end}.parquet"


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def usgs_to_parquet(
    sites: List[str],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    chunk_by: Union[USGSChunkByEnum, None] = None,
    filter_to_hourly: bool = True,
    filter_no_data: bool = True,
    convert_to_si: bool = True,
    overwrite_output: Optional[bool] = False,
):
    """Fetch USGS gage data and save as a Parquet file.

    All dates and times within the files and in the file names are in UTC.

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
        Valid options = ["location_id", "day", "week", "month", "year", None].
    filter_to_hourly : bool = True
        Return only values that fall on the hour (i.e. drop 15 minute data).
    filter_no_data : bool = True
        Filter out -999 values.
    convert_to_si : bool = True
        Multiplies values by 0.3048**3 and sets `measurement_units` to `m3/s`.
    overwrite_output : bool
        Flag specifying whether or not to overwrite output files if they
        already exist.  True = overwrite; False = fail.

    Examples
    --------
    Here we fetch five days worth of USGS hourly streamflow data, to two gages,
    chunking by day.

    Import the module.

    >>> from teehr.loading.usgs.usgs import usgs_to_parquet

    Set the input variables.

    >>> SITES=["02449838", "02450825"]
    >>> START_DATE=datetime(2023, 2, 20)
    >>> END_DATE=datetime(2023, 2, 25)
    >>> OUTPUT_PARQUET_DIR=Path(Path().home(), "temp", "usgs")
    >>> CHUNK_BY="day",
    >>> OVERWRITE_OUTPUT=True

    Fetch the data, writing to the specified output directory.

    >>> usgs_to_parquet(
    >>>     sites=SITES,
    >>>     start_date=START_DATE,
    >>>     end_date=END_DATE,
    >>>     output_parquet_dir=TEMP_DIR,
    >>>     chunk_by=CHUNK_BY,
    >>>     overwrite_output=OVERWRITE_OUTPUT
    >>> )
    """
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    # Check if output_parquet_dir is an existing dir
    output_dir = Path(output_parquet_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    if chunk_by == "location_id":
        for site in sites:
            usgs_df = _fetch_usgs_streamflow(
                sites=[site],
                start_date=start_date - DAYLIGHT_SAVINGS_PAD,
                end_date=end_date + DAYLIGHT_SAVINGS_PAD,
                filter_to_hourly=filter_to_hourly,
                filter_no_data=filter_no_data,
                convert_to_si=convert_to_si
            )

            usgs_df = usgs_df[(usgs_df["value_time"] >= start_date) &
                              (usgs_df["value_time"] < end_date)]

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
        return

    # Chunk data by time
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

        usgs_df = _fetch_usgs_streamflow(
            sites=sites,
            start_date=dts["start_dt"] - DAYLIGHT_SAVINGS_PAD,
            end_date=dts["end_dt"] + DAYLIGHT_SAVINGS_PAD,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si
        )

        usgs_df = usgs_df[(usgs_df["value_time"] >= dts["start_dt"]) &
                          (usgs_df["value_time"] <= dts["end_dt"])]

        if len(usgs_df) > 0:

            output_filename = _format_output_filename(
                chunk_by, dts["start_dt"], dts["end_dt"]
            )

            output_filepath = Path(output_parquet_dir, output_filename)
            write_parquet_file(
                filepath=output_filepath,
                overwrite_output=overwrite_output,
                data=usgs_df
            )


# if __name__ == "__main__":
#     # Examples
#     usgs_to_parquet(
#         sites=[
#             "02449838",
#             "02450825"
#         ],
#         start_date=datetime(2023, 2, 20),
#         end_date=datetime(2023, 2, 25),
#         output_parquet_dir=Path(Path().home(), "temp", "usgs"),
#         chunk_by="location_id",
#         overwrite_output=True
#     )

#     usgs_to_parquet(
#         sites=[
#             "02449838",
#             "02450825"
#         ],
#         start_date=datetime(2023, 2, 20),
#         end_date=datetime(2023, 2, 25),
#         output_parquet_dir=Path(Path().home(), "temp", "usgs"),
#         chunk_by="day",
#         overwrite_output=True
#     )

#     usgs_to_parquet(
#         sites=[
#             "02449838",
#             "02450825"
#         ],
#         start_date=datetime(2023, 2, 20),
#         end_date=datetime(2023, 2, 25),
#         output_parquet_dir=Path(Path().home(), "temp", "usgs"),
#         overwrite_output=True
#     )
#     pass
