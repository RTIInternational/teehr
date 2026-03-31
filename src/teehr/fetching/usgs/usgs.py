"""Module for fetching and processing USGS streamflow data.

The function ``usgs_to_parquet`` fetches USGS streamflow data and saves it to
parquet files following the TEEHR data model.  The USGS tool ``dataretrieval``
is used to fetch the data from the USGS API.  Several ``chunk_by`` options are
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
import logging

import numpy as np
from typing import List, Union, Optional, Dict
from pathlib import Path
from datetime import datetime, timedelta
from dataretrieval import waterdata
from teehr.models.fetching.utils import USGSChunkByEnum, USGSServiceEnum
from pydantic import validate_call, ConfigDict
from teehr.fetching.utils import (
    write_timeseries_parquet_file,
    get_period_start_end_times,
    create_periods_based_on_chunksize
)
from teehr.models.fetching.utils import TimeseriesTypeEnum
from teehr.fetching.const import (
    USGS_NODATA_VALUES,
    USGS_CONFIGURATION_NAME,
    VALUE,
    VALUE_TIME,
    REFERENCE_TIME,
    LOCATION_ID,
    UNIT_NAME,
    VARIABLE_NAME,
    CONFIGURATION_NAME,
    USGS_VARIABLE_MAPPER
)

HOURLY_DATETIME_STR_FMT = "%Y-%m-%dT%H:%M:00+0000"
DAILY_DATETIME_STR_FMT = "%Y-%m-%d"

DAYLIGHT_SAVINGS_PAD = timedelta(hours=2)

pd.options.mode.copy_on_write = True

logger = logging.getLogger(__name__)


def _filter_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out data not reported on the hour."""
    logger.debug("Filtering to hourly data.")
    df.set_index(VALUE_TIME, inplace=True)
    df2 = df[
        df.index.hour.isin(range(0, 24))
        & (df.index.minute == 0)
        & (df.index.second == 0)
    ]
    # Preserve the timezone-aware DatetimeIndex when resetting
    df2.reset_index(inplace=True)
    return df2


def _filter_no_data(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out no data values."""
    logger.debug("Filtering out no data values.")
    df2 = df.loc[~df[VALUE].isin(USGS_NODATA_VALUES), :]
    df2.dropna(subset=[VALUE], inplace=True)
    return df2


def _convert_to_si_units(df: pd.DataFrame, unit_name: str) -> pd.DataFrame:
    """Convert streamflow values from english to metric."""
    logger.debug("Converting to SI units.")
    df[VALUE] = df[VALUE] * 0.3048**3
    df[UNIT_NAME] = unit_name
    return df


def _format_df_column_names(
    df: pd.DataFrame,
    variable_name: str,
    unit_name: str,
) -> pd.DataFrame:
    """Format dataretrieval dataframe columns to TEEHR data model."""
    logger.debug("Formatting column names.")
    # Create numpy arrays for other columns
    location_id_array = df['monitoring_location_id'].str.lower().values
    value_array = df["value"].values
    reference_time_array = np.full(len(df), np.nan, dtype=object)
    variable_name_array = np.full(len(df), variable_name, dtype=object)
    unit_name_array = np.full(len(df), unit_name, dtype=object)
    configuration_name_array = np.full(
        len(df), USGS_CONFIGURATION_NAME, dtype=object
    )
    # Create dictionary with column names as keys
    data_dict = {
        LOCATION_ID: location_id_array,
        REFERENCE_TIME: reference_time_array,
        VALUE_TIME: df["time"],  # preserve UTC timezone-aware datetime
        VALUE: value_array,
        VARIABLE_NAME: variable_name_array,
        UNIT_NAME: unit_name_array,
        CONFIGURATION_NAME: configuration_name_array
    }
    # Construct DataFrame from dictionary
    result_df = pd.DataFrame(data_dict)
    return result_df


def _fetch_site_data_by_description(
    site_dict: Dict[str, str],
    start_dt_iso: str,
    end_dt_iso: str,
    service: str
) -> pd.DataFrame:
    """Fetch data for a single site based on the description."""
    site_no = site_dict["site_no"]
    description = site_dict["description"].strip("[]()").lower()
    metadata_df, _ = waterdata.get_time_series_metadata(
        monitoring_location_id=site_no,
        parameter_code="00060"
    )
    matching_rows = metadata_df[
        metadata_df["web_description"].str.strip("[]()").str.lower() == description
    ]
    if matching_rows.empty:
        logger.error(
            f"No time series found for site {site_no} with description '{description}'."
            " Please check the site number and description."
        )
        return None
    elif len(matching_rows) > 1:
        logger.error(
            f"Multiple time series found for site {site_no} with description '{description}'."
            " Please check the site number and description to ensure it is specific enough to return a single time series."
        )
        return None
    else:
        time_series_id = matching_rows["time_series_id"].iloc[0]
        if service == "iv":
            usgs_df, _ = waterdata.get_continuous(
                monitoring_location_id=site_no,
                parameter_code="00060",
                time=f"{start_dt_iso}Z/{end_dt_iso}Z",
                time_series_id=time_series_id
            )
        elif service == "dv":
            usgs_df, _ = waterdata.get_daily(
                monitoring_location_id=site_no,
                parameter_code='00060',
                time=f"{start_dt_iso}Z/{end_dt_iso}Z",
                time_series_id=time_series_id
            )
        return usgs_df


def _fetch_usgs_streamflow(
    sites: Union[
        List[str],
        List[Dict[str, str]],
        List[Union[str, Dict[str, str]]]
    ],
    start_date: datetime,
    end_date: datetime,
    service: str,
    variable_mapper: Dict[str, Dict[str, str]],
    filter_to_hourly: bool = True,
    filter_no_data: bool = True,
    convert_to_si: bool = True,
) -> pd.DataFrame:
    """Fetch USGS gage data and format to TEEHR format."""
    logger.debug("Fetching USGS streamflow data from NWIS.")

    start_dt_iso = start_date.isoformat()
    end_dt_iso = (
        end_date
        - timedelta(minutes=1)
    ).isoformat()

    # Split the sites into those that are a string and those that are dictionaries,
    # to handle the description filtering for the latter.
    string_sites = [site for site in sites if isinstance(site, str)]
    dict_sites = [site for site in sites if isinstance(site, dict)]
    # Loop over dict_sites. For each site, make a call to get_time_series_metadata()
    # to get the metadata for that site. From that output, get the time_series_id that
    # matches the description in the site dictionary. Then, make a call to get_continuous()
    # or get_daily() and filter by time_series_id to get the data for that site.
    site_df_list = []
    for site_dict in dict_sites:
        site_df = _fetch_site_data_by_description(
            site_dict=site_dict,
            start_dt_iso=start_dt_iso,
            end_dt_iso=end_dt_iso,
            service=service
         )
        if site_df is not None:
            site_df_list.append(site_df)

    # Fetch sites that are just strings (i.e. no description filtering needed)
    usgs_df = None
    if len(string_sites) > 0:
        # Retrieve data --> dataretrieval
        if service == "iv":
            usgs_df, _ = waterdata.get_continuous(
                monitoring_location_id=string_sites,
                parameter_code="00060",
                time=f"{start_dt_iso}Z/{end_dt_iso}Z"
            )
        elif service == "dv":
            usgs_df, _ = waterdata.get_daily(
                monitoring_location_id=string_sites,
                parameter_code='00060',
                time=f"{start_dt_iso}Z/{end_dt_iso}Z",
                skip_geometry=True
            )

    if len(site_df_list) > 0:
        if usgs_df is not None and not usgs_df.empty:
            site_df_list.append(usgs_df)
        usgs_df = pd.concat(site_df_list)

    variable_name = variable_mapper[VARIABLE_NAME][service]
    unit_name = variable_mapper[UNIT_NAME]["Imperial"]

    if usgs_df["time"].dt.tz is None:
        usgs_df["time"] = usgs_df["time"].dt.tz_localize("UTC")
    else:
        usgs_df["time"] = usgs_df["time"].dt.tz_convert("UTC")

    usgs_df = _format_df_column_names(
        df=usgs_df,
        variable_name=variable_name,
        unit_name=unit_name
    )
    if usgs_df is None or usgs_df.empty:
        return None

    if filter_to_hourly is True:
        usgs_df = _filter_to_hourly(usgs_df)
    if filter_no_data is True:
        usgs_df = _filter_no_data(usgs_df)
    if convert_to_si is True:
        unit_name = variable_mapper[UNIT_NAME]["SI"]
        usgs_df = _convert_to_si_units(usgs_df, unit_name)

    return usgs_df


def _format_output_filename(chunk_by: str, start_dt, end_dt) -> str:
    """Format the output filename based on min and max datetime."""
    logger.debug("Formatting output filename.")
    if chunk_by == "day":
        return f"{start_dt.strftime('%Y-%m-%d')}.parquet"
    else:
        start = start_dt.strftime('%Y-%m-%d')
        end = end_dt.strftime('%Y-%m-%d')
        return f"{start}_{end}.parquet"


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def usgs_to_parquet(
    sites: Union[
        List[str],
        List[Dict[str, str]],
        List[Union[str, Dict[str, str]]]
    ],
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Union[str, datetime, pd.Timestamp],
    output_parquet_dir: Union[str, Path],
    service: USGSServiceEnum = USGSServiceEnum.iv,
    chunk_by: Union[USGSChunkByEnum, None] = None,
    filter_to_hourly: bool = True,
    filter_no_data: bool = True,
    convert_to_si: bool = True,
    overwrite_output: Optional[bool] = False,
    variable_mapper: Dict[str, Dict[str, str]] = USGS_VARIABLE_MAPPER,
    timeseries_type: TimeseriesTypeEnum = "primary"
):
    """Fetch USGS gage data and save as a Parquet file.

    All dates and times within the files and in the file names are in UTC.

    Parameters
    ----------
    sites : List[str]
        List of USGS gages sites to fetch.
        Must be string consisting of the USGS site number prefixed with "USGS-"
        (note capitalization).

        In some edge cases, a gage site may contain one or more
        sub-locations that also measure discharge. To differentiate
        these sub-locations, a dictionary can be passed in for a site.
        Each dictionary should contain the site number and a description
        of the sub-location. The description is used to filter the
        data to the specific sub-location. For example:
        [{"site_no": "USGS-02449838", "description": "Main Gage"}]

        Note that the dictionary must contain the keywords
        'site_no' and 'description'.
    start_date : datetime
        Start time of data to fetch.
    end_date : datetime
        End time of data to fetch. Note, since startDt is inclusive for the
        USGS service, we subtract 1 minute from this time so we don't get
        overlap between consecutive calls.
    output_parquet_dir : Union[str, Path]
        Path of directory where parquet files will be saved.
    service : USGSServiceEnum
        The USGS service to use for fetching data ('iv' for hourly
        instantaneous values or 'dv' for daily mean values).
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
    varible_mapper : Dict[str]
        Dictionary mapping the USGS service to the variable name and defining
        the unit name. default: {"variable": {"iv": "streamflow_hourly_inst",
        "dv": "streamflow_daily_mean"}, "unit": {"SI": "m^3/s",
        "Imperial": "ft^3/s"}}

    .. note::

       Only codes '00060' (Discharge, cubic feet per second, service='iv')
       and '00060_Mean' (Discharge, Mean cubic feet per second, service='dv')
       are supported. If data is returned from NWIS with a different field name,
       such as '00060_total spillway releases' in the case of a reservoir,
       the function will return None and log an error message.

    .. note::

       For higher rate limits and more reliable access, register for a free
       USGS Water Data API key at https://api.waterdata.usgs.gov/signup/.
       Once obtained, set it as an environment variable before fetching data::

           import os
           os.environ['API_USGS_PAT'] = 'your_api_key_here'

    Examples
    --------
    Here we fetch five days worth of USGS hourly streamflow data, to two gages,
    chunking by day.

    Import the module.

    >>> from teehr.fetching.usgs.usgs import usgs_to_parquet

    Set the input variables.

    >>> SITES=["USGS-02449838", "USGS-02450825"]
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
    """  # noqa
    logger.info("Fetching USGS streamflow data.")
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
                service=service,
                variable_mapper=variable_mapper,
                filter_to_hourly=filter_to_hourly,
                filter_no_data=filter_no_data,
                convert_to_si=convert_to_si,
            )

            if usgs_df is None or usgs_df.empty:
                logger.warning(
                    f"No USGS streamflow data returned for the specified site: {site}"
                    f" and date range: {start_date} to {end_date}."
                    " This site will be skipped."
                )
                continue
            else:
                usgs_df = usgs_df[(usgs_df[VALUE_TIME] >= start_date.tz_localize("UTC")) &
                                  (usgs_df[VALUE_TIME] < end_date.tz_localize("UTC"))]
                if isinstance(site, dict):
                    site = site["site_no"]
                output_filepath = Path(
                    output_parquet_dir,
                    f"{site}.parquet"
                )
                write_timeseries_parquet_file(
                    filepath=output_filepath,
                    overwrite_output=overwrite_output,
                    data=usgs_df,
                    timeseries_type=timeseries_type
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
            service=service,
            variable_mapper=variable_mapper,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si
        )

        if usgs_df is None or usgs_df.empty:
            logger.warning(
                "No data returned from USGS for the specified sites and date range: "
                f"{dts['start_dt']} to {dts['end_dt']}, skipping period."
            )
            continue
        else:
            usgs_df = usgs_df[
                (usgs_df[VALUE_TIME] >= dts["start_dt"].tz_localize("UTC")) &
                (usgs_df[VALUE_TIME] <= dts["end_dt"].tz_localize("UTC"))
            ]

            output_filename = _format_output_filename(
                chunk_by, dts["start_dt"], dts["end_dt"]
            )

            output_filepath = Path(output_parquet_dir, output_filename)
            write_timeseries_parquet_file(
                filepath=output_filepath,
                overwrite_output=overwrite_output,
                data=usgs_df,
                timeseries_type=timeseries_type
            )
