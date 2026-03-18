"""Convert TEEHR API response data to TEEHR timeseries format."""
from datetime import datetime
from typing import Union, Optional
import pandas as pd
import logging
import time

logger = logging.getLogger(__name__)


def teehr_api_timeseries_to_dataframe(
    json_data: Union[dict, list],
) -> pd.DataFrame:
    """Convert TEEHR API timeseries response to pandas DataFrame.

    Parameters
    ----------
    json_data : dict or list
        Response from TEEHR API /collections/primary_timeseries/items or
        /collections/secondary_timeseries/items
        Can be a single dict or list of dicts, each containing records
        from the API response.

    Returns
    -------
    pd.DataFrame
        DataFrame with TEEHR timeseries schema

    Examples
    --------
    >>> import requests
    >>> response = requests.get(url, params=params)
    >>> json_data = response.json()
    >>> df = teehr_api_timeseries_to_dataframe(json_data)
    """
    # Handle single dict or list of dicts
    start_time = time.time()

    if not json_data:
        raise ValueError("No timeseries data found in the API response.")

    if isinstance(json_data, dict):
        json_data = [json_data]

    df = pd.DataFrame(json_data)

    # Rename columns to match TEEHR schema
    df.rename(
        columns={
            'primary_location_id': 'location_id',
            'secondary_location_id': 'location_id',
        },
        inplace=True,
        errors='ignore'  # ignore if columns don't exist
    )

    # Drop columns that are not part of the TEEHR timeseries schema
    df.drop(columns=['series_type'], inplace=True, errors='ignore')

    # Validate columns required for this function and raise error if missing
    required_columns = ['value_time', 'value', 'reference_time']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Required columns missing from API response: {missing_columns}"
        )

    # reference_time is optional (only present for secondary timeseries, not primary), so add it if missing and fill with NaT
    if 'reference_time' not in df.columns:
        df['reference_time'] = pd.NaT

    # Replace 'null' string with None in reference_time column
    df['reference_time'] = df['reference_time'].replace('null', pd.NaT)

    # Convert value_time to datetime and value to numeric, handling errors
    df['value_time'] = pd.to_datetime(df['value_time'], errors='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df["reference_time"] = pd.to_datetime(df["reference_time"], errors='coerce')

    # Remove null values
    df.dropna(subset=['value'], inplace=True)

    logger.info(
        f"Converted {len(df)} timeseries values from TEEHR API response in {time.time() - start_time:.2f} s"
    )

    return df


def format_datetime_range(
    start_date: Union[str, datetime, pd.Timestamp, None] = None,
    end_date: Union[str, datetime, pd.Timestamp, None] = None
) -> Optional[str]:
    """Format start and end dates into ISO 8601 datetime range string.

    Parameters
    ----------
    start_date : Union[str, datetime, pd.Timestamp, None], optional
        Start date/time. If None and end_date is provided, returns "../end_date".
        If both None, returns None.
    end_date : Union[str, datetime, pd.Timestamp, None], optional
        End date/time. If None and start_date is provided, returns "start_date/..".

    Returns
    -------
    Optional[str]
        ISO 8601 datetime range string (e.g., "2020-01-01/2020-12-31")
        or open-ended range (e.g., "2020-01-01/.." or "../2020-12-31")
        or None if both dates are None
    """
    if start_date is None and end_date is None:
        return None

    if start_date is None:
        end_dt = pd.Timestamp(end_date)
        return f"../{end_dt.isoformat()}"

    start_dt = pd.Timestamp(start_date)

    if end_date is None:
        return f"{start_dt.isoformat()}/.."

    end_dt = pd.Timestamp(end_date)
    return f"{start_dt.isoformat()}/{end_dt.isoformat()}"
