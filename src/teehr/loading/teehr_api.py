"""Convert TEEHR API response data to TEEHR timeseries format."""
from typing import Union
import pandas as pd
import logging

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
        Can be a single dict or list of dicts, each containing:
        - series metadata (configuration_name, variable_name, unit_name, etc.)
        - timeseries: list of {value_time, value} dicts

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
    if isinstance(json_data, dict):
        json_data = [json_data]

    all_rows = []

    for item in json_data:
        # Extract metadata
        series_type = item.get('series_type', 'primary')

        if series_type == 'primary':
            location_id = item.get('primary_location_id')
        elif series_type == 'secondary':
            location_id = item.get('secondary_location_id')
        else:
            raise ValueError(f"Series type not recognized: {series_type}")

        reference_time = item.get('reference_time')
        configuration_name = item.get('configuration_name')
        variable_name = item.get('variable_name')
        unit_name = item.get('unit_name')
        member = item.get('member') if series_type == 'secondary' else None

        # Handle 'null' string as None
        if reference_time == 'null':
            reference_time = None

        # Expand timeseries array
        timeseries = item.get('timeseries', [])

        for ts_point in timeseries:
            row = {
                'reference_time': pd.to_datetime(reference_time) if reference_time else pd.NaT,
                'value_time': pd.to_datetime(ts_point['value_time']),
                'value': ts_point['value'],
                'variable_name': variable_name,
                'configuration_name': configuration_name,
                'unit_name': unit_name,
                'location_id': location_id
            }

            # Add member field for secondary timeseries
            if series_type == 'secondary':
                row['member'] = member

            all_rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(all_rows)
    if df.index.size == 0:
        raise ValueError("No timeseries data found in the API response.")

    # Reorder columns to match TEEHR schema
    if 'member' in df.columns:
        # Secondary timeseries schema
        df = df[['reference_time', 'value_time', 'value', 'variable_name',
                 'configuration_name', 'unit_name', 'location_id', 'member']]
    else:
        # Primary timeseries schema
        df = df[['reference_time', 'value_time', 'value', 'variable_name',
                 'configuration_name', 'unit_name', 'location_id']]

    # Remove null values
    df = df[df['value'].notna()]

    logger.info(f"Converted {len(df)} timeseries values from TEEHR API response")

    return df
