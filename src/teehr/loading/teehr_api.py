"""Convert TEEHR API response data to TEEHR timeseries format."""
from typing import Union
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

    reference_times = []
    value_times = []
    values = []
    variable_names = []
    configuration_names = []
    unit_names = []
    location_ids = []
    members = []
    has_member = False

    start_time = time.time()

    for item in json_data:
        # Extract metadata
        series_type = item.get('series_type', 'primary')

        if series_type == 'primary':
            location_id = item.get('primary_location_id')
        elif series_type == 'secondary':
            location_id = item.get('secondary_location_id')
            has_member = True
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

        timeseries = item.get('timeseries', [])
        if not timeseries:
            continue

        n = len(timeseries)
        reference_times.extend([reference_time] * n)
        value_times.extend(ts['value_time'] for ts in timeseries)
        values.extend(ts['value'] for ts in timeseries)
        variable_names.extend([variable_name] * n)
        configuration_names.extend([configuration_name] * n)
        unit_names.extend([unit_name] * n)
        location_ids.extend([location_id] * n)
        if series_type == 'secondary':
            members.extend([member] * n)

    if not value_times:
        raise ValueError("No timeseries data found in the API response.")

    col_data = {
        'reference_time': pd.to_datetime(reference_times),
        'value_time': pd.to_datetime(value_times),
        'value': values,
        'variable_name': variable_names,
        'configuration_name': configuration_names,
        'unit_name': unit_names,
        'location_id': location_ids,
    }
    if has_member:
        col_data['member'] = members

    # col_data keys are already in TEEHR schema order
    df = pd.DataFrame(col_data)

    # Remove null values
    df.dropna(subset=['value'], inplace=True)

    logger.info(
        f"Converted {len(df)} timeseries values from TEEHR API response in {time.time() - start_time:.2f} s"
    )

    return df
