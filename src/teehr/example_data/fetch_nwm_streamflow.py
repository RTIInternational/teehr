"""Module to fetch test NWM streamflow data for the user guide."""

import requests


files = [
    {
        "name": "usgs_at_radford_location.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/352-set-up-template-evaluations-for-fetching-operational-nwm-data/tests/data/fetch_nwm_streamflow/usgs_at_radford_location.parquet"
    },
    {
        "name": "primary_timeseries.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/352-set-up-template-evaluations-for-fetching-operational-nwm-data/tests/data/fetch_nwm_streamflow/timeseries/primary_timeseries.parquet"
    },
    {
        "name": "secondary_timeseries.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/352-set-up-template-evaluations-for-fetching-operational-nwm-data/tests/data/fetch_nwm_streamflow/timeseries/secondary_timeseries.parquet"
    },
    {
        "name": "location_crosswalks.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/352-set-up-template-evaluations-for-fetching-operational-nwm-data/tests/data/fetch_nwm_streamflow/location_crosswalks.parquet"
    }
]


def fetch_and_save_file(url: str, destination: str) -> None:
    """Fetch a file from a URL and save it to a destination."""
    response = requests.get(url)
    response.raise_for_status()  # Ensure we notice bad responses

    with open(destination, 'wb') as f:
        f.write(response.content)


def list_files() -> list:
    """List the available files."""
    return [file["name"] for file in files]


def fetch_file(file_name: str, destination: str) -> None:
    """Fetch a file by name."""
    file = next(file for file in files if file["name"] == file_name)
    fetch_and_save_file(file["url"], destination)