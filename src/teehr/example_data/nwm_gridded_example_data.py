"""Module to fetch test NWM streamflow data for the user guide."""

import requests


files = [
    {
        "name": "usgs_at_radford_location.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_streamflow/usgs_at_radford_location.parquet"
    },
    {
        "name": "primary_timeseries.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/primary_timeseries.parquet"
    },
    {
        "name": "secondary_timeseries.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/secondary_timeseries.parquet"
    },
    {
        "name": "location_crosswalks.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_streamflow/location_crosswalks.parquet"
    },
    {
        "name": "joined_timeseries.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/joined_timeseries.parquet"
    },
    {
        "name": "forcing_xwalk.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/forcing_xwalk.parquet"
    },
    {
        "name": "configurations.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/configurations.parquet"
    },
    {
        "name": "nwm30_forcing_analysis_assim_pixel_weights.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/nwm30_forcing_analysis_assim_pixel_weights.parquet"
    },
    {
        "name": "three_huc10s_radford.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/fetch_nwm_grids/three_huc10s_radford.parquet"
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