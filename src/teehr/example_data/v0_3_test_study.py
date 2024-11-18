"""Module to fetch test data."""

import requests


files = [
    {
        "name": "gages.geojson",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/geo/gages.geojson"
    },
    {
        "name": "test_short_obs.csv",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/timeseries/test_short_obs.csv"
    },
    {
        "name": "test_short_fcast.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/timeseries/test_short_fcast.parquet"
    },
    {
        "name": "crosswalk.csv",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/geo/crosswalk.csv"
    },
    {
        "name": "test_attr_2yr_discharge.csv",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/geo/test_attr_2yr_discharge.csv"
    },
    {
        "name": "test_attr_drainage_area_km2.csv",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/geo/test_attr_drainage_area_km2.csv"
    },
    {
        "name": "test_attr_ecoregion.csv",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/v0_3_test_study/geo/test_attr_ecoregion.csv"
    }
]

def fetch_and_save_file(url: str, destination: str) -> None:
    """Fetch a file from a URL and save it to a destination."""
    response = requests.get(url)
    response.raise_for_status()  # Ensure we notice bad responses

    with open(destination, 'wb') as f:
        f.write(response.content)


def list_files() -> list:
    return [file["name"] for file in files]


def fetch_file(file_name: str, destination: str) -> None:
    """Fetch a file by name."""
    file = next(file for file in files if file["name"] == file_name)
    fetch_and_save_file(file["url"], destination)


