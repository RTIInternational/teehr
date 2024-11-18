"""Module to fetch test data."""

import requests


files = [
    {
        "name": "two_crosswalks.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/two_locations/two_crosswalks.parquet"
    },
    {
        "name": "two_location_attributes.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/two_locations/two_location_attributes.parquet"
    },
    {
        "name": "two_locations.parquet",
        "url": "https://github.com/RTIInternational/teehr/raw/refs/heads/main/tests/data/two_locations/two_locations.parquet"
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


