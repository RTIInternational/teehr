"""Test downloading from the S3 warehouse via the TEEHR API."""
import os
from types import SimpleNamespace

import pytest
import requests

from teehr.evaluation.download import Download
import geopandas as gpd


LOCAL_API_SETTINGS_MISSING = os.getenv("TEEHR_DOWNLOAD_API_KEY") is None
SKIP_LOCAL_API = pytest.mark.skipif(
    LOCAL_API_SETTINGS_MISSING,
    reason="Set TEEHR_DOWNLOAD_API_KEY to run local teehr-hub download tests.",
)


def _require_local_api_settings() -> tuple[str, str]:
    """Load local API settings from env vars."""
    api_base_url = os.getenv("TEEHR_DOWNLOAD_API_BASE_URL", "https://api.teehr.local.app.garden")
    api_key = os.getenv("TEEHR_DOWNLOAD_API_KEY")
    return api_base_url, api_key


class MockResponse:
    """Simple response double for requests-based unit tests."""

    def __init__(self, payload, http_error=None):
        """Initialize a mock response with JSON payload and optional HTTP error."""
        self._payload = payload
        self._http_error = http_error

    def json(self):
        """Return the configured JSON payload."""
        return self._payload

    def raise_for_status(self):
        """Raise the configured HTTP error when present."""
        if self._http_error:
            raise self._http_error


@pytest.mark.function_scope_evaluation_template
@pytest.mark.remote_api
@SKIP_LOCAL_API
def test_download_locations_by_ids(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    gdf = ev.download.locations(
        ids=["usgs-02424000", "usgs-03068800"],
        include_attributes=False,
    )
    assert len(gdf) == 2
    assert "id" in gdf.columns
    assert "geometry" in gdf.columns
    assert "name" in gdf.columns


@pytest.mark.function_scope_evaluation_template
@pytest.mark.remote_api
@SKIP_LOCAL_API
def test_download_evaluation_subset(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    ev.download.evaluation_subset(
        location_ids="usgs-03068800",
        start_date="2020-01-01",
        end_date="2020-01-02",
        primary_configuration_name="usgs_observations",
        secondary_configuration_name="nwm30_retrospective"
    )
    assert ev.locations.to_sdf().count() == 1
    assert ev.location_attributes.to_sdf().count() >= 48
    assert ev.units.to_sdf().count() >= 5
    assert ev.variables.to_sdf().count() >= 6
    assert ev.attributes.to_sdf().count() >= 50
    assert ev.configurations.to_sdf().count() == 2
    assert ev.primary_timeseries.to_sdf().count() == 25
    assert ev.secondary_timeseries.to_sdf().count() == 25


@pytest.mark.function_scope_evaluation_template
@pytest.mark.local_api
@SKIP_LOCAL_API
def test_download_from_local_primary_timeseries(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    api_base_url, api_key = _require_local_api_settings()

    ev.download.configure(
        api_base_url=api_base_url,
        api_key=api_key,
        verify_ssl=False,
    )
    df = ev.download.primary_timeseries(
        primary_location_id="usgs-03068800",
        start_date="2020-01-01",
        end_date="2020-02-01",
        configuration_name="usgs_observations",
        variable_name="streamflow_hourly_inst",
    )

    assert len(df) == 697


@pytest.mark.function_scope_evaluation_template
@pytest.mark.local_api
@SKIP_LOCAL_API
def test_download_from_local_locations(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    api_base_url, api_key = _require_local_api_settings()

    ev.download.configure(
        api_base_url=api_base_url,
        api_key=api_key,
        verify_ssl=False,
    )
    gdf = ev.download.locations()

    assert isinstance(gdf, gpd.GeoDataFrame)


@pytest.mark.function_scope_evaluation_template
@pytest.mark.local_api
@SKIP_LOCAL_API
def test_download_from_local_primary_timeseries_pagination(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    api_base_url, api_key = _require_local_api_settings()

    ev.download.configure(
        api_base_url=api_base_url,
        api_key=api_key,
        verify_ssl=False,
    )
    df = ev.download.primary_timeseries(
        primary_location_id="usgs-03068800",
        start_date="2020-01-01",
        end_date="2020-02-01",
        configuration_name="usgs_observations",
        variable_name="streamflow_hourly_inst",
        page_size=100,
    )

    assert len(df) == 697


def test_download_api_key_auth_success(monkeypatch):
    """Configured API keys are sent as x-api-key on outbound requests."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com", api_key="test-api-key")

    captured = {"headers": []}
    responses = [
        MockResponse({"items": [{"id": "a"}], "numberReturned": 1}),
        MockResponse({"items": [], "numberReturned": 0}),
    ]

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        captured["headers"].append(headers)
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)

    df = download.attributes(page_size=100)
    assert len(df) == 1
    assert captured["headers"][0]["x-api-key"] == "test-api-key"


def test_download_api_key_auth_failure(monkeypatch):
    """Auth failures from protected routes propagate as HTTP errors."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com", api_key="bad-key")

    captured = {"headers": []}

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        captured["headers"].append(headers)
        return MockResponse({}, http_error=requests.HTTPError("401 Client Error"))

    monkeypatch.setattr(requests, "get", mock_get)

    with pytest.raises(requests.HTTPError):
        download.attributes(page_size=100)

    assert captured["headers"][0]["x-api-key"] == "bad-key"


def test_fetch_paginated_items_next_link_pagination(monkeypatch):
    """Pagination prefers rel=next links when present."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com")

    calls = []
    responses = [
        MockResponse(
            {
                "items": [{"id": "1"}],
                "numberReturned": 1,
                "links": [
                    {
                        "rel": "next",
                        "href": "https://api.example.com/collections/attributes/items?offset=1&limit=1",
                    }
                ],
            }
        ),
        MockResponse({"items": [{"id": "2"}], "numberReturned": 1, "links": []}),


    ]

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        calls.append((url, params))
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)

    items = download._fetch_paginated_items(
        endpoint="collections/attributes/items",
        params={"type": "categorical"},
        page_size=1,
    )

    assert len(items) == 2
    assert calls[0][1]["type"] == "categorical"
    assert calls[1][1]["type"] == "categorical"


def test_fetch_paginated_items_next_link_preserves_list_filters(monkeypatch):
    """Next-link pagination keeps original repeated filter params intact."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com")

    calls = []
    responses = [
        MockResponse(
            {
                "items": [{"id": "1"}],
                "numberReturned": 1,
                "links": [
                    {
                        "rel": "next",
                        "href": (
                            "https://api.example.com/collections/attributes/items?"
                            "primary_location_id=a&primary_location_id=b&offset=1&limit=1"
                        ),
                    }
                ],
            }
        ),
        MockResponse({"items": [{"id": "2"}], "numberReturned": 1, "links": []}),
    ]

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        calls.append((url, params))
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)

    items = download._fetch_paginated_items(
        endpoint="collections/attributes/items",
        params={"primary_location_id": ["a", "b"]},
        page_size=1,
    )

    assert [i["id"] for i in items] == ["1", "2"]
    assert calls[0][1]["primary_location_id"] == ["a", "b"]
    assert calls[1][1]["primary_location_id"] == ["a", "b"]
    assert calls[1][1]["offset"] == 1
    assert calls[1][1]["limit"] == "1"


def test_fetch_paginated_items_offset_fallback(monkeypatch):
    """When links are absent, pagination falls back to offset progression."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com")

    calls = []
    responses = [
        MockResponse({"items": [{"id": "1"}, {"id": "2"}], "numberReturned": 2}),
        MockResponse({"items": [{"id": "3"}], "numberReturned": 1}),
        MockResponse({"items": [], "numberReturned": 0}),
    ]

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        calls.append((url, params))
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)

    items = download._fetch_paginated_items(
        endpoint="collections/attributes/items",
        params={"name": "foo"},
        page_size=100,
    )

    assert [i["id"] for i in items] == ["1", "2", "3"]
    assert calls[0][1]["offset"] == 0
    assert calls[1][1]["offset"] == 2
    assert calls[2][1]["offset"] == 3


def test_fetch_paginated_items_clamped_first_page(monkeypatch):
    """Client does not truncate when server clamps first-page size."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com")

    calls = []
    responses = [
        MockResponse(
            {
                "items": [{"id": "1"}, {"id": "2"}],
                "numberReturned": 2,
                "links": [
                    {
                        "rel": "next",
                        "href": "https://api.example.com/collections/attributes/items?offset=2&limit=2",
                    }
                ],
            }
        ),
        MockResponse({"items": [{"id": "3"}], "numberReturned": 1, "links": []}),
    ]

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        calls.append((url, params))
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)

    items = download._fetch_paginated_items(
        endpoint="collections/attributes/items",
        params={"name": "foo"},
        page_size=100,
    )

    assert [i["id"] for i in items] == ["1", "2", "3"]
    assert len(calls) == 2


def test_fetch_paginated_items_default_omits_limit(monkeypatch):
    """Default page sizing defers to API by not sending a limit param."""
    ev = SimpleNamespace(_load=None)
    download = Download(ev)
    download.configure(api_base_url="https://api.example.com")

    calls = []
    responses = [
        MockResponse({"items": [{"id": "1"}], "numberReturned": 1}),
        MockResponse({"items": [], "numberReturned": 0}),
    ]

    def mock_get(url, params=None, headers=None, verify=None, timeout=None):
        calls.append((url, params))
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", mock_get)

    items = download._fetch_paginated_items(
        endpoint="collections/attributes/items",
        params={"name": "foo"},
        page_size=None,
    )

    assert [i["id"] for i in items] == ["1"]
    assert "limit" not in calls[0][1]
