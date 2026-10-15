"""Unit tests for the Keycloak token refresh helpers in keycloak_auth."""
import base64
import json
import os
import time

import pytest
import requests

from teehr.utils.keycloak_auth import (
    _token_expires_soon,
    ensure_fresh_user_token,
    refresh_user_token,
    refresh_user_token_from_env,
)


def _jwt(exp_offset_seconds):
    """Build an unsigned JWT that expires ``exp_offset_seconds`` from now."""
    def part(obj):
        encoded = base64.urlsafe_b64encode(json.dumps(obj).encode())
        return encoded.decode().rstrip("=")

    exp = int(time.time()) + exp_offset_seconds
    return f"{part({'alg': 'none'})}.{part({'exp': exp})}.sig"


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


@pytest.fixture
def token_endpoint(monkeypatch):
    """Fake the Keycloak token endpoint; return the requests it received."""
    for key in (
        "POLARIS_USER_TOKEN",
        "POLARIS_REFRESH_TOKEN",
        "POLARIS_CLIENT_ID",
        "POLARIS_CLIENT_SECRET",
        "POLARIS_OAUTH2_TOKEN_ENDPOINT",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("POLARIS_OAUTH2_SERVER_URI", "http://keycloak/token")
    calls = []

    def post(url, data=None, timeout=None):
        calls.append((url, data))
        return _Response(
            {"access_token": "new-access", "refresh_token": "new-refresh"}
        )

    monkeypatch.setattr(requests, "post", post)
    return calls


def test_token_expires_soon():
    """Treat tokens inside the window, or unparseable ones, as expiring."""
    assert _token_expires_soon(_jwt(60), refresh_window_seconds=120)
    assert not _token_expires_soon(_jwt(600), refresh_window_seconds=120)
    assert _token_expires_soon("not-a-jwt")


def test_refresh_user_token_sends_refresh_grant(token_endpoint):
    """The refresh grant carries the client id, secret and refresh token."""
    access, refresh = refresh_user_token(
        "old-refresh", "jupyterhub", client_secret="secret"
    )
    assert (access, refresh) == ("new-access", "new-refresh")
    url, data = token_endpoint[0]
    assert url == "http://keycloak/token"
    assert data == {
        "grant_type": "refresh_token",
        "client_id": "jupyterhub",
        "refresh_token": "old-refresh",
        "client_secret": "secret",
    }


def test_refresh_user_token_requires_refresh_token(token_endpoint):
    """A refresh without a refresh token raises."""
    with pytest.raises(RuntimeError):
        refresh_user_token("", "jupyterhub")


def test_ensure_fresh_user_token_keeps_valid_token(token_endpoint):
    """A token outside the refresh window is returned without a request."""
    current = _jwt(600)
    result = ensure_fresh_user_token(current, "jupyterhub", refresh_token="r")
    assert result == (current, "r", False)
    assert token_endpoint == []


def test_ensure_fresh_user_token_without_refresh_token_raises(token_endpoint):
    """An expired token with no refresh token raises."""
    with pytest.raises(RuntimeError):
        ensure_fresh_user_token(_jwt(-60), "jupyterhub")


def test_from_env_refreshes_and_writes_back(token_endpoint, monkeypatch):
    """An expiring env token is refreshed and both tokens are written back."""
    monkeypatch.setenv("POLARIS_USER_TOKEN", _jwt(60))
    monkeypatch.setenv("POLARIS_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv("POLARIS_CLIENT_SECRET", "secret")

    assert refresh_user_token_from_env() == ("new-access", "new-refresh")
    assert os.environ["POLARIS_USER_TOKEN"] == "new-access"
    assert os.environ["POLARIS_REFRESH_TOKEN"] == "new-refresh"
    _, data = token_endpoint[0]
    assert data["client_id"] == "jupyterhub"
    assert data["client_secret"] == "secret"


def test_from_env_uses_token_endpoint_override(token_endpoint, monkeypatch):
    """POLARIS_OAUTH2_TOKEN_ENDPOINT overrides POLARIS_OAUTH2_SERVER_URI."""
    monkeypatch.setenv("POLARIS_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv(
        "POLARIS_OAUTH2_TOKEN_ENDPOINT", "http://override/token"
    )
    refresh_user_token_from_env()
    assert token_endpoint[0][0] == "http://override/token"


def test_from_env_refresh_window(token_endpoint, monkeypatch):
    """Keep a token expiring in 200 s by default; refresh it with 300 s."""
    current = _jwt(200)
    monkeypatch.setenv("POLARIS_USER_TOKEN", current)
    monkeypatch.setenv("POLARIS_REFRESH_TOKEN", "old-refresh")

    assert refresh_user_token_from_env()[0] == current
    refreshed = refresh_user_token_from_env(refresh_window_seconds=300)
    assert refreshed[0] == "new-access"


def test_from_env_explicit_token_takes_precedence(
    token_endpoint, monkeypatch
):
    """An explicit current_token is used instead of POLARIS_USER_TOKEN."""
    explicit = _jwt(600)
    monkeypatch.setenv("POLARIS_USER_TOKEN", _jwt(-60))

    assert refresh_user_token_from_env(current_token=explicit)[0] == explicit
    assert os.environ["POLARIS_USER_TOKEN"] == explicit
    assert token_endpoint == []
