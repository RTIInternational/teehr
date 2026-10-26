"""Keycloak access-token refresh, without Spark.

The user's Keycloak access token is short-lived; these helpers refresh it with
the user's refresh token (a standard OAuth2 ``refresh_token`` grant). They are
used by the Spark/Polaris session setup and by any other code that needs a
fresh user token, such as the Icechunk notebook client.

The ``POLARIS_*`` environment variable names come from their first use, Spark
authenticating to Polaris. The tokens themselves are plain Keycloak tokens.
"""
import base64
import json
import os
import time
from typing import Dict, Optional, Tuple

import requests


def _decode_jwt_claims(token: str) -> Dict[str, object]:
    """Decode a JWT's claims without verifying its signature."""
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload.encode()))


def _token_expires_soon(token: str, refresh_window_seconds: int = 120) -> bool:
    """Check whether a token expires within the window, or can't be read."""
    try:
        claims = _decode_jwt_claims(token)
    except Exception:
        return True
    exp = int(claims.get("exp", 0))
    now = int(time.time())
    return exp <= now + max(refresh_window_seconds, 1)


def _request_oauth_tokens(
    data: Dict[str, str],
    token_endpoint: Optional[str] = None,
    timeout_seconds: int = 20,
) -> Tuple[str, Optional[str]]:
    """Post a token request and return the access and refresh tokens."""
    endpoint = token_endpoint or os.getenv("POLARIS_OAUTH2_SERVER_URI")
    if not endpoint:
        raise RuntimeError(
            "POLARIS_OAUTH2_SERVER_URI is required to mint or refresh a user "
            "token"
        )

    resp = requests.post(endpoint, data=data, timeout=timeout_seconds)
    resp.raise_for_status()
    payload = resp.json()

    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("Token endpoint did not return access_token")

    return access_token, payload.get("refresh_token")


def _refresh_user_token(
    refresh_token: str,
    client_id: str,
    client_secret: Optional[str] = None,
    token_endpoint: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """Exchange a refresh token for a new access token.

    Parameters
    ----------
    refresh_token : str
        The user's Keycloak refresh token.
    client_id : str
        Keycloak client ID used for the refresh grant.
    client_secret : str, optional
        Keycloak client secret. Default is None.
    token_endpoint : str, optional
        Override for the OAuth2 token endpoint URL. Defaults to the
        ``POLARIS_OAUTH2_SERVER_URI`` environment variable.

    Returns
    -------
    Tuple[str, Optional[str]]
        ``(access_token, refresh_token)``. The refresh token is None if
        Keycloak didn't return a new one.
    """
    if not refresh_token:
        raise RuntimeError("refresh_token is required for refresh grant")

    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }
    if client_secret:
        data["client_secret"] = client_secret

    return _request_oauth_tokens(data=data, token_endpoint=token_endpoint)


def _ensure_fresh_user_token(
    current_token: Optional[str],
    client_id: str,
    client_secret: Optional[str] = None,
    refresh_token: Optional[str] = None,
    refresh_window_seconds: int = 120,
    token_endpoint: Optional[str] = None,
) -> Tuple[str, Optional[str], bool]:
    """Return a fresh access token, refreshing via refresh_token if needed.

    Parameters
    ----------
    current_token : str, optional
        The current access token. Returned as-is if still valid.
    client_id : str
        Keycloak client ID used for the refresh grant.
    client_secret : str, optional
        Keycloak client secret. Default is None.
    refresh_token : str, optional
        Refresh token used to acquire a new access token. Required when the
        current token is expired or absent.
    refresh_window_seconds : int
        Seconds before expiry at which the token is considered stale and
        proactively refreshed. Default is 120.
    token_endpoint : str, optional
        Override for the OAuth2 token endpoint URL. Defaults to the
        ``POLARIS_OAUTH2_SERVER_URI`` environment variable.

    Returns
    -------
    Tuple[str, Optional[str], bool]
        ``(access_token, refresh_token, was_refreshed)``.

    Raises
    ------
    RuntimeError
        If the token is expired and no valid refresh_token is available.
    """
    if current_token and not _token_expires_soon(
        current_token, refresh_window_seconds
    ):
        return current_token, refresh_token, False

    if refresh_token:
        refreshed_access, refreshed_refresh = _refresh_user_token(
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint=token_endpoint,
        )
        return refreshed_access, (refreshed_refresh or refresh_token), True

    raise RuntimeError(
        "Unable to obtain a fresh user token: no valid refresh_token "
        "available. Set POLARIS_REFRESH_TOKEN in the session environment."
    )


def refresh_user_token_from_env(
    current_token: Optional[str] = None,
    refresh_window_seconds: int = 120,
) -> Tuple[str, Optional[str]]:
    """Return a fresh access token for the session's user, updating the env.

    Reads ``POLARIS_USER_TOKEN``, ``POLARIS_REFRESH_TOKEN``,
    ``POLARIS_CLIENT_ID`` (default ``jupyterhub``), ``POLARIS_CLIENT_SECRET``
    and the optional ``POLARIS_OAUTH2_TOKEN_ENDPOINT``, refreshes the access
    token if it expires within ``refresh_window_seconds``, and writes the new
    tokens back to the environment.

    Parameters
    ----------
    current_token : str, optional
        Access token to use instead of ``POLARIS_USER_TOKEN``.
    refresh_window_seconds : int
        Seconds before expiry at which the token is refreshed. Default is 120.

    Returns
    -------
    Tuple[str, Optional[str]]
        ``(access_token, refresh_token)``.

    Raises
    ------
    RuntimeError
        If the token is expired and no valid refresh token is available.
    """
    access_token, refresh_token, _ = _ensure_fresh_user_token(
        current_token=current_token or os.getenv("POLARIS_USER_TOKEN", ""),
        client_id=os.getenv("POLARIS_CLIENT_ID", "jupyterhub"),
        client_secret=os.getenv("POLARIS_CLIENT_SECRET"),
        refresh_token=os.getenv("POLARIS_REFRESH_TOKEN", ""),
        refresh_window_seconds=refresh_window_seconds,
        token_endpoint=os.getenv("POLARIS_OAUTH2_TOKEN_ENDPOINT"),
    )
    os.environ["POLARIS_USER_TOKEN"] = access_token
    if refresh_token:
        os.environ["POLARIS_REFRESH_TOKEN"] = refresh_token
    return access_token, refresh_token
