"""The SDK: a Python client for TEEHR Cloud's API (the control plane).

The CLI is a thin layer on this, so anything it does is also a Python call::

    from teehr import Client

    client = Client.from_config()  # the token `teehr auth login` saved
    client.list_projects()

A client authenticates with a personal token (``tpat_...``, made by
``teehr auth login`` or :meth:`Client.start_sign_in`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from teehr import config

USER_AGENT = "teehr-sdk"


class TeehrError(Exception):
    """The API refused a request; ``status`` is its HTTP status, ``detail`` why."""

    def __init__(self, status: int, detail: str):
        super().__init__(f"{detail} (HTTP {status})")
        self.status = status
        self.detail = detail


class NotSignedIn(TeehrError):
    """There's no token for this API: sign in with ``teehr auth login``."""

    def __init__(self, api_url: str | None):
        where = f" for {api_url}" if api_url else ""
        super().__init__(401, f"Not signed in{where}; run `teehr auth login`")


@dataclass(frozen=True)
class SignIn:
    """A sign-in waiting for the user to approve it in their browser."""

    user_code: str
    verification_uri: str
    verification_uri_complete: str
    interval: int
    expires_in: int
    poll: str  # a short-lived secret: whoever holds it collects the token


@dataclass(frozen=True)
class Pending:
    """Not approved yet; ``slow_down``: poll less often."""

    slow_down: bool = False


def _detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return resp.text[:300] or resp.reason_phrase
    if isinstance(body, dict):
        errors = body.get("errors")
        if errors:
            return "; ".join(
                f"{'.'.join(str(p) for p in e.get('loc', [])[1:])}: {e.get('msg')}" for e in errors
            )
        return str(body.get("detail") or body.get("title") or resp.reason_phrase)
    return resp.reason_phrase


class Client:
    """TEEHR Cloud's API, as the user a personal token belongs to.

    Parameters
    ----------
    api_url
        The control plane's address, e.g. ``https://control-plane.example.org``.
    token
        A personal token (``tpat_...``); not needed to sign in.
    verify
        Check the server's TLS certificate (turn off only for a local cluster
        with a self-signed one).
    http
        An ``httpx.Client`` to use instead of a new one (tests).
    """

    def __init__(
        self,
        api_url: str,
        token: str | None = None,
        *,
        verify: bool = True,
        http: httpx.Client | None = None,
        timeout: float = 60.0,
    ):
        self.api_url = api_url.rstrip("/")
        self.token = token
        self._http = http or httpx.Client(
            verify=verify, timeout=timeout, headers={"User-Agent": USER_AGENT}
        )

    @classmethod
    def from_config(cls, api_url: str | None = None) -> Client:
        """Make a client with the saved token for ``api_url``.

        The default is the API last signed in to. ``TEEHR_API_URL`` and
        ``TEEHR_TOKEN`` override the saved ones (CI).
        """
        saved = config.credentials(api_url)
        if saved is None:
            raise NotSignedIn(api_url)
        return cls(saved.api_url, saved.token, verify=saved.verify)

    def _request(
        self, method: str, path: str, *, auth: bool = True, **kwargs: Any
    ) -> httpx.Response:
        headers = dict(kwargs.pop("headers", None) or {})
        if auth:
            if not self.token:
                raise NotSignedIn(self.api_url)
            headers["Authorization"] = f"Bearer {self.token}"
        resp = self._http.request(method, f"{self.api_url}{path}", headers=headers, **kwargs)
        if resp.status_code >= 400:
            raise TeehrError(resp.status_code, _detail(resp))
        return resp

    # --- signing in: making a personal token (no token needed) -------------------

    def start_sign_in(
        self,
        name: str,
        *,
        scopes: list[str] | None = None,
        projects: list[str] | None = None,
        expires_in_days: int | None = None,
    ) -> SignIn:
        """Start making a personal token.

        The user approves it in their browser (``verification_uri_complete``),
        then :meth:`poll_sign_in` returns it.
        """
        body: dict[str, Any] = {"name": name}
        if scopes is not None:
            body["scopes"] = scopes
        if projects is not None:
            body["projects"] = projects
        if expires_in_days is not None:
            body["expires_in_days"] = expires_in_days
        data = self._request("POST", "/v1/tokens/device", auth=False, json=body).json()
        return SignIn(**{k: data[k] for k in SignIn.__dataclass_fields__})

    def poll_sign_in(self, poll: str) -> dict[str, Any] | Pending:
        """Return the new token, or :class:`Pending` until the user approves.

        The token's secret is in ``token``, returned this once.
        """
        resp = self._request("POST", "/v1/tokens/device/poll", auth=False, json={"poll": poll})
        if resp.status_code == 202:
            return Pending(slow_down=bool(resp.json().get("slow_down")))
        made: dict[str, Any] = resp.json()
        return made

    # --- personal tokens -----------------------------------------------------------

    def list_tokens(self, all: bool = False) -> list[dict[str, Any]]:
        """List your personal tokens, newest first.

        ``all``: everyone's, for admins. ``current`` marks the one this client uses.
        """
        params = {"all": "true"} if all else None
        tokens: list[dict[str, Any]] = self._request("GET", "/v1/tokens", params=params).json()
        return tokens

    def current_token(self) -> dict[str, Any] | None:
        """Return the personal token this client uses, as the API lists it."""
        return next((t for t in self.list_tokens() if t.get("current")), None)

    def revoke_token(self, token_id: str) -> None:
        """Revoke a personal token; jobs it started stop being able to act as you."""
        self._request("DELETE", f"/v1/tokens/{token_id}")

    # --- projects ------------------------------------------------------------------

    def list_projects(self) -> list[dict[str, Any]]:
        """List the projects you can use."""
        projects: list[dict[str, Any]] = self._request("GET", "/v1/projects").json()
        return projects
