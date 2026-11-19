"""Signing in: making a personal token with the device flow (ADR-0018).

:func:`sign_in` shows the user a code and a link, opens their browser, and polls
until they approve; the token it returns is theirs. ``teehr auth login`` is this
plus saving the token.
"""

from __future__ import annotations

import contextlib
import time
import webbrowser
from collections.abc import Callable
from typing import Any

from teehr.client import Client, Pending, TeehrError


class SignInFailed(TeehrError):
    """The sign-in wasn't approved in time, or was denied."""


def sign_in(
    client: Client,
    name: str,
    *,
    scopes: list[str] | None = None,
    projects: list[str] | None = None,
    expires_in_days: int | None = None,
    open_browser: bool = True,
    show: Callable[[str], None] = print,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> dict[str, Any]:
    """Make a personal token for whoever approves the sign-in.

    Returns the token, its secret in ``token``. ``show`` tells the user what to do.
    """
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    flow = client.start_sign_in(
        name, scopes=scopes, projects=projects, expires_in_days=expires_in_days
    )
    show(f"Your code: {flow.user_code}")
    show(f"Approve it at: {flow.verification_uri_complete}")
    if open_browser:
        with contextlib.suppress(webbrowser.Error):
            webbrowser.open(flow.verification_uri_complete)
    show("Waiting for approval (check the code matches)...")
    interval, deadline = float(flow.interval), clock() + flow.expires_in
    while clock() < deadline:
        sleep(interval)
        try:
            result = client.poll_sign_in(flow.poll)
        except TeehrError as exc:
            raise SignInFailed(exc.status, exc.detail) from None
        if isinstance(result, Pending):
            if result.slow_down:
                interval += 5
            continue
        return result
    raise SignInFailed(408, "The sign-in wasn't approved in time; try again")
