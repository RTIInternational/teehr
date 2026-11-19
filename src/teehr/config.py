"""Where the CLI keeps its personal tokens (ADR-0018).

One token per API URL, in ``credentials.json`` in the user's config directory
(``$TEEHR_CONFIG_DIR``, else ``$XDG_CONFIG_HOME/teehr``, else ``~/.config/teehr``),
readable by them only. The API last signed in to is the default.

``TEEHR_API_URL`` and ``TEEHR_TOKEN`` override what's saved (CI, containers).
"""

from __future__ import annotations

import contextlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Credentials:
    """A personal token for one API."""

    api_url: str
    token: str
    token_id: str | None = None
    name: str | None = None
    username: str | None = None
    expires_at: str | None = None
    verify: bool = True


def config_dir() -> Path:
    """Return the directory the CLI's files live in."""
    if os.environ.get("TEEHR_CONFIG_DIR"):
        return Path(os.environ["TEEHR_CONFIG_DIR"])
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "teehr"


def _file() -> Path:
    return config_dir() / "credentials.json"


def _load() -> dict[str, Any]:
    try:
        data: dict[str, Any] = json.loads(_file().read_text())
    except (FileNotFoundError, ValueError):
        return {"default": None, "apis": {}}
    data.setdefault("apis", {})
    return data


def _save(data: dict[str, Any]) -> None:
    path = _file()
    path.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        path.parent.chmod(0o700)
    tmp = path.with_suffix(".tmp")
    # Created readable by the user only, then moved into place.
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    tmp.replace(path)


def normalize(api_url: str) -> str:
    """Return the form an API URL is saved under."""
    return api_url.strip().rstrip("/")


def default_api() -> str | None:
    """Return the API to use when none is given."""
    env = os.environ.get("TEEHR_API_URL")
    if env:
        return normalize(env)
    default: str | None = _load().get("default")
    return default


def credentials(api_url: str | None = None) -> Credentials | None:
    """Return the token for ``api_url`` (default: :func:`default_api`), or None."""
    url = normalize(api_url) if api_url else default_api()
    env_token = os.environ.get("TEEHR_TOKEN")
    if url and env_token:
        return Credentials(api_url=url, token=env_token)
    if url is None:
        return None
    saved = _load()["apis"].get(url)
    return Credentials(api_url=url, **saved) if saved else None


def remember(creds: Credentials) -> None:
    """Save a token, and make its API the default."""
    data = _load()
    fields = asdict(creds)
    url = normalize(fields.pop("api_url"))
    data["apis"][url] = fields
    data["default"] = url
    _save(data)


def forget(api_url: str) -> None:
    """Drop the saved token for an API."""
    data = _load()
    url = normalize(api_url)
    data["apis"].pop(url, None)
    if data.get("default") == url:
        data["default"] = next(iter(data["apis"]), None)
    _save(data)
