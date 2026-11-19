"""The ``teehr`` command: TEEHR Cloud from the terminal, on the SDK."""

from __future__ import annotations

import functools
import platform
from collections.abc import Callable
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from teehr import __version__, config
from teehr.client import Client, NotSignedIn, TeehrError
from teehr.login import sign_in

app = typer.Typer(help="TEEHR Cloud from the terminal.", no_args_is_help=True)
auth_app = typer.Typer(help="Sign in, check who you are, sign out.", no_args_is_help=True)
tokens_app = typer.Typer(help="Your personal tokens.", no_args_is_help=True)
projects_app = typer.Typer(help="Projects you can use.", no_args_is_help=True)
app.add_typer(auth_app, name="auth")
app.add_typer(tokens_app, name="tokens")
app.add_typer(projects_app, name="projects")

out = Console()
err = Console(stderr=True)

API = typer.Option(
    None, "--api", envvar="TEEHR_API_URL", help="The API (default: the last you signed in to)"
)


def friendly[F: Callable[..., Any]](fn: F) -> F:
    """Report API errors as one line on stderr and exit code 1, not a traceback."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except TeehrError as exc:
            err.print(f"[red]{exc.detail}[/red]")
            raise typer.Exit(1) from None

    return wrapper  # type: ignore[return-value]


def _client(api: str | None) -> Client:
    return Client.from_config(api)


@app.callback(invoke_without_command=True)
def main(
    version: bool = typer.Option(False, "--version", help="Show the version and exit."),
) -> None:
    """TEEHR Cloud from the terminal."""
    if version:
        out.print(__version__)
        raise typer.Exit()


# --- auth ---------------------------------------------------------------------------


@auth_app.command("login")
@friendly
def login(
    api: str | None = API,
    name: str = typer.Option(None, help="A name for the token (default: this computer's name)"),
    days: int = typer.Option(90, min=1, max=365, help="Days until the token expires"),
    read_only: bool = typer.Option(False, "--read-only", help="A token that can't write"),
    project: list[str] = typer.Option(
        None, "--project", help="Only this project (repeat for more; default: all of yours)"
    ),
    token: str = typer.Option(
        None, "--token", envvar=None, help="Save an existing personal token instead (CI)"
    ),
    no_browser: bool = typer.Option(False, "--no-browser", help="Don't open the browser"),
    insecure: bool = typer.Option(
        False, "--insecure", help="Don't check the TLS certificate (local clusters only)"
    ),
) -> None:
    """Sign in: approve in your browser, and a personal token is saved for this API."""
    url = api or config.default_api()
    if not url:
        err.print("Which API? Pass --api https://control-plane.<your TEEHR Cloud host>")
        raise typer.Exit(2)
    url = config.normalize(url)
    client = Client(url, verify=not insecure)
    if token is None:
        made = sign_in(
            client,
            name or platform.node() or "teehr-cli",
            scopes=["read"] if read_only else ["read", "write"],
            projects=project or None,
            expires_in_days=days,
            open_browser=not no_browser,
            show=out.print,
        )
        token = made["token"]
    client.token = token
    current = client.current_token() or {}
    config.remember(
        config.Credentials(
            api_url=url,
            token=token,
            token_id=current.get("id"),
            name=current.get("name"),
            username=current.get("username"),
            expires_at=current.get("expires_at"),
            verify=not insecure,
        )
    )
    who = current.get("username") or "you"
    out.print(f"Signed in to {url} as [bold]{who}[/bold] (token '{current.get('name')}', "
              f"expires {str(current.get('expires_at', '?'))[:10]}).")  # fmt: skip


@auth_app.command("status")
@friendly
def status(api: str | None = API) -> None:
    """Who you're signed in as, and with which token."""
    client = _client(api)
    current = client.current_token()
    if current is None:
        out.print(f"Signed in to {client.api_url} with a token that isn't a personal token.")
        return
    scopes = ", ".join(current["scopes"])
    projects = ", ".join(current["projects"]) if current["projects"] else "all yours"
    out.print(f"[bold]{current['username']}[/bold] at {client.api_url}")
    out.print(f"token '{current['name']}' ({current['hint']}...): {scopes}; projects: {projects}")
    out.print(f"expires {current['expires_at'][:10]}")


@auth_app.command("logout")
@friendly
def logout(api: str | None = API) -> None:
    """Revoke this computer's token and forget it."""
    try:
        client = _client(api)
    except NotSignedIn:
        out.print("Not signed in.")
        return
    try:
        current = client.current_token()
        if current is not None:
            client.revoke_token(current["id"])
    except TeehrError as exc:
        if exc.status != 401:  # already revoked or expired: just forget it
            raise
    config.forget(client.api_url)
    out.print(f"Signed out of {client.api_url}.")


# --- tokens ---------------------------------------------------------------------------


@tokens_app.command("list")
@friendly
def tokens_list(
    api: str | None = API,
    all: bool = typer.Option(False, "--all", help="Everyone's (admins)"),
) -> None:
    """Your personal tokens."""
    for t in _client(api).list_tokens(all=all):
        state = (
            " [red](revoked)[/red]"
            if t["revoked_at"]
            else (" [green](this one)[/green]" if t["current"] else "")
        )
        used = (t["last_used_at"] or "never")[:16].replace("T", " ")
        who = f"{t['username']}, " if all else ""
        # One line each, id first and whole: it's what `teehr tokens revoke` takes.
        out.print(
            f"{t['id']}  [bold]{t['name']}[/bold]{state}  ({who}{','.join(t['scopes'])}; "
            f"expires {t['expires_at'][:10]}; last used {used})",
            soft_wrap=True,
        )


@tokens_app.command("revoke")
@friendly
def tokens_revoke(token_id: str, api: str | None = API) -> None:
    """Revoke a personal token by id (see `teehr tokens list`)."""
    _client(api).revoke_token(token_id)
    out.print(f"Revoked {token_id}.")


# --- projects -----------------------------------------------------------------------


@projects_app.command("list")
@friendly
def projects_list(api: str | None = API) -> None:
    """Projects you can use."""
    table = Table("name", "role", "status", "description")
    for p in _client(api).list_projects():
        table.add_row(
            p["name"], str(p.get("my_role") or ""), str(p.get("status") or ""),
            str(p.get("description") or ""),
        )  # fmt: skip
    out.print(table)
