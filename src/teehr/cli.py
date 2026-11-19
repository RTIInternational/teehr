"""The ``teehr`` command: TEEHR Cloud from the terminal, on the SDK.

Commands are grouped by resource (``teehr tokens list``, ``teehr projects list``).
``--output json`` (or ``csv``) gives machine-readable output everywhere; notes go to
stderr so it stays parseable. The patterns follow VeriRace's CLI (ADR-0011).
"""

from __future__ import annotations

import csv
import json
import platform
import sys
from enum import StrEnum
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from teehr import __version__, config
from teehr.client import Client, NotSignedIn, TeehrError
from teehr.login import sign_in

app = typer.Typer(help="TEEHR Cloud from the terminal.", no_args_is_help=True)
auth = typer.Typer(help="Sign in, check who you are, sign out.", no_args_is_help=True)
tokens = typer.Typer(help="Personal tokens (for scripts, CI, agents).", no_args_is_help=True)
projects = typer.Typer(help="Projects you can use.", no_args_is_help=True)
for sub, name in ((auth, "auth"), (tokens, "tokens"), (projects, "projects")):
    app.add_typer(sub, name=name)

console = Console()
#: Status messages, kept off stdout so ``-o json`` output stays parseable.
notes = Console(stderr=True)


class Output(StrEnum):
    """Output format."""

    TABLE = "table"
    JSON = "json"
    CSV = "csv"


class State:
    """Options shared by every command."""

    api_url: str | None = None
    output: Output = Output.TABLE
    client: Client | None = None

    def api(self) -> Client:
        """Return the client for the chosen API, signed in with its saved token."""
        if self.client is None:
            self.client = Client.from_config(self.api_url)
        return self.client


state = State()

#: For project-scoped commands (``TEEHR_PROJECT`` sets a default).
ProjectOpt = Annotated[
    str, typer.Option("--project", "-p", envvar="TEEHR_PROJECT", help="Project name.")
]


@app.callback(invoke_without_command=True)
def main(
    api: Annotated[
        str | None,
        typer.Option(
            "--api",
            envvar="TEEHR_API_URL",
            help="The API, e.g. https://control-plane.<host> (default: the last signed in to).",
        ),
    ] = None,
    output: Annotated[Output, typer.Option("--output", "-o", help="Output format.")] = Output.TABLE,
    version: Annotated[bool, typer.Option("--version", help="Show the version.")] = False,
) -> None:
    """TEEHR Cloud from the terminal."""
    if version:
        console.print(__version__)
        raise typer.Exit()
    state.api_url, state.output, state.client = api, output, None


# -- output ---------------------------------------------------------------------------


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, dict | list):
        return json.dumps(value, default=str)
    return str(value)


def emit(rows: list[dict[str, Any]] | dict[str, Any], columns: list[str] | None = None) -> None:
    """Print records as a table, JSON or CSV (``columns``: which, in order)."""
    if state.output == Output.JSON:
        print(json.dumps(rows, indent=2, default=str))
        return
    if isinstance(rows, dict) and state.output == Output.TABLE:
        for k in columns or list(rows):
            console.print(f"[bold]{k}[/bold]: {escape(_cell(rows.get(k)))}")
        return
    records = [rows] if isinstance(rows, dict) else rows
    cols = columns or (list(records[0]) if records else [])
    if state.output == Output.CSV:
        writer = csv.DictWriter(sys.stdout, cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({c: _cell(r.get(c)) for c in cols} for r in records)
        return
    table = Table()
    for c in cols:
        table.add_column(c, overflow="fold")  # wrap long values, never cut them
    for r in records:
        table.add_row(*(escape(_cell(r.get(c))) for c in cols))  # values aren't markup
    console.print(table)


# -- auth -----------------------------------------------------------------------------


@auth.command("login")
def auth_login(
    name: Annotated[
        str | None, typer.Option(help="A name for the token (default: this computer's).")
    ] = None,
    days: Annotated[int, typer.Option(min=1, max=365, help="Days until it expires.")] = 90,
    read_only: Annotated[
        bool, typer.Option("--read-only", help="A token that can't write.")
    ] = False,
    project: Annotated[
        list[str] | None,
        typer.Option("--project", "-p", help="Only this project (repeatable; default: all yours)."),
    ] = None,
    token: Annotated[
        str | None, typer.Option(help="Save an existing personal token instead (CI).")
    ] = None,
    browser: Annotated[bool, typer.Option(help="Open the approval page in a browser.")] = True,
    insecure: Annotated[
        bool, typer.Option("--insecure", help="Don't check TLS certificates (local clusters).")
    ] = False,
) -> None:
    """Sign in: approve in your browser, and a personal token is saved for this API."""
    url = state.api_url or config.default_api()
    if not url:
        raise typer.BadParameter(
            "which API? pass --api https://control-plane.<your TEEHR Cloud host>",
            param_hint="--api",
        )
    url = config.normalize(url)
    client = Client(url, verify=not insecure)
    if token is None:
        made = sign_in(
            client,
            name or platform.node() or "teehr-cli",
            scopes=["read"] if read_only else ["read", "write"],
            projects=project or None,
            expires_in_days=days,
            open_browser=browser,
            show=notes.print,
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
    notes.print(
        f"Signed in to {url} as [bold]{escape(str(current.get('username')))}[/bold] "
        f"(token {escape(repr(current.get('name')))}, "
        f"expires {str(current.get('expires_at', '?'))[:10]})",
        style="green",
    )


@auth.command("status")
def auth_status() -> None:
    """Show who you're signed in as, and with which token."""
    client = state.api()
    current = client.current_token()
    if current is None:
        notes.print(f"Signed in to {client.api_url}, not with a personal token")
        return
    emit(
        {"api": client.api_url, **current},
        ["api", "username", "name", "hint", "scopes", "projects", "expires_at"],
    )


@auth.command("logout")
def auth_logout() -> None:
    """Revoke this computer's token and forget it."""
    try:
        client = state.api()
    except NotSignedIn:
        notes.print("Not signed in")
        return
    try:
        current = client.current_token()
        if current is not None:
            client.revoke_token(current["id"])
    except TeehrError as exc:
        if exc.status != 401:  # already revoked or expired: just forget it
            raise
    config.forget(client.api_url)
    notes.print(f"Signed out of {client.api_url}", style="green")


# -- tokens ---------------------------------------------------------------------------

#: The table shows these; -o json / csv give every field.
_TOKEN_COLUMNS = ["id", "name", "scopes", "expires_at", "last_used_at", "current"]


@tokens.command("list")
def tokens_list(
    all: Annotated[bool, typer.Option("--all", help="Everyone's (admins).")] = False,
) -> None:
    """List your personal tokens, newest first (``current``: this computer's)."""
    rows = state.api().list_tokens(all=all)
    if state.output == Output.JSON:
        emit(rows)
        return
    for r in rows:
        r["expires_at"] = (r["expires_at"] or "")[:10]
        r["last_used_at"] = (r["last_used_at"] or "")[:16].replace("T", " ")
        if r.get("revoked_at"):
            r["current"] = "revoked"
    emit(rows, ["username", *_TOKEN_COLUMNS] if all else _TOKEN_COLUMNS)


@tokens.command("revoke")
def tokens_revoke(token_id: str) -> None:
    """Revoke a personal token by id (see ``teehr tokens list``)."""
    state.api().revoke_token(token_id)
    notes.print(f"Revoked {token_id}", style="green")


# -- projects -------------------------------------------------------------------------


@projects.command("list")
def projects_list() -> None:
    """List the projects you can use."""
    emit(state.api().list_projects(), ["name", "my_role", "status", "description"])


def run() -> None:
    """Run the CLI with API errors shown as one line, not a traceback."""
    try:
        app()
    except TeehrError as exc:
        Console(stderr=True).print(escape(exc.detail), style="red")
        sys.exit(1)
