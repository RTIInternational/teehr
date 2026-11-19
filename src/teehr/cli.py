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
from pathlib import Path
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
uploads = typer.Typer(
    help="Load files into datasets: upload, check the proposed mapping, ingest.",
    no_args_is_help=True,
)
for sub, name in (
    (auth, "auth"),
    (tokens, "tokens"),
    (projects, "projects"),
    (uploads, "uploads"),
):
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


# -- uploads --------------------------------------------------------------------------

_UPLOAD_COLUMNS = ["id", "filename", "kind", "status", "rows_done", "created_at"]
KindOpt = Annotated[
    str,
    typer.Option(
        help="What the file holds: timeseries, layer (locations with geometry) or "
        "crosswalk (series keys linked across two datasets)."
    ),
]
NameOpt = Annotated[str | None, typer.Option("--name", help="Dataset name (new or existing).")]
RoleOpt = Annotated[str | None, typer.Option(help="New timeseries: observed, simulated, forecast.")]
VariableOpt = Annotated[str | None, typer.Option(help="New timeseries: e.g. streamflow.")]
UnitOpt = Annotated[
    str | None, typer.Option(help="Unit of the file's values (UCUM, e.g. '[cft_i]/s'); converted.")
]
StatisticOpt = Annotated[
    str | None, typer.Option(help="New timeseries: inst, or mean, sum, min, max over a period.")
]
PeriodOpt = Annotated[str | None, typer.Option(help="New timeseries: e.g. hour_starting.")]
PartitionOpt = Annotated[
    str | None,
    typer.Option("--partition", help="New timeseries: partition by month (default), year or none."),
]
LayerOpt = Annotated[
    str | None, typer.Option("--layer", help="Timeseries: the layer its series' locations are in.")
]
LeftOpt = Annotated[
    str | None, typer.Option(help="New crosswalk: the dataset its left keys are series of.")
]
RightOpt = Annotated[
    str | None, typer.Option(help="New crosswalk: the dataset its right keys are series of.")
]
SourceOpt = Annotated[str | None, typer.Option(help="Where the data came from.")]
DescriptionOpt = Annotated[str | None, typer.Option(help="What the dataset is.")]
TimezoneOpt = Annotated[
    str | None,
    typer.Option(help="IANA zone of times without one, e.g. America/New_York (no default)."),
]
MapOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--map",
        help="Change the proposed mapping, FIELD=VALUE (repeatable): e.g. value=flow_cfs, "
        "series_key=site_no, layout=wide, series_columns=a,b; JSON for lists of objects, "
        'e.g. \'attributes=[{"column": "huc"}]\'.',
    ),
]
WaitOpt = Annotated[bool, typer.Option("--wait", help="Wait for the load to finish.")]


def _dataset_opts(**given: str | None) -> dict[str, Any]:
    names = {"layer": "location_layer", "partition": "time_partition"}
    return {names.get(k, k): v for k, v in given.items() if v}


def _mapping_opts(items: list[str] | None, timezone: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {"timezone": timezone} if timezone else {}
    for item in items or []:
        key, sep, value = item.partition("=")
        if not sep:
            raise typer.BadParameter(f"{item!r}: use FIELD=VALUE", param_hint="--map")
        if value.startswith(("[", "{")):
            try:
                out[key] = json.loads(value)
            except json.JSONDecodeError as exc:
                raise typer.BadParameter(f"{key}: not JSON ({exc})", param_hint="--map") from exc
        else:
            out[key] = value.split(",") if key == "series_columns" else (value or None)
    return out


#: What gives each of inspect's ``needed`` fields (others: --map FIELD=...).
_NEEDED_OPTION = {
    "mapping.timezone": "--timezone ZONE",
    "dataset.location_layer": "--layer LAYER",
    "dataset.time_partition": "--partition month|year|none",
}


def _option_for(needed: str) -> str:
    field = needed.split(" ", 1)[0]
    if field in _NEEDED_OPTION:
        return _NEEDED_OPTION[field]
    part, _, key = field.partition(".")
    if part == "dataset":
        return f"--{key.replace('_', '-')} ..."
    return f"--map {key}=..."


def _show_preview(upload: dict[str, Any]) -> None:
    if state.output == Output.JSON:
        emit(upload)
        return
    preview = upload.get("preview") or {}
    rows = preview.get("rows")
    console.print(
        f"Upload [bold]{upload['id']}[/bold] ({escape(upload['filename'])}"
        + (f", {rows} rows" if rows is not None else "")
        + f"): {upload['status']}"
    )
    mapping = {k: v for k, v in (preview.get("mapping") or {}).items() if v}
    dataset = {k: v for k, v in (preview.get("dataset") or {}).items() if v}
    console.print(
        "Proposed mapping: " + escape(", ".join(f"{k}={_cell(v)}" for k, v in mapping.items()))
    )
    console.print(
        "Proposed dataset: " + escape(", ".join(f"{k}={_cell(v)}" for k, v in dataset.items()))
    )
    for note in preview.get("notes") or []:
        console.print(f"  note: {escape(note)}", style="yellow")
    for need in preview.get("needed") or []:
        console.print(f"  needed: {escape(need)}  ({_option_for(need)})", style="red")


def _waited(project: str, upload: dict[str, Any], wait: bool) -> dict[str, Any]:
    """Return the upload, after waiting for its load to end if ``wait``."""
    if not wait:
        return upload
    with notes.status(f"upload {upload['id']}: {upload['status']}") as status:
        return state.api().wait_upload(
            project,
            upload["id"],
            lambda u: status.update(f"upload {u['id']}: {u['status']}, {u['rows_done']} rows"),
        )


def _outcome(upload: dict[str, Any]) -> None:
    """Print a load's outcome (table mode; JSON prints the whole upload once)."""
    emit({k: upload.get(k) for k in ("id", "status", "rows_done", "result", "problems", "error")})


def _finish(project: str, upload: dict[str, Any], wait: bool) -> None:
    upload = _waited(project, upload, wait)
    if state.output == Output.JSON:
        emit(upload)
    else:
        _outcome(upload)
    if upload["status"] in ("failed", "cancelled"):
        raise typer.Exit(1)


@uploads.command("add")
def uploads_add(
    files: Annotated[list[Path], typer.Argument(exists=True, dir_okay=False)],
    project: ProjectOpt,
    kind: KindOpt,
    name: NameOpt = None,
    role: RoleOpt = None,
    variable: VariableOpt = None,
    unit: UnitOpt = None,
    statistic: StatisticOpt = None,
    period: PeriodOpt = None,
    partition: PartitionOpt = None,
    layer: LayerOpt = None,
    left: LeftOpt = None,
    right: RightOpt = None,
    source: SourceOpt = None,
    description: DescriptionOpt = None,
    timezone: TimezoneOpt = None,
    map_: MapOpt = None,
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Ingest right away (if nothing is missing).")
    ] = False,
    wait: WaitOpt = False,
) -> None:
    """Upload files and show how they map; with --yes, load them too.

    Without --yes, check the proposed mapping, then `teehr uploads ingest ID` with any
    changes. With several files and --yes, each loads after the one before (give
    --name to load them all into one dataset).
    """
    api = state.api()
    dataset = _dataset_opts(
        name=name, role=role, variable=variable, unit=unit, statistic=statistic,
        period=period, partition=partition, layer=layer, left=left, right=right,
        source=source, description=description,
    )  # fmt: skip
    mapping = _mapping_opts(map_, timezone)
    json_out = state.output == Output.JSON
    done: list[dict[str, Any]] = []  # JSON: one document at the end
    for file in files:
        upload = api.upload_file(project, file, kind)
        if not json_out:
            _show_preview(upload)
        if yes:
            started = api.ingest_upload(project, upload["id"], dataset, mapping)
            upload = _waited(project, started, wait or len(files) > 1)
            if not json_out:
                _outcome(upload)
        elif not json_out:
            notes.print(f"Load it with: teehr uploads ingest {upload['id']} -p {project} [options]")
        done.append(upload)
        if upload["status"] in ("failed", "cancelled"):
            break  # don't load the rest after one that didn't
    if json_out:
        emit(done[0] if len(files) == 1 else done)
    if done[-1]["status"] in ("failed", "cancelled"):
        raise typer.Exit(1)


@uploads.command("ingest")
def uploads_ingest(
    upload_id: str,
    project: ProjectOpt,
    name: NameOpt = None,
    role: RoleOpt = None,
    variable: VariableOpt = None,
    unit: UnitOpt = None,
    statistic: StatisticOpt = None,
    period: PeriodOpt = None,
    partition: PartitionOpt = None,
    layer: LayerOpt = None,
    left: LeftOpt = None,
    right: RightOpt = None,
    source: SourceOpt = None,
    description: DescriptionOpt = None,
    timezone: TimezoneOpt = None,
    map_: MapOpt = None,
    wait: WaitOpt = False,
) -> None:
    """Load an uploaded file, confirming (or changing) the proposed mapping and dataset."""
    upload = state.api().ingest_upload(
        project,
        upload_id,
        _dataset_opts(
            name=name, role=role, variable=variable, unit=unit, statistic=statistic,
            period=period, partition=partition, layer=layer, left=left, right=right,
            source=source, description=description,
        ),
        _mapping_opts(map_, timezone),
    )  # fmt: skip
    _finish(project, upload, wait)


@uploads.command("list")
def uploads_list(project: ProjectOpt) -> None:
    """List a project's uploads, newest first."""
    emit(state.api().list_uploads(project), _UPLOAD_COLUMNS)


@uploads.command("show")
def uploads_show(upload_id: str, project: ProjectOpt) -> None:
    """Show an upload: proposed mapping, status, progress, outcome and problems."""
    upload = state.api().get_upload(project, upload_id)
    if upload.get("preview") and state.output == Output.TABLE:
        _show_preview(upload)
    emit(
        upload
        if state.output == Output.JSON
        else {k: upload.get(k) for k in ("status", "rows_done", "result", "problems", "error")}
    )


@uploads.command("wait")
def uploads_wait(upload_id: str, project: ProjectOpt) -> None:
    """Wait for an upload's load to finish."""
    _finish(project, state.api().get_upload(project, upload_id), wait=True)


@uploads.command("cancel")
def uploads_cancel(upload_id: str, project: ProjectOpt) -> None:
    """Stop a queued or running load (the upload can be ingested again)."""
    emit(state.api().cancel_upload(project, upload_id), ["id", "status"])


@uploads.command("delete")
def uploads_delete(
    upload_id: str,
    project: ProjectOpt,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Don't ask first.")] = False,
) -> None:
    """Delete an upload and its staged file (not data already loaded)."""
    if not yes:
        typer.confirm(f"Delete upload {upload_id} in {project}?", abort=True)
    state.api().delete_upload(project, upload_id)
    notes.print(f"Deleted upload {upload_id}", style="green")


def run() -> None:
    """Run the CLI with API errors shown as one line, not a traceback."""
    try:
        app()
    except TeehrError as exc:
        Console(stderr=True).print(escape(exc.detail), style="red")
        sys.exit(1)
