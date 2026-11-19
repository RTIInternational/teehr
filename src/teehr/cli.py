"""The ``teehr`` command: TEEHR Cloud from the terminal, on the SDK.

Commands are grouped by resource (``teehr tokens list``, ``teehr projects list``).
``--output json`` (or ``csv``) gives machine-readable output everywhere; notes go to
stderr so it stays parseable. The patterns follow VeriRace's CLI (ADR-0011).
"""

from __future__ import annotations

import csv
import functools
import json
import platform
import re
import sys
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import typer
import yaml
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
evaluations = typer.Typer(
    help="Saved evaluations: specs under a name, versioned.", no_args_is_help=True
)
runs = typer.Typer(help="Runs of saved evaluations.", no_args_is_help=True)
datasets = typer.Typer(
    help="Datasets: list, describe, read, create, write, delete.", no_args_is_help=True
)
members = typer.Typer(help="A project's members and their roles.", no_args_is_help=True)
units = typer.Typer(help="Units (UCUM symbols) values can be in.", no_args_is_help=True)
periods = typer.Typer(help="Periods: the span each value covers.", no_args_is_help=True)
variables = typer.Typer(
    help="Variables (e.g. streamflow) and their canonical units.", no_args_is_help=True
)
projects.add_typer(members, name="members")
for sub, name in (
    (auth, "auth"),
    (tokens, "tokens"),
    (projects, "projects"),
    (datasets, "datasets"),
    (uploads, "uploads"),
    (evaluations, "evaluations"),
    (runs, "runs"),
    (variables, "variables"),
    (units, "units"),
    (periods, "periods"),
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
#: For data commands, where the shared public datasets are the default.
DataProjectOpt = Annotated[
    str,
    typer.Option("--project", "-p", envvar="TEEHR_PROJECT", help="Project name, or 'public'."),
]
#: Rows printed unless --limit says otherwise (--out writes every row).
PRINT_LIMIT = 1000
OutOpt = Annotated[
    Path | None, typer.Option("--out", help="Write every row to a .parquet, .csv or .arrow file.")
]
LimitOpt = Annotated[
    int | None,
    typer.Option(
        "--limit", "-n", min=1, help=f"At most this many rows (printing: {PRINT_LIMIT} by default)."
    ),
]
ColumnsOpt = Annotated[
    str | None, typer.Option("--columns", "-c", help="Comma-separated columns (default: all).")
]
FilterOpt = Annotated[
    str | None,
    typer.Option(
        "--filter", "-f", help="Rows to read, e.g. \"primary_series_key = 'usgs-01010000'\"."
    ),
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


_TIMESTAMP = re.compile(r"^(\d{4}-\d\d-\d\d)T(\d\d:\d\d:\d\d)[.\d]*(Z|[+-]00:00)?$")


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and (m := _TIMESTAMP.match(value)):
        return f"{m[1]} {m[2]}"  # UTC, to the second (JSON keeps it whole)
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, dict | list):
        return json.dumps(value, default=str)
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"  # e.g. a geometry (WKB)
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


_PROJECT_FIELDS = ["name", "display_name", "description", "kind", "status", "status_detail",
                   "my_role", "storage_location", "created_by", "created_at"]  # fmt: skip
DisplayNameOpt = Annotated[str | None, typer.Option(help="A longer name to show.")]
ProjectDescriptionOpt = Annotated[str | None, typer.Option(help="What the project is for.")]
YesOpt = Annotated[bool, typer.Option("--yes", "-y", help="Don't ask first.")]


@projects.command("create")
def projects_create(
    name: str, display_name: DisplayNameOpt = None, description: ProjectDescriptionOpt = None
) -> None:
    """Create a project; you become its admin (needs a project-creator role).

    Names are lowercase letters, digits and underscores, starting with a letter.
    """
    emit(state.api().create_project(name, display_name, description), _PROJECT_FIELDS)


@projects.command("show")
def projects_show(name: str) -> None:
    """Show a project: status, storage, your role."""
    emit(state.api().get_project(name), _PROJECT_FIELDS)


@projects.command("update")
def projects_update(
    name: str, display_name: DisplayNameOpt = None, description: ProjectDescriptionOpt = None
) -> None:
    """Change a project's display name or description (admins)."""
    emit(state.api().update_project(name, display_name, description), _PROJECT_FIELDS)


@projects.command("delete")
def projects_delete(name: str, yes: YesOpt = False) -> None:
    """Delete a project with all its datasets, evaluations and runs (admins)."""
    if not yes:
        typer.confirm(f"Delete project {name} and everything in it?", abort=True)
    state.api().delete_project(name)
    notes.print(f"Deleted project {name}", style="green")


@projects.command("provision")
def projects_provision(name: str) -> None:
    """Retry setting up a project's storage and catalog access, if it failed (admins)."""
    emit(state.api().provision_project(name), _PROJECT_FIELDS)


# -- members --------------------------------------------------------------------------

_MEMBER_COLUMNS = ["username", "role", "subject", "added_by", "added_at"]
RoleArg = Annotated[str, typer.Option("--role", help="read, write or admin.")]


@members.command("list")
def members_list(project: ProjectOpt) -> None:
    """List a project's members and their roles."""
    emit(state.api().list_members(project), _MEMBER_COLUMNS)


@members.command("add")
def members_add(username: str, project: ProjectOpt, role: RoleArg) -> None:
    """Add a user to the project with a role (admins)."""
    emit(state.api().add_member(project, username, role), _MEMBER_COLUMNS)


@members.command("set")
def members_set(user: str, project: ProjectOpt, role: RoleArg) -> None:
    """Change a member's role (by username or subject; admins)."""
    emit(state.api().set_member(project, user, role), _MEMBER_COLUMNS)


@members.command("remove")
def members_remove(user: str, project: ProjectOpt, yes: YesOpt = False) -> None:
    """Remove a member from the project (by username or subject; admins)."""
    if not yes:
        typer.confirm(f"Remove {user} from {project}?", abort=True)
    state.api().remove_member(project, user)
    notes.print(f"Removed {user} from {project}", style="green")


# -- units, periods, variables ---------------------------------------------------------

DomainProjectOpt = Annotated[
    str | None,
    typer.Option(
        "--project", "-p", envvar="TEEHR_PROJECT",
        help="Also this project's own (default: the public ones only).",
    ),
]  # fmt: skip
AddToOpt = Annotated[
    str | None,
    typer.Option(
        "--project", "-p",
        help="Add it to this project (default: public, which needs the platform's write role).",
    ),
]  # fmt: skip
DomainDescriptionOpt = Annotated[str | None, typer.Option(help="What it is.")]


def _scoped(rows: list[dict[str, Any]], columns: list[str]) -> None:
    for r in rows:
        r["project"] = r.get("project") or "public"
    emit(rows, [*columns, "project"])


@units.command("list")
def units_list(project: DomainProjectOpt = None) -> None:
    """List units: their dimension and conversion to SI (si = value * to_si + offset)."""
    _scoped(state.api().list_domain("units", project),
            ["symbol", "dimension", "to_si", "offset", "description"])  # fmt: skip


@units.command("add")
def units_add(
    symbol: str,
    dimension: Annotated[str, typer.Option(help="e.g. volume_flow, length, temperature.")],
    to_si: Annotated[float, typer.Option(help="Multiply by this to get SI.")],
    offset: Annotated[float, typer.Option(help="Then add this (temperatures).")] = 0.0,
    description: DomainDescriptionOpt = None,
    project: AddToOpt = None,
) -> None:
    """Add a unit (a UCUM symbol, e.g. '[cft_i]/s')."""
    body = {"symbol": symbol, "dimension": dimension, "to_si": to_si, "offset": offset,
            "description": description, "project": project}  # fmt: skip
    emit(state.api().add_domain("units", {k: v for k, v in body.items() if v is not None}))


@periods.command("list")
def periods_list(project: DomainProjectOpt = None) -> None:
    """List periods: e.g. instant, hour_starting, day_ending."""
    _scoped(state.api().list_domain("periods", project),
            ["name", "form", "length", "value_time_at", "start", "end", "description"])  # fmt: skip


@periods.command("add")
def periods_add(
    name: str,
    form: Annotated[str, typer.Option(help="instant, duration, calendar or absolute.")],
    length: Annotated[str | None, typer.Option(help="Duration: ISO 8601, e.g. PT1H.")] = None,
    value_time_at: Annotated[
        str | None, typer.Option(help="Duration: start or end of the span.")
    ] = None,
    start: Annotated[str | None, typer.Option(help="Absolute: --MM-DD.")] = None,
    end: Annotated[str | None, typer.Option(help="Absolute: --MM-DD.")] = None,
    description: DomainDescriptionOpt = None,
    project: AddToOpt = None,
) -> None:
    """Add a period: the span each value of a dataset covers."""
    body = {"name": name, "form": form, "length": length, "value_time_at": value_time_at,
            "start": start, "end": end, "description": description, "project": project}  # fmt: skip
    emit(state.api().add_domain("periods", {k: v for k, v in body.items() if v is not None}))


@variables.command("list")
def variables_list(project: DomainProjectOpt = None) -> None:
    """List variables and the units their values are stored in."""
    _scoped(state.api().list_domain("variables", project),
            ["name", "canonical_unit", "description"])  # fmt: skip


@variables.command("add")
def variables_add(
    name: str,
    canonical_unit: Annotated[str, typer.Option(help="A unit; its values are stored in it.")],
    description: DomainDescriptionOpt = None,
    project: AddToOpt = None,
) -> None:
    """Add a variable, e.g. streamflow, with the unit its values are stored in."""
    body = {"name": name, "canonical_unit": canonical_unit, "description": description,
            "project": project}  # fmt: skip
    emit(state.api().add_domain("variables", {k: v for k, v in body.items() if v is not None}))


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


# -- evaluations ----------------------------------------------------------------------

SpecArg = Annotated[
    Path, typer.Argument(exists=True, dir_okay=False, help="The spec, as YAML or JSON.")
]


def _spec(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise typer.BadParameter(f"not YAML or JSON: {exc}", param_hint="SPEC") from exc
    if not isinstance(data, dict):
        raise typer.BadParameter("a spec is a mapping (keys and values)", param_hint="SPEC")
    return data


def _show_plan(plan: dict[str, Any]) -> None:
    for warning in plan.get("warnings") or []:
        notes.print(f"warning: {escape(warning)}", style="yellow")
    console.print(f"unit: {escape(str(plan.get('unit')))}")
    for i in plan.get("inputs") or []:
        console.print(
            f"  {i['side']}: {escape(i['dataset'])} ({i['role']}, "
            f"{escape(str(i.get('variable')))}, in {escape(str(i.get('unit')))})"
        )
    if plan.get("relation"):
        console.print(f"  crosswalk: {escape(plan['relation']['dataset'])}")
    for n, stage in enumerate(plan.get("stages") or [], 1):
        by = ", ".join(stage["group_by"]) or "everything"
        console.print(f"  stage {n}: by {escape(by)} -> {escape(', '.join(stage['columns']))}")


@evaluations.command("plan")
def evaluations_plan(spec: SpecArg, project: ProjectOpt) -> None:
    """Check a spec against the project's datasets, without saving or running it."""
    planned = state.api().plan(project, _spec(spec))
    if state.output == Output.JSON:
        emit(planned)
        return
    notes.print("The spec plans.", style="green")
    _show_plan(planned)


@evaluations.command("save")
def evaluations_save(slug: str, spec: SpecArg, project: ProjectOpt) -> None:
    """Save a spec under a name: version 1, the next version, or nothing if unchanged."""
    saved = state.api().save_evaluation(project, slug, _spec(spec))
    if state.output == Output.JSON:
        emit(saved)
        return
    what = (
        f"saved as version {saved['version']}"
        if saved["changed"]
        else (f"unchanged (still version {saved['version']})")
    )
    notes.print(f"{slug}: {what}", style="green")
    _show_plan(saved["plan"])


@evaluations.command("list")
def evaluations_list(project: ProjectOpt) -> None:
    """List the project's saved evaluations."""
    emit(state.api().list_evaluations(project), ["slug", "version", "updated_at", "created_by"])


@evaluations.command("show")
def evaluations_show(
    slug: str,
    project: ProjectOpt,
    version: Annotated[int | None, typer.Option(help="This version (default: latest).")] = None,
) -> None:
    """Show an evaluation's spec (YAML) and versions."""
    evaluation = state.api().get_evaluation(project, slug, version)
    if state.output == Output.JSON:
        emit(evaluation)
        return
    shown = evaluation.get("version")
    notes.print(f"{slug} version {shown}" + (
        f" (versions: {', '.join(str(v['version']) for v in evaluation['versions'])})"
        if evaluation.get("versions") else ""
    ))  # fmt: skip
    print(yaml.safe_dump(evaluation["spec"], sort_keys=False), end="")


@evaluations.command("delete")
def evaluations_delete(
    slug: str,
    project: ProjectOpt,
    keep_results: Annotated[
        bool, typer.Option("--keep-results", help="Keep the runs' tables, as datasets.")
    ] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Don't ask first.")] = False,
) -> None:
    """Delete an evaluation and its runs (and their tables, unless --keep-results)."""
    if not yes:
        typer.confirm(f"Delete evaluation {slug} in {project}?", abort=True)
    state.api().delete_evaluation(project, slug, keep_results)
    notes.print(f"Deleted {slug}", style="green")


# -- runs -----------------------------------------------------------------------------

_RUN_COLUMNS = ["id", "evaluation", "version", "status", "created_at", "finished_at"]
_RUN_OUTCOME = ["id", "evaluation", "version", "status", "results_dataset", "rows", "mode",
                "pairs", "error"]  # fmt: skip


def _run_waited(project: str, run: dict[str, Any], wait: bool) -> dict[str, Any]:
    if not wait:
        return run
    with notes.status(f"run {run['id']}: {run['status']}") as status:
        return state.api().wait_run(
            project, run["id"], lambda r: status.update(f"run {r['id']}: {r['status']}")
        )


def _run_outcome(run: dict[str, Any]) -> None:
    """Print a run: whole as JSON; in a table, a summary of its result."""
    if state.output == Output.JSON:
        emit(run)
    else:
        result = run.get("result") or {}
        mode = result.get("mode")
        pairs = result.get("pairs") or {}
        emit(
            {
                **run,
                "rows": result.get("rows"),
                "mode": f"{mode} ({result['why']})" if mode and result.get("why") else mode,
                "pairs": f"{pairs.get('rows')} rows in {pairs.get('dataset')}" if pairs else None,
            },
            _RUN_OUTCOME,
        )
    if run["status"] in ("failed", "cancelled"):
        raise typer.Exit(1)


@runs.command("start")
def runs_start(
    slug: str,
    project: ProjectOpt,
    full: Annotated[bool, typer.Option("--full", help="Recompute everything.")] = False,
    version: Annotated[int | None, typer.Option(help="This version (default: latest).")] = None,
    wait: Annotated[bool, typer.Option("--wait", help="Wait for the run to end.")] = False,
) -> None:
    """Run a saved evaluation (incremental unless --full)."""
    run = state.api().start_run(project, slug, version, full)
    _run_outcome(_run_waited(project, run, wait))


@runs.command("list")
def runs_list(slug: str, project: ProjectOpt) -> None:
    """List an evaluation's runs, newest first."""
    emit(state.api().list_runs(project, slug), _RUN_COLUMNS)


@runs.command("show")
def runs_show(run_id: str, project: ProjectOpt) -> None:
    """Show a run: status, and its result or error once it has ended."""
    _run_outcome(state.api().get_run(project, run_id))


@runs.command("wait")
def runs_wait(run_id: str, project: ProjectOpt) -> None:
    """Wait for a run to end."""
    _run_outcome(_run_waited(project, state.api().get_run(project, run_id), True))


@runs.command("cancel")
def runs_cancel(run_id: str, project: ProjectOpt) -> None:
    """Stop a queued or running run."""
    emit(state.api().cancel_run(project, run_id), ["id", "status", "finished_at"])


# -- datasets ------------------------------------------------------------------------


#: Columns every run writes on each row (the same within a run).
_LINEAGE = ("run_id", "spec_version", "engine_version")


def _split_ref(ref: str, project: str) -> tuple[str, str]:
    """``public.name`` names another project's dataset; a bare name is ``project``'s."""
    owner, dot, name = ref.partition(".")
    return (owner, name) if dot else (project, ref)


def _read(
    fetch: Callable[..., pa.Table],
    columns: str | None,
    filter: str | None,
    limit: int | None,
    out: Path | None,
) -> None:
    """Read rows and print them (at most PRINT_LIMIT by default) or write them all."""
    want = limit if limit is not None else (None if out is not None else PRINT_LIMIT)
    table = fetch(
        columns=[c.strip() for c in columns.split(",")] if columns else None,
        filter=filter,
        limit=None if want is None else want + 1,  # one more shows whether there are more
    )
    if want is not None and table.num_rows > want:
        table = table.slice(0, want)
        notes.print(
            f"first {want} rows; there are more (--limit N for more"
            + (", --out FILE for all)" if out is None else ")"),
            style="yellow",
        )
    if out is None:
        if "geometry" in table.column_names and state.output != Output.JSON:
            table = table.drop_columns(["geometry"])  # bytes; --out keeps it
        emit(table.to_pylist(), table.column_names)
        return
    suffix = out.suffix.lower()
    if suffix == ".parquet":
        pq.write_table(table, out, compression="zstd")
    elif suffix == ".geojson":
        raise typer.BadParameter(
            ".geojson is for run results with output.geometry (teehr runs results)",
            param_hint="--out",
        )
    elif suffix == ".csv":
        pacsv.write_csv(table, out)
    elif suffix in (".arrow", ".feather"):
        with pa.OSFile(str(out), "wb") as sink, pa.ipc.new_file(sink, table.schema) as w:
            w.write_table(table)
    else:
        raise typer.BadParameter(f"unsupported file type {suffix!r}", param_hint="--out")
    notes.print(f"Wrote {table.num_rows} rows to {out}", style="green")


@datasets.command("list")
def datasets_list(project: DataProjectOpt = "public") -> None:
    """List a project's datasets."""
    emit(state.api().list_datasets(project), ["name", "kind", "description", "created_at"])


@datasets.command("show")
def datasets_show(name: str, project: DataProjectOpt = "public") -> None:
    """Describe a dataset: kind, metadata and columns."""
    owner, dataset = _split_ref(name, project)
    record = state.api().get_dataset(owner, dataset)
    if state.output == Output.JSON:
        emit(record)
        return
    meta = record.get("timeseries") or record.get("layer") or record.get("crosswalk") or {}
    emit(
        {"name": record["name"], "kind": record["kind"], "table": record.get("table"),
         "key": record.get("key_columns"), "description": record.get("description"), **meta},
    )  # fmt: skip
    emit(record.get("columns") or [], ["name", "type", "required", "doc"])


@datasets.command("read")
def datasets_read(
    name: str,
    project: DataProjectOpt = "public",
    columns: ColumnsOpt = None,
    filter: FilterOpt = None,
    limit: LimitOpt = None,
    out: OutOpt = None,
) -> None:
    """Print a dataset's rows, or write them all to a file (--out).

    An evaluation's results table (results_<name>_v<version>) holds every run's rows,
    run_id saying which; `teehr runs results RUN_ID` reads one run's.
    """
    owner, dataset = _split_ref(name, project)
    api = state.api()
    _read(functools.partial(api.read, owner, dataset), columns, filter, limit, out)


def _columns(items: list[str] | None, flag: str) -> list[dict[str, Any]]:
    """``name:type`` (or ``name:type!`` for a required column) as column specs."""
    out = []
    for item in items or []:
        name, sep, kind = item.partition(":")
        if not sep or not name or not kind:
            raise typer.BadParameter(f"{item!r}: use NAME:TYPE, e.g. basin:string", param_hint=flag)
        required = kind.endswith("!")
        out.append(
            {"name": name, "type": kind.rstrip("!"), **({"required": True} if required else {})}
        )
    return out


@datasets.command("create")
def datasets_create(
    name: str,
    project: ProjectOpt,
    kind: Annotated[
        str, typer.Option(help="timeseries, layer, crosswalk or table (table: platform admins).")
    ],
    role: RoleOpt = None,
    variable: VariableOpt = None,
    statistic: StatisticOpt = None,
    period: PeriodOpt = None,
    unit: Annotated[
        str | None, typer.Option(help="Timeseries: the variable's canonical unit (the default).")
    ] = None,
    optional: Annotated[
        str | None,
        typer.Option(help="Timeseries: optional columns, e.g. reference_time,member,quality."),
    ] = None,
    layer: LayerOpt = None,
    partition: PartitionOpt = None,
    series_table: Annotated[
        str | None, typer.Option(help="Timeseries: share an existing series table.")
    ] = None,
    left: LeftOpt = None,
    right: RightOpt = None,
    column: Annotated[
        list[str] | None,
        typer.Option(
            "--column",
            help="NAME:TYPE (TYPE! if required), repeatable: a table's columns, a layer's "
            "attributes, a timeseries' or crosswalk's extra columns.",
        ),
    ] = None,
    key: Annotated[str | None, typer.Option(help="Table: comma-separated key columns.")] = None,
    source: SourceOpt = None,
    description: DescriptionOpt = None,
) -> None:
    """Register a dataset and make its table (most come from uploads instead)."""
    cols = _columns(column, "--column")
    body: dict[str, Any] = {"type": kind, "name": name, "description": description,
                            "source": source}  # fmt: skip
    if kind == "timeseries":
        body |= {"role": role, "variable": variable, "statistic": statistic, "period": period,
                 "unit": unit, "location_layer": layer, "time_partition": partition,
                 "series_table": series_table, "extra_columns": cols or None,
                 "optional_columns": optional.split(",") if optional else None}  # fmt: skip
    elif kind == "layer":
        body["attributes"] = cols or None
    elif kind == "crosswalk":
        body |= {"left": left, "right": right, "extra_columns": cols or None}
    elif kind == "table":
        body |= {"columns": cols, "key_columns": key.split(",") if key else None}
        body.pop("source")
    else:
        raise typer.BadParameter("timeseries, layer, crosswalk or table", param_hint="--kind")
    record = state.api().create_dataset(project, {k: v for k, v in body.items() if v is not None})
    shown = ("name", "kind", "table", "key_columns", "description")
    emit(record if state.output == Output.JSON else {k: record.get(k) for k in shown})


@datasets.command("delete")
def datasets_delete(name: str, project: ProjectOpt, yes: YesOpt = False) -> None:
    """Delete a dataset and its table (and a timeseries' series table, if no other uses it)."""
    if not yes:
        typer.confirm(f"Delete dataset {name} in {project} and its data?", abort=True)
    state.api().delete_dataset(project, name)
    notes.print(f"Deleted {name}", style="green")


def _load_rows(path: Path) -> pa.Table:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pq.read_table(path)
    if suffix == ".csv":
        return pacsv.read_csv(path)
    if suffix in (".json", ".ndjson", ".jsonl"):
        import pyarrow.json as pajson

        return pajson.read_json(path)
    if suffix in (".arrow", ".feather"):
        with pa.memory_map(str(path)) as source:
            return pa.ipc.open_file(source).read_all()
    raise typer.BadParameter(f"unsupported file type {suffix!r}", param_hint="FILE")


@datasets.command("write")
def datasets_write(
    name: str,
    file: Annotated[Path, typer.Argument(exists=True, dir_okay=False, help="Rows to write.")],
    project: ProjectOpt,
    mode: Annotated[
        str | None,
        typer.Option(help="upsert (typed datasets' default), append or overwrite."),
    ] = None,
    filter: Annotated[
        str | None, typer.Option("--filter", "-f", help="Overwrite: only the rows this keeps.")
    ] = None,
    timezone: TimezoneOpt = None,
    unit: UnitOpt = None,
) -> None:
    """Write a small file's rows (.csv, .parquet, .json, .arrow) to a dataset in one commit.

    Columns must already be the dataset's (see `teehr datasets show`). Up to 64 MiB:
    larger files, or ones that need mapping, go through `teehr uploads add`.
    """
    emit(state.api().write(project, name, _load_rows(file), mode=mode, filter=filter,
                           timezone=timezone, unit=unit),
         ["mode", "rows", "series_added", "months", "notes", "snapshot_id"])  # fmt: skip


@datasets.command("writes")
def datasets_writes(
    name: str,
    project: DataProjectOpt = "public",
    limit: Annotated[int | None, typer.Option("--limit", "-n", min=1)] = None,
) -> None:
    """List a dataset's writes, newest first: who, when, mode, rows and months."""
    owner, dataset = _split_ref(name, project)
    emit(state.api().list_writes(owner, dataset, limit),
         ["written_at", "written_by", "mode", "rows", "months", "unit", "series_added",
          "notes"])  # fmt: skip


@runs.command("results")
def runs_results(
    run_id: str,
    project: ProjectOpt,
    columns: ColumnsOpt = None,
    filter: FilterOpt = None,
    limit: LimitOpt = None,
    out: OutOpt = None,
    geometry: Annotated[
        bool,
        typer.Option(help="With output.geometry: each row's place (and output.attributes)."),
    ] = True,
) -> None:
    """Print a run's results, or write them all to a file (--out).

    Just this run's rows: its evaluation version's results table keeps every run's
    (see `teehr datasets read results_<name>_v<version>`). Printed, the lineage
    columns (run_id, spec_version, engine_version: the same on every row) are left
    out unless --columns asks for them.

    With output.geometry in the spec, results have places: `--out results.geojson`
    writes them as GeoJSON, and .parquet / .arrow keep a geometry column (WKB).
    """
    api = state.api()
    run = api.get_run(project, run_id)
    if run["status"] != "succeeded" or not run.get("results_dataset"):
        raise typer.BadParameter(f"the run is {run['status']}; results come once it succeeds")
    notes.print(f"run {run_id} of {run['evaluation']} v{run['version']}")
    if out is not None and out.suffix.lower() == ".geojson":
        collection = api.run_results_geojson(project, run_id, filter=filter, limit=limit)
        out.write_text(json.dumps(collection))
        notes.print(f"Wrote {len(collection['features'])} features to {out}", style="green")
        return
    if columns is None and out is None and state.output == Output.TABLE:
        dataset = api.get_dataset(project, run["results_dataset"])
        names = [c["name"] for c in dataset.get("columns") or []]
        kept = [c for c in names if c not in _LINEAGE]
        columns = ",".join(kept) if kept else None
    fetch = functools.partial(api.run_results, project, run_id, geometry=geometry)
    _read(fetch, columns, filter, limit, out)


def run() -> None:
    """Run the CLI with API errors shown as one line, not a traceback."""
    try:
        app()
    except TeehrError as exc:
        Console(stderr=True).print(escape(exc.detail), style="red")
        sys.exit(1)
