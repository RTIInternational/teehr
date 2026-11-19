"""The SDK: a Python client for TEEHR Cloud's API (the control plane).

The CLI is a thin layer on this, so anything it does is also a Python call::

    from teehr import Client

    client = Client.from_config()  # the token `teehr auth login` saved
    client.list_projects()

A client authenticates with a personal token (``tpat_...``, made by
``teehr auth login`` or :meth:`Client.start_sign_in`).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa

from teehr import config

USER_AGENT = "teehr-sdk"
ARROW = "application/vnd.apache.arrow.stream"
#: Rows asked for per page when reading (the API allows up to a million in Arrow).
READ_PAGE_ROWS = 100_000
#: An upload's or run's statuses once it has ended.
ENDED = ("succeeded", "failed", "cancelled")


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


def _given(**values: Any) -> dict[str, Any]:
    """Keep the values that were given (not None)."""
    return {k: v for k, v in values.items() if v is not None}


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
            detail = _detail(resp)
            if resp.status_code == 404 and detail == "Not Found":
                # The API's answer for a route it doesn't have (its own 404s say what
                # wasn't found): it's older than this client.
                detail = (
                    f"{self.api_url} has no {method} {path.split('?')[0]}; "
                    "the API may be older than this client"
                )
            raise TeehrError(resp.status_code, detail)
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

    # --- uploads -------------------------------------------------------------------

    def create_upload(
        self, project: str, filename: str, kind: str, format: str | None = None
    ) -> dict[str, Any]:
        """Start an upload; its ``upload_url`` takes the file (see :meth:`put_file`)."""
        body: dict[str, Any] = {"filename": filename, "kind": kind}
        if format:
            body["format"] = format
        upload: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/uploads", json=body
        ).json()
        return upload

    def put_file(self, upload_url: str, path: Path) -> None:
        """Send a file to an upload's presigned, short-lived URL.

        It goes straight to staging storage, not through the API. One request, so at
        most 5 GB.
        """
        with path.open("rb") as f:
            resp = self._http.put(
                upload_url, content=f, headers={"Content-Length": str(path.stat().st_size)}
            )
        if resp.status_code >= 400:
            raise TeehrError(resp.status_code, f"staging refused the file: {resp.text[:300]}")

    def inspect_upload(self, project: str, upload_id: str) -> dict[str, Any]:
        """Read the start of the staged file and propose how it maps (``preview``)."""
        upload: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/uploads/{upload_id}/inspect"
        ).json()
        return upload

    def upload_file(
        self, project: str, path: Path, kind: str, format: str | None = None
    ) -> dict[str, Any]:
        """Upload and inspect a file.

        Returns the upload, with the proposed mapping and what's still needed in its
        ``preview``.
        """
        upload = self.create_upload(project, path.name, kind, format)
        self.put_file(upload["upload_url"], path)
        return self.inspect_upload(project, upload["id"])

    def ingest_upload(
        self,
        project: str,
        upload_id: str,
        dataset: dict[str, Any] | None = None,
        mapping: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Load an inspected upload.

        ``dataset`` and ``mapping`` change what inspect proposed (a field left out
        keeps it). Starts a job; see :meth:`wait_upload`.
        """
        upload: dict[str, Any] = self._request(
            "POST",
            f"/v1/projects/{project}/uploads/{upload_id}/ingest",
            json={"dataset": dataset or {}, "mapping": mapping or {}},
        ).json()
        return upload

    def get_upload(self, project: str, upload_id: str) -> dict[str, Any]:
        """Return an upload: preview, status, progress, outcome and problems."""
        upload: dict[str, Any] = self._request(
            "GET", f"/v1/projects/{project}/uploads/{upload_id}"
        ).json()
        return upload

    def list_uploads(self, project: str) -> list[dict[str, Any]]:
        """List a project's uploads, newest first."""
        uploads: list[dict[str, Any]] = self._request(
            "GET", f"/v1/projects/{project}/uploads"
        ).json()
        return uploads

    def wait_upload(
        self,
        project: str,
        upload_id: str,
        on_update: Callable[[dict[str, Any]], None] | None = None,
        interval: float = 2.0,
    ) -> dict[str, Any]:
        """Wait for an upload's load to end; ``on_update`` sees each poll."""
        while True:
            upload = self.get_upload(project, upload_id)
            if on_update:
                on_update(upload)
            if upload["status"] in ENDED:
                return upload
            time.sleep(interval)

    def cancel_upload(self, project: str, upload_id: str) -> dict[str, Any]:
        """Stop a queued or running load (the upload can be ingested again)."""
        upload: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/uploads/{upload_id}/cancel"
        ).json()
        return upload

    def delete_upload(self, project: str, upload_id: str) -> None:
        """Delete an upload and its staged file (not data already loaded)."""
        self._request("DELETE", f"/v1/projects/{project}/uploads/{upload_id}")

    # --- datasets ------------------------------------------------------------------

    def list_datasets(self, project: str) -> list[dict[str, Any]]:
        """List a project's datasets."""
        datasets: list[dict[str, Any]] = self._request(
            "GET", f"/v1/projects/{project}/datasets"
        ).json()
        return datasets

    def get_dataset(self, project: str, dataset: str) -> dict[str, Any]:
        """Return a dataset: kind, columns, key, and its kind's metadata."""
        record: dict[str, Any] = self._request(
            "GET", f"/v1/projects/{project}/datasets/{dataset}"
        ).json()
        return record

    def create_dataset(self, project: str, body: dict[str, Any]) -> dict[str, Any]:
        """Register a dataset and make its table.

        ``body["type"]`` is ``timeseries``, ``layer``, ``crosswalk`` or ``table``; the
        rest is that kind's metadata (see the API docs, ``create_dataset``).
        """
        record: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/datasets", json=body
        ).json()
        return record

    def delete_dataset(self, project: str, dataset: str) -> None:
        """Delete a dataset and its table."""
        self._request("DELETE", f"/v1/projects/{project}/datasets/{dataset}")

    def write(
        self,
        project: str,
        dataset: str,
        table: pa.Table,
        *,
        mode: str | None = None,
        filter: str | None = None,
        timezone: str | None = None,
        unit: str | None = None,
    ) -> dict[str, Any]:
        """Write rows to a dataset in one commit, through the API (up to 64 MiB).

        ``mode``: ``upsert`` (the default for typed datasets), ``append`` or
        ``overwrite`` (of the rows ``filter`` keeps, or all). Larger files: uploads.
        """
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        written: dict[str, Any] = self._request(
            "POST",
            f"/v1/projects/{project}/datasets/{dataset}/data",
            params=_given(mode=mode, filter=filter, timezone=timezone, unit=unit),
            content=sink.getvalue().to_pybytes(),
            headers={"Content-Type": ARROW},
        ).json()
        return written

    def list_writes(
        self, project: str, dataset: str, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """List a dataset's writes, newest first: who, when, mode, rows, months, unit."""
        writes: list[dict[str, Any]] = self._request(
            "GET",
            f"/v1/projects/{project}/datasets/{dataset}/writes",
            params=_given(limit=limit),
        ).json()
        return writes

    def read(
        self,
        project: str,
        dataset: str,
        *,
        columns: list[str] | None = None,
        filter: str | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        """Read a dataset's rows (all, or the first ``limit``) as an Arrow table.

        An evaluation's results table holds every run's rows (``run_id`` says which);
        :meth:`run_results` reads one run's.

        ``filter`` is the API's expression language, e.g.
        ``"primary_series_key = 'usgs-01010000'"``. Pages are read in Arrow, all from
        one snapshot, so writes landing meanwhile don't shift them.
        """
        return self._pages(
            f"/v1/projects/{project}/datasets/{dataset}/data", columns, filter, limit
        )

    def _pages(
        self,
        path: str,
        columns: list[str] | None,
        filter: str | None,
        limit: int | None,
        extra: dict[str, Any] | None = None,
    ) -> pa.Table:
        """Read a paged table route in Arrow, following its cursor."""
        params: dict[str, Any] = dict(extra or {})
        if columns:
            params["columns"] = ",".join(columns)
        if filter:
            params["filter"] = filter
        batches: list[pa.Table] = []
        got, cursor = 0, None
        while limit is None or got < limit:
            page = READ_PAGE_ROWS if limit is None else min(READ_PAGE_ROWS, limit - got)
            resp = self._request(
                "GET",
                path,
                params={**params, "limit": page, **({"cursor": cursor} if cursor else {})},
                headers={"Accept": ARROW},
            )
            table = pa.ipc.open_stream(resp.content).read_all()
            batches.append(table)
            got += table.num_rows
            cursor = resp.headers.get("X-Next-Cursor")
            if not cursor:
                break
        if not batches:
            return pa.table({})
        return pa.concat_tables(batches, promote_options="default")

    # --- evaluations and runs --------------------------------------------------------

    def plan(self, project: str, spec: dict[str, Any]) -> dict[str, Any]:
        """Check a spec against the project's datasets, without saving or running it.

        Raises :class:`TeehrError` (422) listing every problem found.
        """
        planned: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/runs/plan", json=spec
        ).json()
        return planned

    def save_evaluation(self, project: str, slug: str, spec: dict[str, Any]) -> dict[str, Any]:
        """Save a spec under ``slug``: version 1, the next version, or nothing new.

        ``changed`` says whether a version was saved; ``plan`` is the spec as planned.
        """
        saved: dict[str, Any] = self._request(
            "PUT", f"/v1/projects/{project}/evaluations/{slug}", json=spec
        ).json()
        return saved

    def list_evaluations(self, project: str) -> list[dict[str, Any]]:
        """List a project's saved evaluations, with their latest versions."""
        evaluations: list[dict[str, Any]] = self._request(
            "GET", f"/v1/projects/{project}/evaluations"
        ).json()
        return evaluations

    def get_evaluation(self, project: str, slug: str, version: int | None = None) -> dict[str, Any]:
        """Return an evaluation: its latest spec and versions (or one ``version``)."""
        path = f"/v1/projects/{project}/evaluations/{slug}"
        if version is not None:
            path += f"/versions/{version}"
        evaluation: dict[str, Any] = self._request("GET", path).json()
        return evaluation

    def delete_evaluation(self, project: str, slug: str, keep_results: bool = False) -> None:
        """Delete an evaluation, its runs and (unless ``keep_results``) their tables."""
        params = {"keep_results": "true"} if keep_results else None
        self._request("DELETE", f"/v1/projects/{project}/evaluations/{slug}", params=params)

    def start_run(
        self, project: str, slug: str, version: int | None = None, full: bool = False
    ) -> dict[str, Any]:
        """Run an evaluation (its latest version by default), incrementally unless ``full``."""
        body: dict[str, Any] = {"full": full}
        if version is not None:
            body["version"] = version
        run: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/evaluations/{slug}/runs", json=body
        ).json()
        return run

    def list_runs(self, project: str, slug: str) -> list[dict[str, Any]]:
        """List an evaluation's runs, newest first."""
        runs: list[dict[str, Any]] = self._request(
            "GET", f"/v1/projects/{project}/evaluations/{slug}/runs"
        ).json()
        return runs

    def get_run(self, project: str, run_id: str) -> dict[str, Any]:
        """Return a run: status, and its result or error once it has ended."""
        run: dict[str, Any] = self._request("GET", f"/v1/projects/{project}/runs/{run_id}").json()
        return run

    def wait_run(
        self,
        project: str,
        run_id: str,
        on_update: Callable[[dict[str, Any]], None] | None = None,
        interval: float = 3.0,
    ) -> dict[str, Any]:
        """Wait for a run to end; ``on_update`` sees each poll."""
        while True:
            run = self.get_run(project, run_id)
            if on_update:
                on_update(run)
            if run["status"] in ENDED:
                return run
            time.sleep(interval)

    def run_results(
        self,
        project: str,
        run_id: str,
        *,
        columns: list[str] | None = None,
        filter: str | None = None,
        limit: int | None = None,
        geometry: bool = True,
    ) -> pa.Table:
        """Read a succeeded run's results as an Arrow table.

        A version's results table keeps every run's rows; this reads this run's. With
        ``output.geometry`` in the spec, rows get a ``geometry`` column (WKB) and the
        ``output.attributes``; ``geometry=False`` leaves them out.
        """
        return self._pages(
            f"/v1/projects/{project}/runs/{run_id}/results", columns, filter, limit,
            None if geometry else {"geometry": "false"},
        )  # fmt: skip

    def run_results_geojson(
        self,
        project: str,
        run_id: str,
        *,
        filter: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Read a run's results as one GeoJSON feature collection (``output.geometry``)."""
        params: dict[str, Any] = _given(filter=filter)
        features: list[dict[str, Any]] = []
        cursor = None
        while limit is None or len(features) < limit:
            page = 10_000 if limit is None else min(10_000, limit - len(features))
            resp = self._request(
                "GET",
                f"/v1/projects/{project}/runs/{run_id}/results",
                params={**params, "limit": page, **({"cursor": cursor} if cursor else {})},
                headers={"Accept": "application/geo+json"},
            )
            features += resp.json()["features"]
            cursor = resp.headers.get("X-Next-Cursor")
            if not cursor:
                break
        return {"type": "FeatureCollection", "features": features}

    def logs(
        self, project: str, kind: str, job_id: str, after: int | None = None
    ) -> dict[str, Any]:
        """Return a run's or upload's log lines after line ``after``, oldest first.

        ``kind`` is ``runs`` or ``uploads``; ``ended`` says no more lines will come.
        """
        page: dict[str, Any] = self._request(
            "GET", f"/v1/projects/{project}/{kind}/{job_id}/logs", params=_given(after=after)
        ).json()
        return page

    def follow_logs(
        self, project: str, kind: str, job_id: str, follow: bool = True, interval: float = 2.0
    ) -> Iterator[dict[str, Any]]:
        """Yield a run's or upload's log lines.

        With ``follow``, keep yielding new ones until it ends.
        """
        after = None
        while True:
            page = self.logs(project, kind, job_id, after)
            for line in page["lines"]:
                after = line["id"]
                yield line
            if page["lines"] and len(page["lines"]) >= 1000:
                continue  # more already waiting
            if not follow or page["ended"]:
                return
            time.sleep(interval)

    def cancel_run(self, project: str, run_id: str) -> dict[str, Any]:
        """Stop a queued or running run."""
        run: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/runs/{run_id}/cancel"
        ).json()
        return run

    # --- projects ------------------------------------------------------------------

    def list_projects(self) -> list[dict[str, Any]]:
        """List the projects you can use."""
        projects: list[dict[str, Any]] = self._request("GET", "/v1/projects").json()
        return projects

    def create_project(
        self, name: str, display_name: str | None = None, description: str | None = None
    ) -> dict[str, Any]:
        """Create a project; you become its admin. Needs a project-creator role."""
        body = _given(name=name, display_name=display_name, description=description)
        project: dict[str, Any] = self._request("POST", "/v1/projects", json=body).json()
        return project

    def get_project(self, name: str) -> dict[str, Any]:
        """Return a project: status, storage, your role."""
        project: dict[str, Any] = self._request("GET", f"/v1/projects/{name}").json()
        return project

    def update_project(
        self, name: str, display_name: str | None = None, description: str | None = None
    ) -> dict[str, Any]:
        """Change a project's display name or description (admins)."""
        body = _given(display_name=display_name, description=description)
        project: dict[str, Any] = self._request("PATCH", f"/v1/projects/{name}", json=body).json()
        return project

    def delete_project(self, name: str) -> None:
        """Delete a project and everything in it (admins)."""
        self._request("DELETE", f"/v1/projects/{name}")

    def provision_project(self, name: str) -> dict[str, Any]:
        """Retry setting up a project's storage and catalog access (admins)."""
        project: dict[str, Any] = self._request("POST", f"/v1/projects/{name}/provision").json()
        return project

    # --- members -------------------------------------------------------------------

    def list_members(self, project: str) -> list[dict[str, Any]]:
        """List a project's members and their roles."""
        members: list[dict[str, Any]] = self._request(
            "GET", f"/v1/projects/{project}/members"
        ).json()
        return members

    def add_member(self, project: str, username: str, role: str) -> dict[str, Any]:
        """Add a user to a project with a role: read, write or admin (admins)."""
        member: dict[str, Any] = self._request(
            "POST", f"/v1/projects/{project}/members", json={"username": username, "role": role}
        ).json()
        return member

    def _subject(self, project: str, user: str) -> str:
        for m in self.list_members(project):
            if user in (m["subject"], m.get("username")):
                return str(m["subject"])
        raise TeehrError(404, f"{user} isn't a member of {project}")

    def set_member(self, project: str, user: str, role: str) -> dict[str, Any]:
        """Change a member's role (``user``: their username or subject; admins)."""
        member: dict[str, Any] = self._request(
            "PUT",
            f"/v1/projects/{project}/members/{self._subject(project, user)}",
            json={"role": role},
        ).json()
        return member

    def remove_member(self, project: str, user: str) -> None:
        """Remove a member (``user``: their username or subject; admins)."""
        self._request("DELETE", f"/v1/projects/{project}/members/{self._subject(project, user)}")

    # --- domains: units, periods, variables ------------------------------------------

    def list_domain(self, kind: str, project: str | None = None) -> list[dict[str, Any]]:
        """List ``units``, ``periods`` or ``variables``: the public ones, and a project's own."""
        params = {"project": project} if project else None
        rows: list[dict[str, Any]] = self._request(
            "GET", f"/v1/domains/{kind}", params=params
        ).json()
        return rows

    def add_domain(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        """Add a unit, period or variable: to a project (``project`` in ``body``), or public."""
        row: dict[str, Any] = self._request("POST", f"/v1/domains/{kind}", json=body).json()
        return row
