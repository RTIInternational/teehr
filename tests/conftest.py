"""A fake TEEHR Cloud API (the control plane's token and project routes), and the
CLI pointed at it with its config in a temporary directory."""

import json
import uuid
from typing import ClassVar

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pytest

API = "https://cp.test"


class FakeAPI:
    def __init__(self):
        self.tokens: dict[str, dict] = {}  # secret -> token record
        self.approved = False
        self.auto_approve = True  # the user approves after the first poll
        self.polls = 0
        self.started: list[dict] = []
        self.uploads: dict[str, dict] = {}
        self.staged: dict[str, bytes] = {}
        self.ingested: list[dict] = []
        self.evaluations: dict[str, list[dict]] = {}  # slug -> versions' specs
        self.sent: list[tuple[str, str, object]] = []  # (method, path, body) of admin calls
        self.members = [{"subject": "sub-bob", "username": "bob", "role": "read",
                         "added_by": "alice", "added_at": "2026-11-19T00:00:00Z"}]  # fmt: skip
        self.runs: dict[str, dict] = {}

    def _me(self, request):
        auth = request.headers.get("Authorization", "")
        record = self.tokens.get(auth.removeprefix("Bearer "))
        if record is None or record["revoked_at"]:
            return None
        return record

    def handle(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if request.url.host == "staging.test":  # presigned: no API token
            assert "Authorization" not in request.headers
            self.staged[path] = request.read()
            return httpx.Response(200)
        if (method, path) == ("POST", "/v1/tokens/device"):
            self.started.append(json.loads(request.content))
            return httpx.Response(200, json={
                "user_code": "ABCD-EFGH", "verification_uri": f"{API}/device",
                "verification_uri_complete": f"{API}/device?user_code=ABCD-EFGH",
                "interval": 5, "expires_in": 600, "poll": "sealed-handle",
            })  # fmt: skip
        if (method, path) == ("POST", "/v1/tokens/device/poll"):
            self.polls += 1
            if self.auto_approve and self.polls >= 2:
                self.approved = True
            if json.loads(request.content)["poll"] != "sealed-handle":
                return httpx.Response(400, json={"detail": "not a sign-in we started"})
            if not self.approved:
                return httpx.Response(202, json={"status": "pending", "slow_down": False})
            asked = self.started[-1]
            secret = f"tpat_{uuid.uuid4().hex}"
            record = {
                "id": str(uuid.uuid4()), "name": asked["name"], "username": "alice",
                "hint": secret[:9], "scopes": asked.get("scopes", ["read", "write"]),
                "projects": asked.get("projects"), "created_at": "2026-11-19T00:00:00Z",
                "expires_at": "2027-02-17T00:00:00Z", "last_used_at": None,
                "revoked_at": None,
            }  # fmt: skip
            self.tokens[secret] = record
            return httpx.Response(200, json={**record, "current": False, "token": secret})
        me = self._me(request)
        if me is None:
            return httpx.Response(
                401, json={"title": "Unauthorized", "status": 401, "detail": "Invalid token"}
            )
        if (method, path) == ("GET", "/v1/tokens"):
            return httpx.Response(200, json=[
                {**t, "current": t["id"] == me["id"]} for t in self.tokens.values()
            ])  # fmt: skip
        if method == "DELETE" and path.startswith("/v1/tokens/"):
            token_id = path.rsplit("/", 1)[1]
            for t in self.tokens.values():
                if t["id"] == token_id:
                    t["revoked_at"] = "2026-11-19T01:00:00Z"
                    return httpx.Response(204)
            return httpx.Response(404, json={"detail": "No such token"})
        admin = self._admin(request, method, path)
        if admin is not None:
            return admin
        if "/datasets" in path:
            return self._datasets(request, method, path)
        if path.startswith(("/v1/projects/flood/evaluations", "/v1/projects/flood/runs")):
            return self._evaluations(request, method, path.removeprefix("/v1/projects/flood"))
        if path.startswith("/v1/projects/flood/uploads"):
            return self._uploads(request, method, path.removeprefix("/v1/projects/flood/uploads"))
        if (method, path) == ("GET", "/v1/projects"):
            return httpx.Response(200, json=[
                {"name": "flood", "my_role": "admin", "status": "ready", "description": "x"}
            ])  # fmt: skip
        return httpx.Response(404, json={"detail": f"no route {method} {path}"})

    def _uploads(self, request, method, rest):
        if (method, rest) == ("POST", ""):
            body = json.loads(request.content)
            upload_id = str(uuid.uuid4())
            self.uploads[upload_id] = {
                "id": upload_id, "filename": body["filename"], "kind": body["kind"],
                "status": "awaiting_file", "rows_done": 0, "preview": None, "result": None,
                "problems": None, "error": None, "created_at": "2026-11-19T00:00:00Z",
            }  # fmt: skip
            return httpx.Response(201, json={
                **self.uploads[upload_id], "upload_url": f"https://staging.test/put/{upload_id}"
            })  # fmt: skip
        if (method, rest) == ("GET", ""):
            return httpx.Response(200, json=list(self.uploads.values()))
        upload_id, _, action = rest.strip("/").partition("/")
        upload = self.uploads.get(upload_id)
        if upload is None:
            return httpx.Response(404, json={"detail": "No such upload"})
        if (method, action) == ("POST", "inspect"):
            assert f"/put/{upload_id}" in self.staged, "inspected before the file was sent"
            upload["status"] = "inspected"
            upload["preview"] = {
                "rows": 3, "columns": [], "sample": [],
                "mapping": {"series_key": "site_no", "value_time": "datetime", "value": None},
                "dataset": {"name": "obs"}, "notes": ["quality: qualifiers"],
                "needed": ["mapping.value", "mapping.timezone", "dataset.role"],
            }  # fmt: skip
            return httpx.Response(200, json=upload)
        if (method, action) == ("POST", "ingest"):
            body = json.loads(request.content)
            self.ingested.append(body)
            upload["status"], upload["polls"] = "queued", 0
            return httpx.Response(202, json=upload)
        if (method, action) == ("GET", ""):
            if upload["status"] in ("queued", "running"):
                upload["polls"] += 1
                upload["status"] = "running" if upload["polls"] < 2 else "succeeded"
                upload["rows_done"] = 3 if upload["status"] == "succeeded" else 1
                if upload["status"] == "succeeded":
                    upload["result"] = {"dataset": "obs", "rows": 3}
            return httpx.Response(200, json=upload)
        if (method, action) == ("POST", "cancel"):
            upload["status"] = "cancelled"
            return httpx.Response(200, json=upload)
        if (method, action) == ("DELETE", ""):
            del self.uploads[upload_id]
            return httpx.Response(204)
        return httpx.Response(404, json={"detail": f"no route {method} {rest}"})

    PLAN: ClassVar[dict] = {
        "unit": "m3/s",
        "inputs": [
            {"dataset": "public.usgs", "side": "primary", "role": "observed",
             "variable": "streamflow", "unit": "m3/s", "period": None},
            {"dataset": "public.nwm", "side": "secondary", "role": "simulated",
             "variable": "streamflow", "unit": "m3/s", "period": None},
        ],
        "relation": {"dataset": "public.usgs_nwm", "reversed": False},
        "stages": [{"group_by": ["primary_series_key"], "columns": ["kge"]}],
    }  # fmt: skip

    def _evaluations(self, request, method, path):
        parts = path.strip("/").split("/")  # evaluations/<slug>[/runs] or runs/<id>[/cancel]
        if parts == ["runs", "plan"] and method == "POST":
            spec = json.loads(request.content)
            if "inputs" not in spec:
                return httpx.Response(422, json={
                    "detail": "inputs: Field required",
                    "errors": [{"loc": ["body", "inputs"], "msg": "Field required"}],
                })  # fmt: skip
            return httpx.Response(200, json=self.PLAN)
        if parts == ["evaluations"] and method == "GET":
            return httpx.Response(200, json=[
                {"slug": k, "version": len(v), "updated_at": "2026-11-19T00:00:00Z",
                 "created_by": "alice"} for k, v in self.evaluations.items()
            ])  # fmt: skip
        if parts[0] == "evaluations" and len(parts) == 2:
            slug = parts[1]
            versions = self.evaluations.get(slug)
            if method == "PUT":
                spec = json.loads(request.content)
                versions = self.evaluations.setdefault(slug, [])
                changed = not versions or versions[-1] != spec
                if changed:
                    versions.append(spec)
                return httpx.Response(201 if len(versions) == 1 and changed else 200, json={
                    "slug": slug, "version": len(versions), "spec": versions[-1],
                    "versions": [], "created_by": "alice", "created_at": "x",
                    "changed": changed, "plan": self.PLAN,
                })  # fmt: skip
            if versions is None:
                return httpx.Response(404, json={"detail": "No such evaluation"})
            if method == "GET":
                return httpx.Response(200, json={
                    "slug": slug, "version": len(versions), "spec": versions[-1],
                    "versions": [{"version": n + 1} for n in range(len(versions))],
                })  # fmt: skip
            if method == "DELETE":
                del self.evaluations[slug]
                return httpx.Response(204)
        if parts[0] == "evaluations" and parts[2:] == ["runs"]:
            slug = parts[1]
            if method == "POST":
                body = json.loads(request.content)
                run_id = str(uuid.uuid4())
                self.runs[run_id] = {
                    "id": run_id, "evaluation": slug, "version": body.get("version", 1),
                    "status": "queued", "results_dataset": f"results_{slug}_v1",
                    "result": None, "error": None, "full": body["full"], "polls": 0,
                    "created_at": "x", "finished_at": None,
                }  # fmt: skip
                return httpx.Response(202, json=self.runs[run_id])
            return httpx.Response(200, json=[r for r in self.runs.values()
                                             if r["evaluation"] == slug])  # fmt: skip
        if parts[0] == "runs" and len(parts) >= 2:
            run = self.runs.get(parts[1])
            if run is None:
                return httpx.Response(404, json={"detail": "No such run"})
            if parts[2:] == ["results"]:
                if run["status"] != "succeeded":
                    return httpx.Response(409, json={"detail": f"The run is {run['status']}"})
                # The version's table holds every run's rows; this route reads one run's.
                mine = self.RESULTS.append_column(
                    "run_id", pa.array([run["id"]] * self.RESULTS.num_rows)
                )
                other = mine.set_column(
                    2, "run_id", pa.array(["another-run"] * self.RESULTS.num_rows)
                )
                self.results_reads = getattr(self, "results_reads", 0) + 1
                return self._arrow_page(request, pa.concat_tables([mine, other]).filter(
                    pc.equal(pa.concat_tables([mine, other])["run_id"], run["id"])
                ))  # fmt: skip
            if parts[2:] == ["cancel"]:
                run["status"] = "cancelled"
                return httpx.Response(200, json=run)
            if run["status"] in ("queued", "running"):
                run["polls"] += 1
                run["status"] = "running" if run["polls"] < 2 else "succeeded"
                if run["status"] == "succeeded":
                    run["result"] = {"rows": 7053, "mode": "full"}
            return httpx.Response(200, json=run)
        return httpx.Response(404, json={"detail": f"no route {method} {path}"})

    #: results_kge_v1 in flood: 250 rows, served in pages.
    RESULTS: ClassVar[pa.Table] = pa.table({
        "primary_series_key": [f"usgs-{i:03d}" for i in range(250)],
        "kge": [i / 250 for i in range(250)],
    })  # fmt: skip

    def _datasets(self, request, method, path):
        project, _, rest = path.removeprefix("/v1/projects/").partition("/datasets")
        if rest == "":
            return httpx.Response(200, json=[
                {"name": "results_kge_v1", "kind": "table", "description": "results",
                 "created_at": "2026-11-19T00:00:00Z"}
            ])  # fmt: skip
        name, _, action = rest.strip("/").partition("/")
        if name != "results_kge_v1" or project != "flood":
            return httpx.Response(404, json={"detail": f"No dataset '{name}'"})
        if action == "":
            return httpx.Response(200, json={
                "name": name, "kind": "table", "description": "results",
                "table": "flood.results_kge_v1", "key_columns": None,
                "columns": [{"name": "primary_series_key", "type": "string"},
                            {"name": "kge", "type": "float64"}],
            })  # fmt: skip
        return self._arrow_page(request, self.RESULTS)

    def _arrow_page(self, request, table):
        """A page of ``table`` in Arrow, as the API serves reads (small pages)."""
        assert request.headers["Accept"] == "application/vnd.apache.arrow.stream"
        params = request.url.params
        if params.get("filter"):
            key = params["filter"].split("'")[1]
            table = table.filter(pc.equal(table["primary_series_key"], key))
        if params.get("columns"):
            table = table.select(params["columns"].split(","))
        page = min(int(params.get("limit", 1000)), 100)  # small pages: exercise the cursor
        offset = int(params.get("cursor", 0))
        rows = table.slice(offset, page)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, rows.schema) as writer:
            writer.write_table(rows)
        headers = {}
        if offset + page < table.num_rows:
            headers["X-Next-Cursor"] = str(offset + page)
        self.reads = getattr(self, "reads", 0) + 1
        return httpx.Response(200, content=sink.getvalue().to_pybytes(), headers=headers)

    def _admin(self, request, method, path):
        """Projects, members, domains and dataset changes: recorded, answered plainly."""
        body = None
        if request.content:
            is_arrow = request.headers.get("Content-Type") == "application/vnd.apache.arrow.stream"
            body = (pa.ipc.open_stream(request.content).read_all() if is_arrow
                    else json.loads(request.content))  # fmt: skip
        project = {"name": "flood", "display_name": None, "description": "x", "kind": "tenant",
                   "status": "ready", "my_role": "admin", "storage_location": "s3://x",
                   "created_by": "alice", "created_at": "2026-11-19T00:00:00Z"}  # fmt: skip
        parts = path.strip("/").split("/")
        if (method, path) == ("POST", "/v1/projects"):
            self.sent.append((method, path, body))
            return httpx.Response(201, json={**project, **body})
        if (
            parts[:2] == ["v1", "projects"]
            and len(parts) == 3
            and method in ("GET", "PATCH", "DELETE")
        ):
            self.sent.append((method, path, body))
            if method == "DELETE":
                return httpx.Response(204)
            return httpx.Response(200, json={**project, "name": parts[2], **(body or {})})
        if parts[3:] == ["provision"]:
            self.sent.append((method, path, body))
            return httpx.Response(200, json=project)
        if parts[3:4] == ["members"]:
            self.sent.append((method, path, body))
            if method == "GET":
                return httpx.Response(200, json=self.members)
            if method == "POST":
                member = {"subject": f"sub-{body['username']}", "added_by": "alice",
                          "added_at": "x", **body}  # fmt: skip
                self.members.append(member)
                return httpx.Response(201, json=member)
            subject = parts[4]
            member = next((m for m in self.members if m["subject"] == subject), None)
            if member is None:
                return httpx.Response(404, json={"detail": "Not a member"})
            if method == "PUT":
                member["role"] = body["role"]
                return httpx.Response(200, json=member)
            self.members.remove(member)
            return httpx.Response(204)
        if parts[:2] == ["v1", "domains"]:
            self.sent.append((method, path, body))
            if method == "POST":
                return httpx.Response(201, json=body)
            public = {"symbol": "m3/s", "dimension": "volume_flow_rate", "to_si": 1.0,
                      "offset": 0.0, "description": None, "project": None,
                      "name": "streamflow", "canonical_unit": "m3/s"}  # fmt: skip
            return httpx.Response(200, json=[public])
        if parts[3:4] == ["datasets"] and (
            (method == "POST" and len(parts) == 4) or method == "DELETE"
            or (method == "POST" and parts[5:] == ["data"]) or parts[5:] == ["writes"]
        ):  # fmt: skip
            self.sent.append((method, path, body))
            if method == "DELETE":
                return httpx.Response(204)
            if parts[5:] == ["data"]:
                return httpx.Response(201, json={
                    "id": "w1", "mode": request.url.params.get("mode", "upsert"), "filter": None,
                    "rows": body.num_rows, "snapshot_id": "1", "written_by": "alice",
                    "written_at": "x", "series_added": 0, "months": ["2026-01"], "notes": None,
                })  # fmt: skip
            if parts[5:] == ["writes"]:
                return httpx.Response(200, json=[{"written_at": "2026-11-19T00:00:00Z",
                                                  "written_by": "alice", "mode": "upsert",
                                                  "rows": 3, "months": ["2026-01"]}])  # fmt: skip
            return httpx.Response(201, json={"name": body["name"], "kind": body["type"],
                                             "table": f"flood.{body['name']}",
                                             "key_columns": None, "description": None})  # fmt: skip
        return None


@pytest.fixture
def api(monkeypatch, tmp_path):
    """The fake API; every client the SDK or CLI makes talks to it. Config goes
    to a temporary directory, and waiting is instant."""
    fake = FakeAPI()
    transport = httpx.MockTransport(fake.handle)
    real = httpx.Client

    def client(*args, **kwargs):
        kwargs.pop("verify", None)
        return real(*args, transport=transport, **kwargs)

    monkeypatch.setattr("teehr.client.httpx.Client", client)
    monkeypatch.setenv("TEEHR_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.delenv("TEEHR_API_URL", raising=False)
    monkeypatch.delenv("TEEHR_TOKEN", raising=False)

    monkeypatch.setattr("time.sleep", lambda seconds: None)  # waiting is instant
    # Plain, wide output wherever tests run (CI turns colours on).
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    from rich.console import Console

    from teehr import cli

    monkeypatch.setattr(cli, "console", Console(color_system=None, width=200))
    monkeypatch.setattr(cli, "notes", Console(stderr=True, color_system=None, width=200))
    monkeypatch.setattr("teehr.login.webbrowser.open", lambda url: True)
    return fake
