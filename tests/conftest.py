"""A fake TEEHR Cloud API (the control plane's token and project routes), and the
CLI pointed at it with its config in a temporary directory."""

import json
import uuid

import httpx
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
    monkeypatch.setattr("teehr.login.webbrowser.open", lambda url: True)
    return fake
