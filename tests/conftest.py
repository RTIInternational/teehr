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
        self.polls = 0
        self.started: list[dict] = []

    def _me(self, request):
        auth = request.headers.get("Authorization", "")
        record = self.tokens.get(auth.removeprefix("Bearer "))
        if record is None or record["revoked_at"]:
            return None
        return record

    def handle(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if (method, path) == ("POST", "/v1/tokens/device"):
            self.started.append(json.loads(request.content))
            return httpx.Response(200, json={
                "user_code": "ABCD-EFGH", "verification_uri": f"{API}/device",
                "verification_uri_complete": f"{API}/device?user_code=ABCD-EFGH",
                "interval": 5, "expires_in": 600, "poll": "sealed-handle",
            })  # fmt: skip
        if (method, path) == ("POST", "/v1/tokens/device/poll"):
            self.polls += 1
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
        if (method, path) == ("GET", "/v1/projects"):
            return httpx.Response(200, json=[
                {"name": "flood", "my_role": "admin", "status": "ready", "description": "x"}
            ])  # fmt: skip
        return httpx.Response(404, json={"detail": f"no route {method} {path}"})


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

    def approve_on_second_poll(seconds):
        if fake.polls >= 1:
            fake.approved = True

    monkeypatch.setattr("teehr.login.time.sleep", approve_on_second_poll)
    monkeypatch.setattr("teehr.login.webbrowser.open", lambda url: True)
    return fake
