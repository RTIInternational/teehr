import json
import os
import stat
import sys

import pytest
from typer.testing import CliRunner

from teehr import Client, TeehrError, cli, config
from teehr.cli import app
from tests.conftest import API

runner = CliRunner()


def run(*args):
    return runner.invoke(app, list(args))


def test_login_signs_in_and_saves_a_token_only_the_user_can_read(api):
    result = run("--api", API + "/", "auth", "login", "--name", "laptop")
    assert result.exit_code == 0, result.output
    assert "ABCD-EFGH" in result.output and "as alice" in result.output
    assert api.started == [{"name": "laptop", "scopes": ["read", "write"], "expires_in_days": 90}]

    path = config.config_dir() / "credentials.json"
    saved = json.loads(path.read_text())
    assert saved["default"] == API
    assert saved["apis"][API]["token"].startswith("tpat_")
    assert saved["apis"][API]["name"] == "laptop"
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_status_projects_and_tokens_use_the_saved_token(api):
    run("--api", API, "auth", "login", "--name", "laptop")
    status = run("auth", "status")
    assert status.exit_code == 0 and "alice" in status.output and "laptop" in status.output
    assert "flood" in run("projects", "list").output
    assert "True" in run("tokens", "list").output
    assert Client.from_config().list_projects()[0]["name"] == "flood"


def test_read_only_and_project_limits_are_asked_for(api):
    run("--api", API, "auth", "login", "--read-only", "--project", "flood", "--days", "7")
    assert api.started[-1] == {"name": api.started[-1]["name"], "scopes": ["read"],
                               "projects": ["flood"], "expires_in_days": 7}  # fmt: skip


def test_logout_revokes_and_forgets(api):
    run("--api", API, "auth", "login")
    result = run("auth", "logout")
    assert result.exit_code == 0 and "Signed out" in result.output
    assert all(t["revoked_at"] for t in api.tokens.values())
    assert config.credentials() is None
    assert isinstance(run("auth", "status").exception, TeehrError)  # not signed in


def test_a_revoked_token_is_reported_plainly(api):
    run("--api", API, "auth", "login")
    for t in api.tokens.values():
        t["revoked_at"] = "now"
    result = run("projects", "list")
    assert isinstance(result.exception, TeehrError)
    assert result.exception.detail == "Invalid token"


def test_login_with_an_existing_token(api):
    run("--api", API, "auth", "login")
    secret = next(iter(api.tokens))
    config.forget(API)
    result = run("--api", API, "auth", "login", "--token", secret)
    assert result.exit_code == 0 and api.polls == 2  # no new sign-in
    assert config.credentials().token == secret


def test_environment_overrides_for_ci(api, monkeypatch):
    run("--api", API, "auth", "login")
    secret = next(iter(api.tokens))
    config.forget(API)
    monkeypatch.setenv("TEEHR_API_URL", API)
    monkeypatch.setenv("TEEHR_TOKEN", secret)
    assert run("projects", "list").exit_code == 0


def test_login_needs_an_api_the_first_time(api):
    result = run("auth", "login")
    assert result.exit_code == 2 and "--api" in result.output


def test_api_errors_are_one_line_not_a_traceback(api, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["teehr", "projects", "list"])
    with pytest.raises(SystemExit) as exit_:
        cli.run()
    assert exit_.value.code == 1
    err = capsys.readouterr().err
    assert "Not signed in" in err and "Traceback" not in err


def test_json_output_is_parseable(api):
    run("--api", API, "auth", "login")
    result = run("-o", "json", "tokens", "list")
    assert result.exit_code == 0
    assert json.loads(result.stdout)[0]["current"] is True
    csv_out = run("-o", "csv", "projects", "list").stdout.splitlines()
    assert csv_out[0] == "name,my_role,status,description" and csv_out[1].startswith("flood,")


def test_token_ids_are_listed_whole_for_revoking(api):
    run("--api", API, "auth", "login")
    token_id = next(iter(api.tokens.values()))["id"]
    wide = runner.invoke(app, ["tokens", "list"], env={"COLUMNS": "200"})
    assert token_id in wide.output


def test_timestamps_read_to_the_second_in_tables():
    assert cli._cell("2026-11-19T18:29:21.237820Z") == "2026-11-19 18:29:21"
    assert cli._cell("2026-11-19T18:29:21+00:00") == "2026-11-19 18:29:21"
    assert cli._cell("not a time") == "not a time"


def test_a_route_the_api_lacks_says_so():
    import httpx

    from teehr.client import Client

    client = Client(API, "tpat_x", http=httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(404, json={"detail": "Not Found"})
    )))  # fmt: skip
    with pytest.raises(TeehrError) as raised:
        client.get_run("flood", "abc")
    assert "has no GET /v1/projects/flood/runs/abc" in raised.value.detail
    assert "older than this client" in raised.value.detail


def test_several_problems_read_one_per_line():
    import httpx

    from teehr.client import Client

    body = {"detail": "x", "errors": [
        {"loc": ["body"], "msg": "Value error, unknown metric 'kge2'"},
        {"loc": ["body", "inputs", "secondary", 0], "msg": "no dataset 'public.nope'"},
    ]}  # fmt: skip
    client = Client(API, "tpat_x", http=httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(422, json=body)
    )))  # fmt: skip
    with pytest.raises(TeehrError) as raised:
        client.list_projects()
    assert raised.value.detail == (
        "2 problems:\n- unknown metric 'kge2'\n- inputs.secondary.0: no dataset 'public.nope'"
    )


def test_an_unreachable_api_is_one_line(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("TEEHR_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("TEEHR_TOKEN", "tpat_x")
    monkeypatch.setattr(sys, "argv", ["teehr", "--api", "http://127.0.0.1:9", "projects", "list"])
    with pytest.raises(SystemExit) as exit_:
        cli.run()
    assert exit_.value.code == 1
    err = capsys.readouterr().err
    assert err.startswith("Can't reach the API") and "Traceback" not in err
