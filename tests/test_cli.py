import json
import os
import stat

from typer.testing import CliRunner

from teehr import Client, config
from teehr.cli import app
from tests.conftest import API

runner = CliRunner()


def run(*args):
    result = runner.invoke(app, list(args))
    return result


def test_login_signs_in_and_saves_a_token_only_the_user_can_read(api):
    result = run("auth", "login", "--api", API + "/", "--name", "laptop")
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
    run("auth", "login", "--api", API, "--name", "laptop")
    status = run("auth", "status")
    assert status.exit_code == 0 and "alice" in status.output and "laptop" in status.output
    assert "flood" in run("projects", "list").output
    assert "this one" in run("tokens", "list").output
    assert Client.from_config().list_projects()[0]["name"] == "flood"


def test_read_only_and_project_limits_are_asked_for(api):
    run("auth", "login", "--api", API, "--read-only", "--project", "flood", "--days", "7")
    assert api.started[-1] == {"name": api.started[-1]["name"], "scopes": ["read"],
                               "projects": ["flood"], "expires_in_days": 7}  # fmt: skip


def test_logout_revokes_and_forgets(api):
    run("auth", "login", "--api", API)
    result = run("auth", "logout")
    assert result.exit_code == 0 and "Signed out" in result.output
    assert all(t["revoked_at"] for t in api.tokens.values())
    assert config.credentials() is None
    assert run("auth", "status").exit_code == 1  # not signed in: one line, no traceback


def test_a_revoked_token_is_reported_plainly(api):
    run("auth", "login", "--api", API)
    for t in api.tokens.values():
        t["revoked_at"] = "now"
    result = run("projects", "list")
    assert result.exit_code == 1 and "Invalid token" in result.output
    assert "Traceback" not in result.output


def test_login_with_an_existing_token(api):
    run("auth", "login", "--api", API)
    secret = next(iter(api.tokens))
    config.forget(API)
    result = run("auth", "login", "--api", API, "--token", secret)
    assert result.exit_code == 0 and api.polls == 2  # no new sign-in
    assert config.credentials().token == secret


def test_environment_overrides_for_ci(api, monkeypatch):
    run("auth", "login", "--api", API)
    secret = next(iter(api.tokens))
    config.forget(API)
    monkeypatch.setenv("TEEHR_API_URL", API)
    monkeypatch.setenv("TEEHR_TOKEN", secret)
    assert run("projects", "list").exit_code == 0


def test_login_needs_an_api_the_first_time(api):
    result = run("auth", "login")
    assert result.exit_code == 2 and "--api" in result.output


def test_token_ids_are_listed_whole_for_revoking(api):
    run("auth", "login", "--api", API)
    token_id = next(iter(api.tokens.values()))["id"]
    listed = run("tokens", "list").output
    assert token_id in listed.replace("\n", "").replace("│", "").replace(" ", "")
