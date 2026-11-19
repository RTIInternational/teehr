import json

from typer.testing import CliRunner

from teehr.cli import app
from tests.conftest import API

runner = CliRunner()
CSV = b"site_no,datetime,discharge\\n01010000,2026-01-01T00:00:00,100\\n"


def run(*args):
    return runner.invoke(app, list(args), env={"COLUMNS": "200"})


def signed_in(tmp_path):
    run("--api", API, "auth", "login")
    path = tmp_path / "obs.csv"
    path.write_bytes(CSV)
    return path


def test_add_uploads_inspects_and_says_what_is_needed(api, tmp_path, monkeypatch):
    path = signed_in(tmp_path)
    result = run("uploads", "add", str(path), "-p", "flood", "--kind", "timeseries")
    assert result.exit_code == 0, result.output
    upload_id = next(iter(api.uploads))
    assert api.staged[f"/put/{upload_id}"] == CSV  # sent to staging, not the API
    out = result.output
    assert "series_key=site_no" in out and "note: quality" in out
    assert "--map value=..." in out and "--timezone ZONE" in out and "--role ..." in out
    assert f"teehr uploads ingest {upload_id} -p flood" in out
    assert api.ingested == []  # not loaded without --yes


def test_ingest_sends_the_changes_and_waits(api, tmp_path, monkeypatch):
    path = signed_in(tmp_path)
    run("uploads", "add", str(path), "-p", "flood", "--kind", "timeseries")
    upload_id = next(iter(api.uploads))
    result = run(
        "uploads", "ingest", upload_id, "-p", "flood", "--role", "observed",
        "--variable", "streamflow", "--unit", "[cft_i]/s", "--statistic", "inst",
        "--layer", "gauges", "--partition", "year", "--timezone", "UTC",
        "--map", "value=discharge", "--map", "series_columns=a,b", "--wait",
    )  # fmt: skip
    assert result.exit_code == 0, result.output
    assert api.ingested == [{
        "dataset": {"role": "observed", "variable": "streamflow", "unit": "[cft_i]/s",
                    "statistic": "inst", "time_partition": "year", "location_layer": "gauges"},
        "mapping": {"timezone": "UTC", "value": "discharge", "series_columns": ["a", "b"]},
    }]  # fmt: skip
    assert "succeeded" in result.output


def test_add_with_yes_loads_and_json_output(api, tmp_path, monkeypatch):
    path = signed_in(tmp_path)
    result = run(
        "-o", "json", "uploads", "add", str(path), "-p", "flood", "--kind", "timeseries",
        "--role", "observed", "-y", "--wait",
    )  # fmt: skip
    assert result.exit_code == 0, result.output
    loaded = json.loads(result.stdout)  # one document
    assert loaded["status"] == "succeeded" and loaded["result"]["rows"] == 3


def test_list_show_cancel_delete(api, tmp_path, monkeypatch):
    path = signed_in(tmp_path)
    run("uploads", "add", str(path), "-p", "flood", "--kind", "timeseries")
    upload_id = next(iter(api.uploads))
    assert "obs.csv" in run("uploads", "list", "-p", "flood").output
    assert "inspected" in run("uploads", "show", upload_id, "-p", "flood").output
    assert "cancelled" in run("uploads", "cancel", upload_id, "-p", "flood").output
    assert run("uploads", "delete", upload_id, "-p", "flood", "-y").exit_code == 0
    assert api.uploads == {}


def test_project_comes_from_the_environment(api, tmp_path, monkeypatch):
    path = signed_in(tmp_path)
    monkeypatch.setenv("TEEHR_PROJECT", "flood")
    assert run("uploads", "add", str(path), "--kind", "timeseries").exit_code == 0
