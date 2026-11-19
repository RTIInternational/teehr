import json

from typer.testing import CliRunner

from teehr import TeehrError
from teehr.cli import app
from tests.conftest import API

runner = CliRunner()


def run(*args, input=None):
    return runner.invoke(app, list(args), input=input)


def login():
    run("--api", API, "auth", "login")


def test_projects(api):
    login()
    created = run("projects", "create", "drought", "--description", "dry spells")
    assert created.exit_code == 0, created.output
    assert api.sent[-1] == (
        "POST",
        "/v1/projects",
        {"name": "drought", "description": "dry spells"},
    )
    assert "flood" in run("projects", "show", "flood").output
    run("projects", "update", "flood", "--display-name", "Flood study")
    assert api.sent[-1] == ("PATCH", "/v1/projects/flood", {"display_name": "Flood study"})
    assert run("projects", "delete", "flood", input="n\n").exit_code == 1  # asked; declined
    assert run("projects", "delete", "flood", "-y").exit_code == 0
    assert api.sent[-1][:2] == ("DELETE", "/v1/projects/flood")
    run("projects", "provision", "flood")
    assert api.sent[-1][:2] == ("POST", "/v1/projects/flood/provision")


def test_members_by_username(api):
    login()
    assert "bob" in run("projects", "members", "list", "-p", "flood").output
    run("projects", "members", "add", "carol", "-p", "flood", "--role", "write")
    assert api.sent[-1][2] == {"username": "carol", "role": "write"}
    result = run("projects", "members", "set", "bob", "-p", "flood", "--role", "admin")
    assert result.exit_code == 0, result.output
    assert api.sent[-1][:2] == ("PUT", "/v1/projects/flood/members/sub-bob")  # resolved
    assert run("projects", "members", "remove", "bob", "-p", "flood", "-y").exit_code == 0
    missing = run("projects", "members", "set", "zed", "-p", "flood", "--role", "read")
    assert (
        isinstance(missing.exception, TeehrError) and "isn't a member" in missing.exception.detail
    )


def test_domains(api):
    login()
    listed = run("units", "list")
    assert "volume_flow_rate" in listed.output and "public" in listed.output
    run("units", "add", "[cft_i]/s", "--dimension", "volume_flow_rate",
        "--to-si", "0.028316846592", "-p", "flood")  # fmt: skip
    assert api.sent[-1] == ("POST", "/v1/domains/units", {
        "symbol": "[cft_i]/s", "dimension": "volume_flow_rate", "to_si": 0.028316846592,
        "offset": 0.0, "project": "flood"})  # fmt: skip
    run("periods", "add", "hour_ending", "--form", "duration", "--length", "PT1H",
        "--value-time-at", "end")  # fmt: skip
    assert api.sent[-1][2] == {"name": "hour_ending", "form": "duration", "length": "PT1H",
                               "value_time_at": "end"}  # fmt: skip
    run("variables", "list", "-p", "flood")
    assert api.sent[-1][:2] == ("GET", "/v1/domains/variables")
    run("variables", "add", "stage", "--canonical-unit", "m")
    assert api.sent[-1][2] == {"name": "stage", "canonical_unit": "m"}


def test_dataset_create_by_kind(api):
    login()
    result = run(
        "datasets", "create", "my_obs", "-p", "flood", "--kind", "timeseries",
        "--role", "observed", "--variable", "streamflow", "--statistic", "inst",
        "--optional", "quality", "--layer", "gauges", "--partition", "year",
        "--column", "basin:string",
    )  # fmt: skip
    assert result.exit_code == 0, result.output
    assert api.sent[-1][2] == {
        "type": "timeseries", "name": "my_obs", "role": "observed", "variable": "streamflow",
        "statistic": "inst", "location_layer": "gauges", "time_partition": "year",
        "extra_columns": [{"name": "basin", "type": "string"}], "optional_columns": ["quality"],
    }  # fmt: skip
    run("datasets", "create", "t", "-p", "flood", "--kind", "table",
        "--column", "id:int64!", "--column", "note:string", "--key", "id")  # fmt: skip
    assert api.sent[-1][2] == {
        "type": "table",
        "name": "t",
        "key_columns": ["id"],
        "columns": [
            {"name": "id", "type": "int64", "required": True},
            {"name": "note", "type": "string"},
        ],
    }
    run("datasets", "create", "xw", "-p", "flood", "--kind", "crosswalk",
        "--left", "public.usgs", "--right", "my_obs")  # fmt: skip
    assert api.sent[-1][2] == {"type": "crosswalk", "name": "xw", "left": "public.usgs",
                               "right": "my_obs"}  # fmt: skip
    bad = run("datasets", "create", "t", "-p", "flood", "--kind", "table", "--column", "id")
    assert bad.exit_code == 2 and "NAME:TYPE" in bad.output


def test_dataset_write_writes_and_delete(api, tmp_path):
    login()
    path = tmp_path / "rows.csv"
    path.write_text(
        "series_key,value_time,value\na,2026-01-01T00:00:00Z,1.5\nb,2026-01-01T00:00:00Z,2\n"
    )
    result = run("-o", "json", "datasets", "write", "my_obs", str(path), "-p", "flood",
                 "--unit", "[cft_i]/s")  # fmt: skip
    assert result.exit_code == 0, result.output
    method, sent_path, table = api.sent[-1]
    assert (method, sent_path, table.num_rows) == (
        "POST",
        "/v1/projects/flood/datasets/my_obs/data",
        2,
    )
    assert json.loads(result.stdout)["rows"] == 2
    assert "upsert" in run("datasets", "writes", "my_obs", "-p", "flood").output
    assert run("datasets", "delete", "my_obs", "-p", "flood", "-y").exit_code == 0
    assert api.sent[-1][:2] == ("DELETE", "/v1/projects/flood/datasets/my_obs")
