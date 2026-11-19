import json

import yaml
from typer.testing import CliRunner

from teehr import TeehrError
from teehr.cli import app
from tests.conftest import API

runner = CliRunner()
SPEC = {"inputs": {"primary": {"dataset": "public.usgs"}}, "metrics": ["kge"]}


def run(*args):
    return runner.invoke(app, list(args), env={"COLUMNS": "200"})


def spec_file(tmp_path, spec=SPEC, name="spec.yaml"):
    path = tmp_path / name
    path.write_text(yaml.safe_dump(spec))
    return str(path)


def test_plan_shows_the_planned_inputs_and_problems(api, tmp_path):
    run("--api", API, "auth", "login")
    result = run("evaluations", "plan", spec_file(tmp_path), "-p", "flood")
    assert result.exit_code == 0, result.output
    assert "primary: public.usgs" in result.output and "kge" in result.output
    bad = run("evaluations", "plan", spec_file(tmp_path, {"metrics": []}), "-p", "flood")
    assert isinstance(bad.exception, TeehrError)
    assert bad.exception.detail == "inputs: Field required"


def test_save_versions_only_when_the_spec_changes(api, tmp_path):
    run("--api", API, "auth", "login")
    path = spec_file(tmp_path)
    assert "saved as version 1" in run("evaluations", "save", "kge", path, "-p", "flood").output
    assert "unchanged" in run("evaluations", "save", "kge", path, "-p", "flood").output
    changed = spec_file(tmp_path, {**SPEC, "metrics": ["kge", "nse"]}, "v2.json")
    assert "version 2" in run("evaluations", "save", "kge", changed, "-p", "flood").output
    shown = run("evaluations", "show", "kge", "-p", "flood")
    assert yaml.safe_load(shown.stdout)["metrics"] == ["kge", "nse"]
    assert "kge" in run("evaluations", "list", "-p", "flood").output
    assert run("evaluations", "delete", "kge", "-p", "flood", "-y").exit_code == 0
    assert api.evaluations == {}


def test_run_start_wait_and_show(api, tmp_path):
    run("--api", API, "auth", "login")
    run("evaluations", "save", "kge", spec_file(tmp_path), "-p", "flood")
    result = run("-o", "json", "runs", "start", "kge", "-p", "flood", "--full", "--wait")
    assert result.exit_code == 0, result.output
    done = json.loads(result.stdout)
    assert done["status"] == "succeeded" and done["result"]["rows"] == 7053
    assert api.runs[done["id"]]["full"] is True
    assert "succeeded" in run("runs", "show", done["id"], "-p", "flood").output
    assert done["id"] in run("runs", "list", "kge", "-p", "flood").output


def test_a_cancelled_run_exits_nonzero(api, tmp_path):
    run("--api", API, "auth", "login")
    run("evaluations", "save", "kge", spec_file(tmp_path), "-p", "flood")
    started = json.loads(run("-o", "json", "runs", "start", "kge", "-p", "flood").stdout)
    assert "cancelled" in run("runs", "cancel", started["id"], "-p", "flood").output
    assert run("runs", "show", started["id"], "-p", "flood").exit_code == 1


def test_a_spec_must_be_a_mapping(api, tmp_path):
    run("--api", API, "auth", "login")
    path = tmp_path / "list.yaml"
    path.write_text("- a\n- b\n")
    result = run("evaluations", "plan", str(path), "-p", "flood")
    assert result.exit_code == 2 and "mapping" in result.output


def test_plan_and_save_show_warnings(api, tmp_path):
    run("--api", API, "auth", "login")
    api.PLAN = {**api.PLAN, "warnings": ["every run is full: spearman needs every pair"]}
    planned = run("evaluations", "plan", spec_file(tmp_path), "-p", "flood")
    assert "warning: every run is full" in planned.output
    saved = run("evaluations", "save", "kge", spec_file(tmp_path), "-p", "flood")
    assert "warning: every run is full" in saved.output


def test_run_logs_print_and_follow(api):
    run("--api", API, "auth", "login")
    once = run("runs", "logs", "r1", "-p", "flood")
    assert once.exit_code == 0, once.output
    assert "2026-11-19 18:00:00 [run-0]  planned public.usgs" in once.output
    assert "shard 2/2" not in once.output  # not there yet; no --follow
    api.log_polls = 0
    followed = run("runs", "logs", "r1", "-p", "flood", "--follow")
    assert "shard 1/2" in followed.output and "shard 2/2 (try 2)" in followed.output
    assert "[after a lost pod]" in followed.output  # text, not markup
    assert followed.output.count("planned public.usgs") == 1  # each line once
    api.log_polls = 0
    lines = json.loads(run("-o", "json", "uploads", "logs", "u1", "-p", "flood", "-F").stdout)
    assert [line["id"] for line in lines] == [1, 2, 3]
