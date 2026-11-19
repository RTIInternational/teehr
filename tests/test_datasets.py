import json

import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from typer.testing import CliRunner

from teehr import Client
from teehr.cli import app
from tests.conftest import API

runner = CliRunner()


def run(*args):
    return runner.invoke(app, list(args))


def test_read_pages_with_the_cursor_into_one_table(api):
    run("--api", API, "auth", "login")
    table = Client.from_config().read("flood", "results_kge_v1")
    assert table.num_rows == 250 and api.reads == 3  # 100-row pages
    some = Client.from_config().read("flood", "results_kge_v1", limit=120, columns=["kge"])
    assert some.num_rows == 120 and some.column_names == ["kge"]


def test_read_prints_a_limited_table_and_says_so(api):
    run("--api", API, "auth", "login")
    result = run("datasets", "read", "results_kge_v1", "-p", "flood", "-n", "5")
    assert result.exit_code == 0, result.output
    assert "usgs-004" in result.output and "usgs-005" not in result.output
    assert "first 5 rows; there are more" in result.output


def test_read_with_a_filter_and_json(api):
    run("--api", API, "auth", "login")
    result = run("-o", "json", "datasets", "read", "results_kge_v1", "-p", "flood",
                 "-f", "primary_series_key = 'usgs-010'")  # fmt: skip
    assert json.loads(result.stdout) == [{"primary_series_key": "usgs-010", "kge": 0.04}]


def test_read_writes_every_row_to_a_file(api, tmp_path):
    run("--api", API, "auth", "login")
    out = tmp_path / "r.parquet"
    assert (
        run("datasets", "read", "results_kge_v1", "-p", "flood", "--out", str(out)).exit_code == 0
    )
    assert pq.read_table(out).num_rows == 250
    csv_out = tmp_path / "r.csv"
    run("datasets", "read", "results_kge_v1", "-p", "flood", "--out", str(csv_out), "-n", "10")
    assert pacsv.read_csv(csv_out).num_rows == 10


def test_list_and_show(api):
    run("--api", API, "auth", "login")
    assert "results_kge_v1" in run("datasets", "list", "-p", "flood").output
    shown = run("datasets", "show", "results_kge_v1", "-p", "flood").output
    assert "flood.results_kge_v1" in shown and "float64" in shown
    missing = run("datasets", "show", "public.nope")
    assert missing.exception.detail == "No dataset 'nope'"


def test_run_results_reads_the_runs_dataset(api, tmp_path):
    import yaml

    run("--api", API, "auth", "login")
    spec = tmp_path / "s.yaml"
    spec.write_text(yaml.safe_dump({"inputs": {}}))
    run("evaluations", "save", "kge", str(spec), "-p", "flood")
    started = json.loads(run("-o", "json", "runs", "start", "kge", "-p", "flood", "--wait").stdout)
    result = run("runs", "results", started["id"], "-p", "flood", "-n", "3")
    assert result.exit_code == 0, result.output
    assert f"run {started['id']} of kge v1" in result.output and "usgs-002" in result.output
    assert api.results_reads == 1  # the run's own route, not the whole table
    rows = json.loads(run("-o", "json", "runs", "results", started["id"], "-p", "flood").stdout)
    assert len(rows) == 250 and {r["run_id"] for r in rows} == {started["id"]}
