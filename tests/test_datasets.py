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


def test_a_layer_reads_with_its_geometry(api, tmp_path):
    run("--api", API, "auth", "login")
    printed = run("datasets", "read", "public.gauges")
    assert printed.exit_code == 0, printed.output
    assert "POINT (-69.7156 46.7006)" in printed.output
    assert "bbox_xmin" not in printed.output  # bounds left out of the table
    asked = run("datasets", "read", "gauges", "-c", "location_key,bbox_xmin").output
    assert "bbox_xmin" in asked  # unless asked for
    rows = json.loads(run("-o", "json", "datasets", "read", "gauges").stdout)
    assert rows[1]["geometry"] == "POINT (-69.7517 46.8939)" and "bbox_xmin" in rows[1]
    csv_out = tmp_path / "g.csv"
    run("datasets", "read", "gauges", "--out", str(csv_out))
    assert pacsv.read_csv(csv_out)["geometry"][0].as_py() == "POINT (-69.7156 46.7006)"
    out = tmp_path / "g.parquet"
    run("datasets", "read", "gauges", "--out", str(out))
    table = pq.read_table(out)
    assert table["geometry"].type == "binary"  # WKB kept
    assert json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"] == {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "bbox": [-69.7517, 46.7006, -69.7156, 46.8939],
    }


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


def test_run_results_with_places(api, tmp_path):
    import yaml

    run("--api", API, "auth", "login")
    spec = tmp_path / "s.yaml"
    spec.write_text(yaml.safe_dump({"inputs": {}}))
    run("evaluations", "save", "kge", str(spec), "-p", "flood")
    started = json.loads(run("-o", "json", "runs", "start", "kge", "-p", "flood", "--wait").stdout)
    # JSON: geometry as WKT, not bytes.
    rows = json.loads(run("-o", "json", "runs", "results", started["id"], "-p", "flood",
                          "-n", "1").stdout)  # fmt: skip
    assert rows[0]["geometry"] == "POINT (0 0)"
    # Parquet keeps the geometry column, as GeoParquet.
    out = tmp_path / "r.parquet"
    run("runs", "results", started["id"], "-p", "flood", "--out", str(out))
    table = pq.read_table(out)
    assert "geometry" in table.column_names
    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]
    # GeoJSON: a feature collection.
    geo = tmp_path / "r.geojson"
    result = run("runs", "results", started["id"], "-p", "flood", "--out", str(geo))
    assert result.exit_code == 0, result.output
    collection = json.loads(geo.read_text())
    assert collection["type"] == "FeatureCollection" and len(collection["features"]) == 3
    # --no-geometry asks the API to leave places out.
    run("runs", "results", started["id"], "-p", "flood", "--no-geometry", "-n", "1")
    assert api.geometry_asked == "false"


def test_read_in_another_unit_says_so(api):
    run("--api", API, "auth", "login")
    result = run("datasets", "read", "results_kge_v1", "-p", "flood", "-n", "2",
                 "--unit", "[cft_i]/s")  # fmt: skip
    assert result.exit_code == 0, result.output
    assert api.unit_asked == "[cft_i]/s" and "values in [cft_i]/s" in result.output
    table = Client.from_config().read("flood", "results_kge_v1", limit=5, unit="L/s")
    from teehr import unit_of

    assert unit_of(table) == "L/s"


def test_an_api_that_ignores_unit_is_refused_not_trusted(api):
    run("--api", API, "auth", "login")
    api.ignores_units = True
    result = run("datasets", "read", "results_kge_v1", "-p", "flood", "--unit", "L/s")
    assert result.exit_code != 0 and "doesn't convert to 'L/s'" in str(result.exception)
