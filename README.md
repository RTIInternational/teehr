# TEEHR

The Python SDK and CLI for **TEEHR Cloud**: sign in, manage personal tokens, and work with
projects, datasets, uploads and evaluation runs from Python or the terminal.

> This is the `v1` branch: TEEHR 1.0, in development. The current TEEHR package (0.x, Spark)
> is on `main` and frozen until 1.0 replaces it (teehr-cloud-core ADR-0015).

## Install

```
uv tool install "git+https://github.com/RTIInternational/teehr.git@v1"
```

or `pip install "git+https://github.com/RTIInternational/teehr.git@v1"`.

## Sign in

```
teehr auth login --api https://control-plane.<your TEEHR Cloud host>
```

It shows a code and opens your browser; approve, and a personal token for this computer is saved
in `~/.config/teehr/credentials.json` (readable by you only). Then:

```
teehr auth status
teehr projects list
teehr tokens list
teehr auth logout
```

For CI, make a token once (`teehr auth login --name ci --days 30`) and give the job
`TEEHR_API_URL` and `TEEHR_TOKEN`, or save it with `teehr auth login --token tpat_...`.
A local cluster with a self-signed certificate needs `--insecure` at login.

## Upload data

```
teehr uploads add obs.csv -p my_project --kind timeseries
```

It sends the file, then shows how it maps to a dataset and anything still needed, with the
option that gives it. Load it with those, e.g.:

```
teehr uploads ingest <upload id> -p my_project --name usgs_obs --role observed \
  --variable streamflow --unit '[cft_i]/s' --statistic inst --timezone America/New_York --wait
```

or do both at once with `uploads add ... -y --wait`. `teehr uploads list|show|wait|cancel|delete`
manage them; `--map FIELD=VALUE` changes the proposed mapping (e.g. `--map value=flow_cfs`).
`TEEHR_PROJECT` sets a default `-p`. Every command takes `-o json` (or `csv`).

## Evaluations and runs

```
teehr evaluations plan spec.yaml -p my_project        # check it against the datasets
teehr evaluations save my_eval spec.yaml -p my_project  # version 1, then a new version per change
teehr runs start my_eval -p my_project --wait          # incremental unless --full
teehr runs list my_eval -p my_project
```

`teehr evaluations show my_eval -p other > spec.yaml` copies a spec to start from.

## Read data and results

```
teehr runs results <run id> -p my_project                       # the run's metrics
teehr datasets list -p my_project                                # default project: public
teehr datasets show public.usgs_streamflow
teehr datasets read public.usgs_streamflow -n 20 -f "series_key = 'usgs-01646500'"
teehr datasets read pairs_my_eval_v1 -p my_project --out pairs.parquet   # every row
```

An evaluation version's results table (`results_<name>_v<version>`) keeps every run's rows,
`run_id` saying which: `datasets read` shows them all, `runs results` one run's.

Printing stops at 1000 rows (`-n` for more); `--out` writes every row to `.parquet`, `.csv` or
`.arrow`. In Python, `client.read(project, dataset, filter=..., columns=...)` and
`client.run_results(project, run_id)` return Arrow tables.

## From Python

```python
from pathlib import Path

from teehr import Client

client = Client.from_config()  # the token `teehr auth login` saved
client.list_projects()
upload = client.upload_file("my_project", Path("obs.csv"), "timeseries")
print(upload["preview"]["needed"])
```

## Develop

```
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

## License

MIT (see `LICENSE.md`).
