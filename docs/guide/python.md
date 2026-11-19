# Python

Everything the CLI does is a call on `teehr.Client`; the CLI is a thin layer on it.

```python
from pathlib import Path

from teehr import Client

client = Client.from_config()   # the token `teehr auth login` saved (or TEEHR_API_URL / TEEHR_TOKEN)
client.list_projects()
```

Or give the API and a token directly: `Client("https://control-plane.teehr.rtiamanzi.org",
"tpat_...")`.

## Reading data

Reads return Arrow tables (`pyarrow.Table`); `.to_pandas()` turns one into a DataFrame (with
pandas installed: `pip install pandas`).

```python
obs = client.read(
    "public", "usgs_streamflow",
    filter="series_key = 'usgs-01646500' AND value_time >= '2026-10-01'",
    columns=["value_time", "value"],
)
df = obs.to_pandas()

datasets = client.list_datasets("public")
client.get_dataset("public", "usgs_streamflow")       # kind, metadata, columns
```

## Evaluations and runs

```python
import yaml

spec = yaml.safe_load(Path("nwm-vs-usgs.yaml").read_text())
client.plan("my_project", spec)                        # raises TeehrError listing the problems
client.save_evaluation("my_project", "nwm31_vs_usgs", spec)

run = client.start_run("my_project", "nwm31_vs_usgs")  # full=True to recompute everything
run = client.wait_run("my_project", run["id"])
results = client.run_results("my_project", run["id"]).to_pandas()
```

## Uploads

```python
upload = client.upload_file("my_project", Path("obs.csv"), "timeseries")
print(upload["preview"]["needed"])                     # what the load still needs

upload = client.ingest_upload(
    "my_project", upload["id"],
    dataset={"name": "usgs_obs", "role": "observed", "variable": "streamflow",
             "unit": "[cft_i]/s", "statistic": "inst"},
    mapping={"timezone": "America/New_York"},
)
upload = client.wait_upload("my_project", upload["id"])
```

## Errors

A refused request raises `teehr.TeehrError`, with the HTTP `status` and the API's `detail`;
`teehr.NotSignedIn` when there's no token.
