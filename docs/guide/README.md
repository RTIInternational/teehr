# TEEHR user guide

TEEHR compares simulated and forecast time series against observations and reports how good they
are: National Water Model forecasts against USGS stream gages, your own model against your own
observations, or anything else that comes as time series.

**TEEHR Cloud** keeps the data and runs the evaluations. You work with it through its API, using
the `teehr` command-line tool, the `teehr` Python package, or HTTP directly (the interactive API
docs are at `/docs` on the API's address).

| Page | Read it to… |
| --- | --- |
| [Getting started](getting-started.md) | Install `teehr`, sign in, look at the data and run a first evaluation |
| [Concepts](concepts.md) | Learn the vocabulary: projects, datasets, series, evaluations, runs, results |
| [Uploading data](uploads.md) | Load your own CSV, Parquet or geospatial files into a project |
| [Evaluations and runs](evaluations.md) | Write a spec, check it, save it, run it, and rerun it as data arrives |
| [Reading data and results](reading.md) | Look at datasets and run results, or save them to files |
| [Tokens and access](tokens.md) | Sign in on other computers, use tokens in scripts and CI, revoke them |
| [Python](python.md) | Do all of the above from Python |

Reference:

- **The evaluation spec:** the [spec guide](https://github.com/RTIInternational/teehr-core/blob/main/docs/spec/README.md)
  and its [JSON Schema](https://github.com/RTIInternational/teehr-core/blob/main/docs/spec/evaluation-spec-v1.schema.json),
  in `teehr-core`. The spec is shared with VeriRace, so the guide's examples sometimes use
  `verirace` commands; the TEEHR equivalents are on [Evaluations and runs](evaluations.md).
- **Every command:** `teehr --help`, `teehr <group> --help`, `teehr <group> <command> --help`.
- **The API:** `/docs` on the API's address, e.g. `https://control-plane.teehr.rtiamanzi.org/docs`.

> TEEHR 1.0 is in development on the `v1` branch. The current TEEHR package (0.x, Spark) stays on
> `main` until 1.0 replaces it.
