# Reading data and results

## A run's results

```
teehr runs results <run id> -p my_project
teehr runs results <run id> -p my_project -f "kling_gupta_efficiency < 0" -n 50
teehr runs results <run id> -p my_project --out results.parquet
```

This is one run's rows of its version's results table: one row per group of the evaluation's
last aggregation stage, with a column per metric. Printed, the columns that are the same on every
row (`run_id`, `spec_version`, `engine_version`) are left out; `--columns` (`-c`) asks for them,
and `-o json` and `--out` keep everything.

The run ID is in `teehr runs list <evaluation> -p my_project`.

When the spec has `output.geometry` ([Places on results](evaluations.md#places-on-results)),
results have places: `--out results.geojson` writes a GeoJSON feature collection for a map, and
`.parquet` / `.arrow` keep a `geometry` column (WKB; GeoParquet). Printed tables leave the
geometry out; `--no-geometry` leaves it (and `output.attributes`) out of files too.

## Any dataset

```
teehr datasets list -p my_project                 # default: public
teehr datasets show public.usgs_streamflow         # kind, metadata, columns
teehr datasets read public.usgs_streamflow -n 20
teehr datasets read public.usgs_streamflow -c series_key,value_time,value \
  -f "series_key = 'usgs-01646500' AND value_time >= '2026-10-01'"
teehr datasets read pairs_nwm31_by_lead_v1 -p my_project --out pairs.parquet
```

**Results tables hold every run's rows.** `teehr datasets read results_<name>_v<version>` shows
all of them, `run_id` saying which run each row is from; `teehr runs results` shows one run's.
Pairs and state tables hold the latest run's.

## Write history

```
teehr datasets writes public.usgs_streamflow -n 10
```

Each write to a dataset: when, by whom, the mode, rows, the months of data it touched, the unit
it came in, and how many series it added. Incremental runs use the months to know what changed.

## Options

| Option | |
| --- | --- |
| `-f`, `--filter` | rows to read, in the same expression language as specs, e.g. `"series_key IN ('usgs-01646500', 'usgs-02087500')"`. Numbers are in the dataset's stored unit |
| `-c`, `--columns` | comma-separated columns |
| `-n`, `--limit` | at most this many rows; printing stops at 1,000 by default |
| `--out FILE` | write every row (or `-n` of them) to `.parquet`, `.csv` or `.arrow` |
| `-o json` / `-o csv` | print JSON or CSV instead of a table (before the command: `teehr -o json datasets read ...`) |

A read sees one snapshot of the table from its first page to its last, so writes landing
meanwhile don't shift or repeat rows.
