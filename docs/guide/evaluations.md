# Evaluations and runs

An evaluation is a spec saved under a name in a project; a run computes one of its versions.
The [spec guide](https://github.com/RTIInternational/teehr-core/blob/main/docs/spec/README.md)
documents everything a spec can say; this page covers working with them.

## A spec

```yaml
version: 1
name: nwm31-vs-usgs-by-lead-time
inputs:
  primary:
    dataset: public.usgs_streamflow
  secondary:
    - dataset: public.nwm31_short_range
    - dataset: public.nwm31_medium_range_mem1
      filter: "hour(reference_time) = 0"
pair:
  relation: public.usgs_nwm31
fields:
  - {name: lead_time_bin, fn: lead_time_bins, width: PT6H}
filter: "primary_value > 0"
aggregate:
  - group_by: [primary_series_key, secondary_dataset, lead_time_bin]
    metrics: [count, kling_gupta_efficiency, nash_sutcliffe_efficiency, root_mean_square_error]
output:
  unit: "[cft_i]/s"
  pairs: all
```

- **Datasets** are named as anywhere else: `public.<name>`, or a bare name for the project's own.
- **Metrics** have one canonical name (`nash_sutcliffe_efficiency`) and short aliases (`nse`,
  `kge`, `rmse`); results always use the canonical name.
- **Filters and expressions** are a small SQL subset: comparisons, `AND`/`OR`/`NOT`, `IN`,
  `BETWEEN`, `LIKE`, and functions such as `hour()` and `date_trunc()`.
- **Units:** `output.unit` is the unit results are in (default: the primary dataset's). Any number
  with a unit in the spec says its unit (`{value: 1000, unit: "[cft_i]/s"}`).
- **`output.pairs: all`** keeps the pairs, in `pairs_<name>_v<version>`.

`teehr evaluations show <name> -p <project> > spec.yaml` copies an existing spec to start from.

## Places on results

`output.geometry` gives each result row a place, so results can be mapped:

```yaml
aggregate:
  - group_by: [primary_series_key, secondary_dataset]
    metrics: [kling_gupta_efficiency]
output:
  geometry: primary_series_key     # each gage's location
  attributes: [huc_cd]             # and attributes of the same place, as columns
```

- `primary_series_key`: the observed series' location, from its series table's `location_key`
  in its layer (the dataset's `location_layer` unless a series says otherwise).
- A `within` field's name: the polygon of its layer, e.g. a basin.

The column must be in the last stage's `group_by`. Places are looked up when results are read
(`teehr runs results`): `--out results.geojson` writes a feature collection, `.parquet` and
`.arrow` keep a `geometry` column. A dataset read of the results table has no places: they're
not stored.

## Check, save, run

```
teehr evaluations plan spec.yaml -p my_project
```

**Plan** checks the spec against the project's datasets without saving or running anything. It
prints what the run will read (each input's dataset, role, variable and unit), the crosswalk, and
each aggregation stage's groups and columns, or every problem found: an unknown dataset or
metric, a column that doesn't exist at that step, a unit that doesn't convert, or a feature TEEHR
doesn't support yet (below).

It also prints **warnings** about what the spec does that you may not expect, e.g. that every run
of it is full (and why), so reruns won't be incremental.

```
teehr evaluations save nwm31_by_lead nwm31-by-lead.yaml -p my_project
```

**Save** plans it too, then stores it as version 1. Saving again with a changed spec makes the
next version; saving a spec that means the same (key order, aliases and defaults aside) changes
nothing. Versions never change. Names are lowercase letters, digits and underscores, starting
with a letter, at most 45 characters.

```
teehr runs start nwm31_by_lead -p my_project --wait
```

**Run** computes the latest version (`--version N` for another) as a job that acts as you. A
version runs one at a time: starting another while one is going is refused.

## Rerunning as data arrives

After the first run, runs are **incremental** when they can be: a run recomputes only the months
whose input data changed since the last run (uploads and ingests record the months they touch),
and merges them with the state the last run kept. So a daily rerun after new observations arrive
reads days, not years.

```
teehr runs start nwm31_by_lead -p my_project --wait          # incremental when it can be
teehr runs start nwm31_by_lead -p my_project --full --wait   # recompute everything
```

The run's `mode` says which it did, and why when it was full (e.g. "no earlier run kept state").

## Not supported in TEEHR yet

The spec is shared with VeriRace, so the [spec
guide](https://github.com/RTIInternational/teehr-core/blob/main/docs/spec/README.md) describes
everything either system may run. In TEEHR, today:

| Feature | Status |
| --- | --- |
| `generate: climatology`, `generate: blend` inputs | **refused:** need a climatology dataset, which TEEHR can't make yet ([#251](https://github.com/RTIInternational/teehr-cloud-core/issues/251)) |
| `fn: percentile` field (`from:` a percentiles dataset) | **refused:** needs a percentiles dataset (#251) |
| `fn: naive_scale` field (for MASE) | **refused:** needs a naive_scale dataset (#251) |
| `generate: persistence` | supported |
| Answering small evaluations right away, `--estimate` (VeriRace's `evaluate`) | not in TEEHR: save, then run |
| Datasets of other projects (`<project>.<name>`) | not yet; `public.<name>` and the project's own |

`plan` refuses each with a message saying so. These run, but **every run is full** (no
incremental reruns, and all the data is read in one pod, so very large evaluations may not fit);
`plan` warns about them:

- metrics that need every pair rather than running sums (e.g. `spearman_correlation`);
- `guards`, `reduce` windows, and skill matched on observations;
- some `bootstrap` settings.

## Following runs

```
teehr runs list nwm31_by_lead -p my_project     # newest first
teehr runs show <run id> -p my_project          # status, results table, rows, mode, pairs
teehr runs logs <run id> -p my_project --follow # its log as it goes: plan, shards, writes
teehr runs wait <run id> -p my_project
teehr runs cancel <run id> -p my_project
```

A run's **log** is kept with it: what it planned to read, each shard (from every pod, with
retries), what it wrote, and why it failed if it did. `--follow` prints new lines until the run
ends. A run that fails also says why in `error`. Reading results is on
[Reading data and results](reading.md).

## Managing evaluations

```
teehr evaluations list -p my_project
teehr evaluations show nwm31_by_lead -p my_project               # the latest spec, as YAML
teehr evaluations show nwm31_by_lead -p my_project --version 1
teehr evaluations delete nwm31_by_lead -p my_project             # needs the admin role
```

Deleting an evaluation deletes its versions, runs and their tables, unless `--keep-results`
keeps the tables as datasets of their own.
