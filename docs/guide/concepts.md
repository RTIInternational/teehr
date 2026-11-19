# Concepts

## Projects

A **project** is a workspace you share with others: its own datasets, evaluations and runs, and
its own members. Each member has a role:

| Role | Can |
| --- | --- |
| `read` | read the project's datasets, evaluations and results |
| `write` | also upload data, save evaluations and start runs |
| `admin` | also manage members and the project |

The **`public`** project holds data everyone signed in can read (observations, model outputs,
crosswalks); the platform writes it. Anywhere a dataset is named (a spec, a CLI command),
`public.<name>` is one of `public`'s and a bare `<name>` is the current project's. So comparing
your own forecasts with public observations is an ordinary evaluation in your project.

## Datasets

A **dataset** is one table. There are a few kinds:

- **Timeseries:** values over time from one source, e.g. `usgs_streamflow` (observed) or
  `nwm31_short_range` (forecast). A timeseries holds **one variable, in one unit**, with one
  meaning per value; that's recorded on the dataset, not on every row:
  - **role:** `observed`, `simulated` or `forecast`;
  - **variable:** e.g. `streamflow`, from the variable registry, with its canonical unit;
  - **unit:** values are stored in the variable's canonical unit (converted on upload);
  - **statistic:** what a value is: `inst` (a reading at an instant), or `mean`, `sum`, `min`,
    `max` over its **period** (e.g. `hour_starting`).

  Every row has `series_key`, `value_time` and `value`; forecasts add `reference_time` (when the
  forecast was issued); ensembles `member`; probabilistic forecasts `quantile`; observations may
  have `quality` codes.
- **Series table:** one row per series of a timeseries (made with it, named `<name>_series`): its
  attributes, and where it is (`location_key` in a layer).
- **Layer:** locations with geometry, e.g. gages or basins, keyed by `location_key`.
- **Crosswalk:** which series of one timeseries go with which of another (`left_key`,
  `right_key`), e.g. a USGS gage to its NWM reach.
- **Table:** anything else, including what runs write (results, pairs, state).

A **series** is one time series in a dataset, named by its **series key**, e.g. `usgs-01646500`
(one gage) or `nwm31-8786017` (one NWM v3.1 reach). Keys say where they come from, including the
model version, so a crosswalk for one NWM version can't silently pair another's reaches.

```mermaid
erDiagram
    TIMESERIES }o--|| SERIES_TABLE : "series_key"
    SERIES_TABLE }o--o| LAYER : "location_key"
    CROSSWALK }o--|| SERIES_TABLE : "left_key"
    CROSSWALK }o--|| SERIES_TABLE : "right_key"

    TIMESERIES {
        string series_key "required"
        timestamp value_time "required"
        double value "null: missing"
        timestamp reference_time "forecasts: issue time"
        string member "ensembles"
        decimal quantile "probabilistic, 0 to 1"
        string quality "the source's codes"
    }
    SERIES_TABLE {
        string series_key "one row per series"
        string location_key "where it is"
        string location_layer "in which layer"
        any attributes "name, drainage area, ..."
    }
    LAYER {
        string location_key "e.g. the gage number"
        string name
        binary geometry "point, line or polygon"
        any attributes "state, huc, ..."
    }
    CROSSWALK {
        string left_key "e.g. a USGS gage's series key"
        string right_key "e.g. its NWM reach's series key"
    }
```

`teehr datasets show public.usgs_streamflow` prints a dataset's metadata and columns;
`teehr datasets read public.usgs_streamflow_series` its series.

## Variables and units

A **variable** is what's measured, e.g. `streamflow`, with a **canonical unit** (`m3/s`) its
values are stored in. Values arrive in any unit that converts to it (an upload's `--unit
'[cft_i]/s'` is converted as it's stored); one that doesn't convert is refused, so streamflow
can't be loaded in `mm`.

Units are written in [UCUM](https://ucum.org/), the standard unit codes: `m3/s`, `[cft_i]/s`
(cubic feet per second), `mm`, `Cel` (°C), `[degF]`. An evaluation compares values in one unit:
its `output.unit`, or the observed dataset's.

```
teehr variables list         # each variable and the unit it's stored in
teehr units list             # every unit: its dimension and conversion to SI
```

A project can add its own variables, units and periods (`teehr variables add`, `units add`,
`periods add`); everyone's are listed with `-p <project>`.

## Periods: the time a value covers

A reading covers an instant; an hourly mean covers an hour. A dataset's **period** says which:

| Period | A value at `value_time` covers |
| --- | --- |
| `instant` | that instant |
| `hour_starting` | the hour starting then (10:00 means 10:00 to 11:00) |
| `hour_ending` | the hour ending then (11:00 means 10:00 to 11:00) |
| `day_starting`, `day_ending` | the day starting or ending then |

**Values pair only when they cover the same span of time.** So an hourly mean labelled by the
hour's start pairs with one labelled by its end, and an instantaneous reading doesn't pair with
an hourly mean at the same `value_time`. `teehr periods list` shows them all; a project can add
its own, e.g. a season: `teehr periods add apr_jul --form absolute --start --04-01 --end --07-31`.

## Layers and places

A **layer** holds places with geometry: gages as points, reaches as lines, basins as polygons.
A timeseries' series are *at* places: each series' `location_key` in its series table names its
place in the dataset's layer (`location_layer`).

That lets an evaluation work by place:

- group or filter by an attribute of each series' place (`{name: state, fn: attribute,
  attribute: state_cd, of: location}`);
- group by the polygon each place is in (`{name: basin, fn: within, layer: huc8}`), e.g. KGE per
  basin;
- give results their place, to map them (`output.geometry`; see
  [Evaluations and runs](evaluations.md#places-on-results)).

## Crosswalks

A **crosswalk** says which secondary series go with which primary series: `left_key` is the
primary side (e.g. `usgs-01646500`), `right_key` the secondary (`nwm31-4512772`). A spec names it
in `pair: {relation: public.usgs_nwm31}`. It must link the series tables of the evaluation's
inputs; a crosswalk for other series (say NWM 3.0's) is refused when you plan:

```
pair.relation: relation 'public.usgs_nwm30' links public.usgs_streamflow_series to
public.nwm30_short_range_series, not the primary input's series to every secondary's
```

When both sides use the same keys (your model run at the gages), `pair: {on_attribute:
series_key}` needs no crosswalk.

## Evaluations

An **evaluation** is a spec saved under a name in a project: what to compare, how to pair it, and
which metrics to compute for which groups. Saving a changed spec makes a new **version**; versions
never change, so a result always says exactly what produced it.

Every evaluation goes through the same steps:

`inputs → pair → fields → filter → aggregate → output`

- **inputs:** the observed (primary) dataset and one or more simulated or forecast (secondary)
  ones, each optionally filtered.
- **pair:** match secondary series to primary series (through a crosswalk, or a shared
  attribute), convert units, and match values at the same time. Each match is a **pair**.
- **fields:** derived columns, e.g. a lead-time bin or the month.
- **filter:** drop pairs, e.g. low flows.
- **aggregate:** group the pairs and compute metrics per group, e.g. KGE per gage and product;
  later stages can summarize earlier ones (the median KGE across gages).
- **output:** the unit results are in, and whether to keep the pairs.

The [spec guide](https://github.com/RTIInternational/teehr-core/blob/main/docs/spec/README.md)
documents each step.

## Runs and results

A **run** computes one version of an evaluation as a job in the cloud, acting as you: it reads
the data you can read, with your permissions. A version runs one at a time.

```mermaid
stateDiagram-v2
    [*] --> queued: teehr runs start
    queued --> running: its job starts
    running --> succeeded
    running --> failed: why is in error and the log
    queued --> cancelled: teehr runs cancel
    running --> cancelled: teehr runs cancel
    succeeded --> [*]
    failed --> [*]
    cancelled --> [*]
```

Each version writes three tables in its project:

| Table | Holds |
| --- | --- |
| `results_<name>_v<version>` | the metrics, **every run's rows** (`run_id` says which run) |
| `pairs_<name>_v<version>` | the pairs, the latest run's (when the spec keeps pairs) |
| `state_<name>_v<version>` | per month, what the next run builds on (see below) |

`teehr runs results <run id>` reads one run's results; `teehr datasets read
results_<name>_v<version>` reads the whole table, every run's rows.

Runs are **incremental** when they can be: after the first run, a run recomputes only the months
whose input data changed since the last one and merges them with the kept state, so rerunning
after new data arrives is quick. `--full` recomputes everything. A run's result says which it
did, and why when it was full.

## Personal tokens

`teehr auth login` makes a **personal token**: a secret that acts as you, saved on your computer.
Each computer, script or CI job gets its own, which you can list and revoke. See
[Tokens and access](tokens.md).
