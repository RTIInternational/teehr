# Uploading data

Load your own files into a project: observations, model output, forecasts, the locations they're
at, and the crosswalks between them. You need the `write` role in the project.

| Kind | What it is | Files |
| --- | --- | --- |
| `timeseries` | values over time | CSV, Parquet |
| `layer` | locations with geometry | GeoJSON, zipped Shapefile, GeoPackage, Parquet, CSV (longitude/latitude columns or WKT) |
| `crosswalk` | which series go with which | CSV, Parquet |

## How an upload works

```
teehr uploads add obs.csv -p my_project --kind timeseries
```

1. **The file is sent** to staging storage (not through the API, so large files are fine; up to
   5 GB per file).
2. **It's inspected:** the start of the file is read and `teehr` shows how its columns map to the
   dataset's (`series_key`, `value_time`, `value`, ...), notes on what it noticed, and anything
   **still needed**, with the option that gives it:

   ```
   Upload c81d4586-... (obs.csv, 144 rows): inspected
   Proposed mapping: series_key=site_no, value_time=datetime, value=discharge, quality=qualifiers
   Proposed dataset: name=upload_obs
     note: 'datetime' times have no zone offset; set mapping.timezone (e.g. UTC or America/Denver)
     needed: mapping.timezone  (--timezone ZONE)
     needed: dataset.role  (--role ...)
     needed: dataset.variable  (--variable ...)
     needed: dataset.unit  (--unit ...)
     needed: dataset.statistic  (--statistic ...)
   Load it with: teehr uploads ingest c81d4586-... -p my_project [options]
   ```

3. **You load it**, giving what's needed and changing anything proposed:

   ```
   teehr uploads ingest c81d4586-... -p my_project --name usgs_obs --role observed \
     --variable streamflow --unit '[cft_i]/s' --statistic inst --timezone America/New_York --wait
   ```

   The load runs as a job acting as you. The **whole file is checked** before anything is
   written (types, required values, time zones, repeated keys), and every problem is reported.
   Then it's written in **one commit**: a load that fails leaves the dataset as it was.

`uploads add ... -y` loads straight after inspecting, when you already know what's needed:

```
teehr uploads add obs.csv -p my_project --kind timeseries --name usgs_obs --role observed \
  --variable streamflow --unit '[cft_i]/s' --statistic inst --timezone UTC -y --wait
```

Several files with `-y` load one after another, and stop at the first that fails; give `--name`
to load them all into one dataset.

When a load succeeds, its result says what it did:

```
id: c81d4586-2a03-41f1-a618-6b41be63324e
status: succeeded
rows_done: 144
result: {"dataset": "usgs_obs", "created": true, "rows": 144, "snapshot_id": 6230135544349836096,
"series_added": 3}
```

`created`: whether it made the dataset; `series_added`: new series added to its series table;
with locations, `locations_set`; and `notes` on anything it noticed (e.g. location keys that
aren't in the layer). `teehr uploads logs <id>` shows each step.

## Timeseries

- **Names to use:** `teehr variables list` (and the unit each is stored in), `teehr units list`
  and `teehr periods list` show what `--variable`, `--unit` and `--period` accept; `-p
  my_project` adds the project's own. A project can add its own: `teehr variables add`, `units
  add`, `periods add` (`--help` for each).
- **New or existing:** `--name` an existing dataset to add to it; otherwise a new one is made
  (with `--role`, `--variable`, `--statistic` and, for values over a span, `--period`). Adding to
  an existing dataset keeps its role, variable, statistic and period; the file's values may be in
  any unit that converts to its variable's.
- **Units:** `--unit` is the unit of the values in the file (UCUM, e.g. `[cft_i]/s`, `m3/s`).
  They're converted to the variable's canonical unit as they're stored.
- **Times:** times with a zone offset (`2026-01-01T00:00:00Z`, `...-05:00`) are used as they are.
  Times without one need `--timezone` (an IANA name such as `UTC` or `America/Denver`); there's no
  default. Times that aren't ISO 8601 need their format: `--map time_format='%m/%d/%Y %H:%M'`.
- **Loading again is safe:** rows with the same key (`series_key`, `value_time`, and
  `reference_time`, `member`, `quantile` where present) are replaced, the rest added. Reloading a
  corrected file fixes it; loading the next month adds it.
- **One series without a key column:** `--map series_key_value=usgs-01646500`.
- **Wide files** (a column per gage): inspect proposes `layout=wide` with the value columns as
  `series_columns`, each named by its series key.
- **Locations:** `--layer gauges` names the layer the series are in, and `--map
  location_key=site_no` the column with each series' location (often the series key column).
  Keys not in the layer are reported, not refused.
- **New datasets are partitioned by month** of their time column; `--partition year` (or `none`)
  suits long series read whole.

## Layers and crosswalks

```
teehr uploads add gauges.geojson -p my_project --kind layer -y --wait
teehr uploads add usgs_to_model.csv -p my_project --kind crosswalk \
  --left public.usgs_streamflow --right my_model -y --wait
```

- **Layers:** coordinates are reprojected to longitude/latitude (EPSG:4326). A file that doesn't
  say its coordinate system needs `--map crs=EPSG:26918`. Other columns become attributes.
- **Crosswalks:** `--left` and `--right` name the timeseries whose series the keys are. Keys not
  (yet) in them are reported, so timeseries and their crosswalk load in either order.

### Example: your model against USGS gages

Your model's output at its own reach IDs, and a crosswalk from the gages to them:

```
usgs_to_model.csv
primary_location_id,secondary_location_id
usgs-01646500,mymodel-1001
usgs-02087500,mymodel-2040
```

```
teehr uploads add model_flows.csv -p my_project --kind timeseries --name my_model \
  --role simulated --variable streamflow --unit m3/s --statistic inst --timezone UTC -y --wait
teehr uploads add usgs_to_model.csv -p my_project --kind crosswalk --name usgs_my_model \
  --left public.usgs_streamflow --right my_model -y --wait
```

Then a spec pairs them through it:

```yaml
inputs:
  primary: {dataset: public.usgs_streamflow}
  secondary: [{dataset: my_model}]
pair: {relation: usgs_my_model}
```

If your model's series keys are the gages' own (`usgs-01646500`), no crosswalk is needed:
`pair: {on_attribute: series_key}`.

### Example: metrics per basin

Load the basins as a polygon layer, then group by the basin each gage is in:

```
teehr uploads add huc8.geojson -p my_project --kind layer --name huc8 -y --wait
```

```yaml
fields:
  - {name: basin, fn: within, layer: huc8}
aggregate:
  - group_by: [basin, secondary_dataset]
    metrics: [kge]
output:
  geometry: basin          # results come with each basin's polygon
```

The gages' places come from the observed dataset's layer (`public.usgs_streamflow` has one).

Leading zeros in keys are kept (`01646500` stays text), whatever the file's columns look like.

## Changing the proposed mapping

`--map FIELD=VALUE`, repeatable, changes any part of it:

```
--map value=flow_cfs --map series_key=site_no
--map layout=wide --map series_columns=a,b,c
--map 'attributes=[{"column": "huc_cd", "type": "string"}]'
```

Every field (`--timezone` is `--map timezone=...`):

| Field | For | Is |
| --- | --- | --- |
| `layout` | timeseries | `long` (a row per value) or `wide` (a column per series) |
| `series_key` | timeseries | the column with series keys |
| `series_key_value` | timeseries | one series key for every row (a file of one series) |
| `series_columns` | timeseries, wide | the value columns, each named by its series key |
| `value_time`, `value` | timeseries | the columns with the times and the values |
| `reference_time` | forecasts | the column with each forecast's issue time |
| `member` | ensembles | the column with the ensemble member |
| `quantile` | probabilistic | the column with the quantile level, 0 to 1, at most 4 decimals |
| `quality` | timeseries | the column with the source's quality codes |
| `location_key` | timeseries, layers | each series' (or place's) location key |
| `timezone` | timeseries | IANA zone of times without one (no default) |
| `time_format` | timeseries | `strptime` format of times that aren't ISO 8601 |
| `name`, `geometry`, `longitude`, `latitude` | layers | the place's name, and its geometry (WKB or WKT) or point coordinates |
| `crs` | layers | the file's coordinate system, when it doesn't say (e.g. `EPSG:26918`) |
| `source_layer` | layers | which layer of a GeoPackage |
| `attributes` | layers | the columns to keep as attributes, with names and types |
| `left_key`, `right_key` | crosswalks | the columns with each side's series keys |

## Common problems

The whole file is checked before anything is written, and every problem is listed:

| Problem | Looks like | Fix |
| --- | --- | --- |
| Times without a zone | needed: `mapping.timezone` | `--timezone America/New_York` (or `UTC`) |
| **Repeated keys** | `12 key values (series_key, value_time) occur more than once in the file` | Often local times across the fall daylight-saving change: 01:30 happens twice. Load with UTC times, or check the source |
| Times not ISO 8601 | needed: `mapping.time_format` | `--map time_format='%m/%d/%Y %H:%M'` |
| Missing keys or times | `column 'value_time' is required but has 3 nulls` | Fix or drop those rows |
| An unknown unit | `unknown unit 'cfs'` | Use its UCUM code (`[cft_i]/s`); `teehr units list` |
| A unit that doesn't convert | `mm (length) doesn't convert to m3/s (volume_flow)` | The values aren't that variable, or the wrong `--unit` |
| Quantiles too precise | `quantile: values must have at most 4 decimal places` | Round them |
| Layer coordinates that aren't longitude/latitude | the load fails on them | `--map crs=EPSG:...` with the file's system |

## Small writes without an upload

A dataset can also be made and written directly, for small files already in its columns:

```
teehr datasets create my_obs -p my_project --kind timeseries --role observed \
  --variable streamflow --statistic inst --optional quality --layer gauges
teehr datasets write my_obs rows.csv -p my_project --unit '[cft_i]/s'
```

`datasets write` takes `.csv`, `.parquet`, `.json` or `.arrow`, up to 64 MiB, written in one
commit (`--mode upsert|append|overwrite`). Uploads are for anything larger, or that needs mapping.
`teehr datasets update <name> -p <project> --description "..."` changes a dataset's description;
`teehr datasets delete` removes a dataset and its data.

## Following uploads

```
teehr uploads list -p my_project
teehr uploads show <id> -p my_project      # proposed mapping, status, rows, problems
teehr uploads wait <id> -p my_project
teehr uploads logs <id> -p my_project --follow  # the load's log: read, checked, written
teehr uploads cancel <id> -p my_project    # it can be loaded again afterwards
teehr uploads delete <id> -p my_project    # the upload and its staged file, not loaded data
```
