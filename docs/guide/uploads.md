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

Several files with `-y` load one after another; give `--name` to load them all into one dataset.

## Timeseries

- **New or existing:** `--name` an existing dataset to add to it; otherwise a new one is made
  (with `--role`, `--variable`, `--statistic` and, for values over a span, `--period`).
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

## Changing the proposed mapping

`--map FIELD=VALUE`, repeatable, changes any part of it:

```
--map value=flow_cfs --map series_key=site_no
--map layout=wide --map series_columns=a,b,c
--map 'attributes=[{"column": "huc_cd", "type": "string"}]'
```

## Following uploads

```
teehr uploads list -p my_project
teehr uploads show <id> -p my_project      # proposed mapping, status, rows, problems
teehr uploads wait <id> -p my_project
teehr uploads cancel <id> -p my_project    # it can be loaded again afterwards
teehr uploads delete <id> -p my_project    # the upload and its staged file, not loaded data
```
