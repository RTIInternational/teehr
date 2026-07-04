# NWM Points Pipeline — VirtualiZarr + Batch Reads

## Context

Current pipeline:
`nwm_to_parquet()` → `generate_json_paths()` (kerchunk: S3 pre-built + local cache + auto) → `fetch_and_format_nwm_points()` → `process_chunk_of_files()` → `file_chunk_loop()` (dask.delayed ×N) → `dask.compute()` → write parquet

**Bottlenecks:**
1. N separate zarr store opens per chunk — N metadata reads, no batching across files
2. For `kerchunk_method="local"`: N JSON disk writes, O(N)
3. `dask.delayed` scheduling overhead for I/O-bound tasks

**Keep unchanged:** `check_if_files_exist`, `generate_json_paths`, `gen_json`, `build_zarr_references`, `build_remote_nwm_filelist` — all S3/local JSON reference management and GCS file listing stays intact.

---

## Approach: Two paths, unified batch-open tail

**Path A — kerchunk with batch improvement** (`json_dir` provided, existing behavior):
`generate_json_paths()` unchanged → returns mix of S3/local JSON paths → per chunk: open each JSON as a VirtualiZarr virtual dataset via `filetype="kerchunk"` → `xr.concat` → single zarr open

**Path B — VirtualiZarr direct** (`json_dir=None`, new default):
skip `generate_json_paths()` entirely → per chunk: open each GCS HDF5 path as a VirtualiZarr virtual dataset → `xr.concat` → single zarr open

Both paths share a single `_open_virtual_ref(filepath, ignore_missing_file)` helper that detects the path type (`.json`/`.parq` → `filetype="kerchunk"`; GCS `.nc` → HDF5 scan with `indexes={}`). The `xr.concat` → single zarr open tail is identical for both.

**Why Path A is still valuable:** S3 pre-built JSON reads are much faster than re-reading GCS HDF5 headers for large backlogs. The `check_if_files_exist` async check remains the fast path for sourcing references. The batch-open improvement still benefits Path A by eliminating N separate zarr store opens.

---

## Files to Modify

| File | Change |
|------|--------|
| `pyproject.toml` | Add `virtualizarr` dependency |
| `src/teehr/fetching/utils.py` | Add three public utilities (`open_virtual_ref`, `open_virtual_refs_parallel`, `concat_and_open_virtual_datasets`) and `parse_nwm_forecast_gcs_paths()`; remove experimental comment block |
| `src/teehr/fetching/nwm/point_utils.py` | Replace `file_chunk_loop` + `dask.compute` with batch approach in `process_chunk_of_files()`; update `fetch_and_format_nwm_points()` param name |
| `src/teehr/fetching/nwm/nwm_points.py` | Make `json_dir` optional; add routing branch |

---

## Design Principles for New Code

All new and refactored code must follow these rules:

- **Generic over specific**: utility functions in `utils.py` must be NWM-agnostic — no hardcoded bucket names, credential formats, or domain assumptions. NWM-specific logic stays in `nwm/point_utils.py`.
- **Full public API**: new functions in `utils.py` are public (no leading underscore), fully type-annotated, and have docstrings. They are importable externally.
- **Credentials as parameters**: reader options / storage options passed in, never hardcoded.
- **Single responsibility**: each function does one thing. The open → concat → materialize pipeline is broken into three composable functions, not inlined.
- **DRY**: the parallel-open + concat + materialise pattern is extracted once and called from `process_chunk_of_files()`; it is not duplicated elsewhere.

---

## Steps

### Phase 1 — Dependency
1. Add `virtualizarr = ">=1.0,<2.0"` to `pyproject.toml` dependencies (it is currently imported in `utils.py` but untracked).

### Phase 2 — `utils.py` (parallel with Phase 1)

**2a. Add `parse_nwm_forecast_gcs_paths(gcs_paths: List[str]) -> pd.DataFrame`**
- Parses `day` and `z_hour` from GCS forecast paths using existing `DAY_PATTERN` + `TZ_PATTERN` regexes.
- Returns `z_hour` in `t00z` format (same as `parse_nwm_json_paths`) so downstream `ref_time` arithmetic is unchanged.
- Distinct from the existing `parse_nwm_gcs_paths()` which is assimilation-only, requires `tm_hour` in filename, and returns z_hour as bare digits.

**2b. Add three composable, NWM-agnostic public utility functions:**

```
open_virtual_ref(
    filepath: str,
    reader_options: Optional[Dict] = None,
    ignore_missing_file: bool = True,
) -> Optional[xr.Dataset]
```
- Detects path type from extension: `.json`/`.parq` → `filetype="kerchunk"`; otherwise → HDF5 scan with `indexes={}`.
- Passes `reader_options` through to `virtualizarr.open_virtual_dataset` — no credentials hardcoded.
- Error handling: `FileNotFoundError` and `OSError` → log warning + return `None` if `ignore_missing_file=True`, raise if `False`. All other exceptions always re-raise.
- Full docstring. Supersedes `_parse_hourly_metadata` and `get_dataset` for the new path.

```
open_virtual_refs_parallel(
    filepaths: List[str],
    reader_options: Optional[Dict] = None,
    ignore_missing_file: bool = True,
    max_workers: int = 64,
) -> Tuple[List[xr.Dataset], List[bool]]
```
- Wraps `ThreadPoolExecutor.map(open_virtual_ref, filepaths)` with a `valid_mask` for caller filtering.
- Returns `(v_datasets, valid_mask)` — caller zips mask against its own DataFrame to stay in sync.
- Generic: works for any list of remote paths (GCS HDF5, S3 JSON, local JSON). No NWM assumptions.
- Full docstring.

```
concat_and_open_virtual_datasets(
    v_datasets: List[xr.Dataset],
    concat_dim: str = "time",
    storage_options: Optional[Dict] = None,
) -> xr.Dataset
```
- `xr.concat(v_datasets, dim=concat_dim)` → write combined VDS to `NamedTemporaryFile(suffix=".parq")` → open with `engine="kerchunk"` + `storage_options` → delete temp file → return open `xr.Dataset`.
- Single responsibility: given a list of virtual datasets, returns one materialised dataset.
- `storage_options` passed in; no hardcoded `target_options`.
- Full docstring. Reusable for any VirtualiZarr batch-open pattern in the codebase.

**2c.** Remove the `# TO REPLACE build_zarr_references()? ------>` experimental comment block (lines ~521–555). The three functions above replace `_parse_hourly_metadata` and `build_hourly_timeseries`.

### Phase 3 — `point_utils.py` (depends on Phase 2)

**3a. Replace the `file_chunk_loop` + `dask.compute` block in `process_chunk_of_files()`** using the three new utilities:

```python
# Parallel virtual reference opens (generic utility)
v_datasets, valid_mask = open_virtual_refs_parallel(
    filepaths=df.filepath.tolist(),
    reader_options={"storage_options": {"token": "anon"}},  # GCS anon or S3 anon depending on path
    ignore_missing_file=ignore_missing_file,
    max_workers=64,
)
df_valid = df[valid_mask].reset_index(drop=True)

if not v_datasets:
    raise FileNotFoundError("No NWM files for specified input configuration were found.")

# Materialise as a single zarr-backed dataset (generic utility)
ds = concat_and_open_virtual_datasets(
    v_datasets=v_datasets,
    concat_dim="time",
    storage_options={"target_options": {"anon": True}},
)
```

**3b. NWM-specific table building stays in `point_utils.py`** (not extracted to a generic utility — it is inherently domain-specific):

```python
ds = ds.sel(feature_id=location_ids)
vals = ds[variable_name].astype("float32").values  # shape: (N_files, N_locations)
nwm_units = ds[variable_name].units
n_files, n_locations = vals.shape

ref_times = [
    pd.to_datetime(r.day) + pd.to_timedelta(int(r.z_hour[1:3]), unit="h")
    for r in df_valid.itertuples()
]
ref_times_arr = np.repeat(ref_times, n_locations)
valid_times_arr = np.repeat(ds.time.values, n_locations)
teehr_location_ids = [f"{nwm_version}-{fid}" for fid in ds.feature_id.values.astype(int)]
location_ids_tiled = np.tile(teehr_location_ids, n_files)
```

The `drop_overlapping_assimilation_values`, filename construction, and `write_timeseries_parquet_file` call are unchanged.

**3c. Keep `file_chunk_loop` intact** — not deleted; can be formally deprecated later.

**3d. Update `fetch_and_format_nwm_points(file_paths: List[str], ...)`:**
- Rename param `json_paths` → `file_paths`.
- Detect path type: paths ending in `.nc` → call `parse_nwm_forecast_gcs_paths`; otherwise → call `parse_nwm_json_paths`.
- All other logic (groupby/split, chunk loop) unchanged.

### Phase 4 — `nwm_points.py` (depends on Phase 3)

**4a.** Change signature: `json_dir: Union[str, Path]` → `json_dir: Optional[Union[str, Path]] = None`

**4b.** Branch after the `start_on_z_hour` / `end_on_z_hour` filters:

```python
if json_dir is None:
    logger.info(
        "json_dir not provided; using VirtualiZarr direct path. "
        "kerchunk_method is ignored."
    )
    fetch_and_format_nwm_points(
        file_paths=gcs_component_paths,
        ...
    )
else:
    json_paths = generate_json_paths(
        kerchunk_method=kerchunk_method,
        gcs_component_paths=gcs_component_paths,
        json_dir=json_dir,
        ignore_missing_file=ignore_missing_file,
    )
    fetch_and_format_nwm_points(
        file_paths=json_paths,
        ...
    )
```

Path A (`json_dir` provided): `generate_json_paths()` runs as today — S3 pre-built lookup via `check_if_files_exist`, local cache re-use, and auto-creation of missing references all work exactly as before.

---

## Verification

1. `uv run pytest tests/` — existing tests pass without modification
2. **Smoke test Path B** (`json_dir=None`) over 1–2 days; confirm parquet schema and values match expected
3. **Smoke test Path A** (`json_dir` + `kerchunk_method="remote"`) — confirm S3 JSON lookup via `check_if_files_exist` + batch open works, output matches Path B
4. **Smoke test Path A** (`json_dir` + `kerchunk_method="local"`) — confirm local JSON cache re-use works
5. Profile wall-clock time for 100+ files vs. baseline to quantify improvement

---

## Decisions

| Item | Decision |
|------|----------|
| `check_if_files_exist` | Untouched — only executed on Path A via `generate_json_paths` |
| `generate_json_paths`, `build_zarr_references`, `gen_json` | Untouched |
| `build_remote_nwm_filelist` | Untouched — sits upstream of the branch, always returns GCS paths |
| `_parse_hourly_metadata` / `build_hourly_timeseries` | Removed (experimental block); superseded by the three new public utilities |
| New utilities (`open_virtual_ref`, `open_virtual_refs_parallel`, `concat_and_open_virtual_datasets`) | Public, NWM-agnostic, fully typed and documented — importable externally |
| `file_chunk_loop` + `@dask.delayed` | Kept, not deleted |
| Temp parquet per chunk | Encapsulated inside `concat_and_open_virtual_datasets`; BytesIO is a follow-up optimization |
| `parse_nwm_gcs_paths` (existing) | Unchanged — assimilation-only, used by `build_remote_nwm_filelist` |

---

## Further Considerations

1. **In-memory temp file**: If `combined_vds.vz.to_kerchunk()` accepts `io.BytesIO`, the one temp-file-per-chunk disk write can be eliminated entirely — easy follow-up once the API is confirmed.
2. **`xr.concat` options**: May need `compat='override'` and/or `data_vars='minimal'` for edge cases where NWM files across different configurations have mismatched coordinate attributes.
3. **VirtualiZarr `reader_options` for S3 JSON (Path A)**: Confirm the correct anonymous-access kwarg when `filetype="kerchunk"` is used with S3 paths — may differ from the GCS `{"token": "anon"}` convention.
4. **`max_workers`**: 64 threads is a reasonable default matching the experimental code and GCS concurrency characteristics; could be exposed as a parameter if needed.
