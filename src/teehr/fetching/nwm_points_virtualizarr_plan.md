# NWM Points Pipeline — VirtualiZarr + Batch Reads

## Context

Current pipeline:
`nwm_to_parquet()` → `generate_json_paths()` (kerchunk: S3 pre-built + local cache + auto) → `fetch_and_format_nwm_points()` → `process_chunk_of_files()` → `file_chunk_loop()` (dask.delayed ×N) → `dask.compute()` → write parquet

**Bottlenecks:**
1. N separate zarr store opens per chunk — N metadata reads, no batching across files
2. For `kerchunk_method="local"`: N kerchunk JSON disk writes, O(N)
3. `dask.delayed` scheduling overhead for I/O-bound tasks

**Unchanged:** `check_if_files_exist`, `gen_json`, `build_zarr_references`, `build_remote_nwm_filelist`,
`process_by_z_hour`, `stepsize`, `file_chunk_loop` (kept, not called from new path).

**Modified:** `generate_json_paths()` is no longer called from `nwm_to_parquet()`; replaced by new
`resolve_nwm_file_paths()`. The old function stays in `utils.py` untouched.

---

## Approach: Single unified VirtualiZarr path

VirtualiZarr opens all reference types uniformly:
- Existing kerchunk JSON (local or S3) → `open_virtual_dataset(path, filetype="kerchunk")`
- GCS HDF5 directly → `open_virtual_dataset(path, indexes={})`

A single pipeline handles every reference source:

```
nwm_to_parquet()
  → build_remote_nwm_filelist()       # unchanged
  → resolve_nwm_file_paths()          # NEW: returns best-available path per GCS file
  → fetch_and_format_nwm_points()     # receives mixed list (.json / .parq / .nc)
      → group by day+z_hour or chunk by stepsize — unchanged
      → per chunk: process_chunk_of_files()
          open_virtual_refs_parallel()        # parallel header reads, all path types
          concat_and_open_virtual_datasets()  # xr.concat → single zarr open (zarr v3 async)
          NWM table build                     # multi-timestep, same schema
          write_timeseries_parquet_file()     # unchanged
```

---

## Reference Sourcing: `json_dir` + `kerchunk_method`

`json_dir` is always required. It is the single cache directory for all virtual reference file types —
both existing kerchunk `.json` files and new VirtualiZarr `.parq` files. `kerchunk_method` controls
where lookups happen and whether new references are cached:

| `kerchunk_method` | Lookup order | Cache writes |
|---|---|---|
| `"local"` | Check `json_dir` for `.parq` (priority) then `.json` → GCS fallback | New GCS scans saved as `.parq` to `json_dir` |
| `"remote"` | Check S3 via `check_if_files_exist` (single async batch) → **skip** if missing | None |
| `"auto"` | Check S3 → check `json_dir` for `.parq`/`.json` → GCS fallback | New GCS scans saved as `.parq` to `json_dir` |

For `"remote"`, files with no S3 JSON are **skipped** (consistent with current behavior).
`resolve_nwm_file_paths()` returns `None` for those positions; `open_virtual_refs_parallel()` filters them out.

---

## Design Principles for New Code

- **Generic over specific**: the three utility functions in `utils.py` are NWM-agnostic — no hardcoded
  bucket names, credential formats, or domain assumptions. NWM-specific logic stays in `nwm/point_utils.py`.
- **Full public API**: new functions in `utils.py` are public, fully type-annotated, and have docstrings.
  They are importable externally.
- **Credentials as parameters**: `reader_options` and `storage_options` are always passed in.
- **Single responsibility**: `open_virtual_ref` opens one file; `open_virtual_refs_parallel` parallelises;
  `concat_and_open_virtual_datasets` materialises. These are composable, not inlined.
- **DRY**: the parallel-open + concat + materialise pattern is extracted once and called from
  `process_chunk_of_files()`.

---

## Files to Modify

| File | Change |
|---|---|
| `pyproject.toml` | Add `virtualizarr = ">=1.0,<2.0"` |
| `src/teehr/fetching/utils.py` | Add `open_virtual_ref`, `open_virtual_refs_parallel`, `concat_and_open_virtual_datasets`, `resolve_nwm_file_paths`, `parse_nwm_forecast_gcs_paths`; remove experimental comment block |
| `src/teehr/fetching/nwm/point_utils.py` | Replace `file_chunk_loop` + `dask.compute` in `process_chunk_of_files()`; rename `json_paths` → `file_paths` and add `cache_dir` to `fetch_and_format_nwm_points()` |
| `src/teehr/fetching/nwm/nwm_points.py` | Replace `generate_json_paths()` call with `resolve_nwm_file_paths()`; update imports |

---

## Steps

### Phase 1 — Dependency
1. Add `virtualizarr = ">=1.0,<2.0"` to `pyproject.toml` (currently imported in `utils.py` but untracked).

### Phase 2 — `utils.py` (parallel with Phase 1)

**2a. Add `parse_nwm_forecast_gcs_paths(gcs_paths: List[str]) -> pd.DataFrame`**
- Parses `day` and `z_hour` from GCS forecast paths using existing `DAY_PATTERN` + `TZ_PATTERN`.
- Returns `z_hour` in `t00z` format (same as `parse_nwm_json_paths`) so downstream `ref_time`
  arithmetic in `process_chunk_of_files` is unchanged.
- Distinct from the existing `parse_nwm_gcs_paths()` (assimilation-only, returns z_hour as bare digits).

**2b. Add `open_virtual_ref`**
```python
def open_virtual_ref(
    filepath: str,
    reader_options: Optional[Dict] = None,
    ignore_missing_file: bool = True,
    cache_dir: Optional[Path] = None,
) -> Optional[xr.Dataset]:
```
- Detects path type: `.json` or `.parq` → `filetype="kerchunk"`; otherwise (`.nc`) → `indexes={}`.
- Passes `reader_options` through to `virtualizarr.open_virtual_dataset` — no credentials hardcoded.
- When filepath is `.nc` and `cache_dir` is provided: saves the resulting virtual dataset as
  `{date}.{fname}.parq` in `cache_dir` after scanning, enabling future reuse without a GCS header scan.
- Error handling mirrors `get_dataset` exactly:
  - `FileNotFoundError` (missing): log warning + return `None` if `ignore_missing_file=True`, raise if `False`
  - `OSError` (corrupt): log warning + return `None` if `ignore_missing_file=True`, raise if `False`
  - All other exceptions always re-raise regardless of flag

**2c. Add `open_virtual_refs_parallel`**
```python
def open_virtual_refs_parallel(
    filepaths: List[Optional[str]],
    reader_options: Optional[Dict] = None,
    ignore_missing_file: bool = True,
    max_workers: int = 64,
    cache_dir: Optional[Path] = None,
) -> Tuple[List[xr.Dataset], List[bool]]:
```
- Accepts `None` entries in `filepaths` (pre-skipped by `resolve_nwm_file_paths` for `"remote"` mode);
  maps them directly to `None` without calling `open_virtual_ref`.
- Uses `ThreadPoolExecutor.map` over non-null entries for parallel virtual reference creation.
- Returns `(v_datasets, valid_mask)` — caller aligns mask with its DataFrame. No NWM assumptions.

**2d. Add `concat_and_open_virtual_datasets`**
```python
def concat_and_open_virtual_datasets(
    v_datasets: List[xr.Dataset],
    concat_dim: str = "time",
    storage_options: Optional[Dict] = None,
) -> xr.Dataset:
```
- `xr.concat(v_datasets, dim=concat_dim)` → write to `NamedTemporaryFile(suffix=".parq")` via
  `combined_vds.vz.to_kerchunk(tmp, format="parquet")` → open with `engine="kerchunk"` +
  `storage_options` → delete temp file → return open `xr.Dataset`.
- Single responsibility: given a list of virtual datasets, returns one materialised dataset.
- `storage_options` passed in; nothing hardcoded.

**2e. Add `resolve_nwm_file_paths`**
```python
def resolve_nwm_file_paths(
    gcs_paths: List[str],
    kerchunk_method: str,
    json_dir: Path,
    ignore_missing_file: bool,
) -> List[Optional[str]]:
```
- Returns a list of the same length as `gcs_paths`. Each entry is the best available reference path,
  or `None` if the file should be skipped (`"remote"` mode, no S3 JSON found).
- Implements the lookup table in the "Reference Sourcing" section above.
- The S3 check is a single async `check_if_files_exist` batch call (not per-file).
- For `json_dir` lookups, prefers `{date}.{fname}.parq` over `{date}.{fname}.json` (same naming
  convention as current kerchunk JSON files, with `.parq` extension for VirtualiZarr parquet).
- NWM-specific function (knows S3 JSON path convention and `json_dir` naming scheme).

**2f.** Remove the `# TO REPLACE build_zarr_references()? ------>` experimental comment block.
`_parse_hourly_metadata` and `build_hourly_timeseries` are superseded by the new utilities.

### Phase 3 — `point_utils.py` (depends on Phase 2)

**3a. Replace `file_chunk_loop` + `dask.compute` in `process_chunk_of_files()`.**
Add `cache_dir: Optional[Path] = None` parameter.

```python
v_datasets, valid_mask = open_virtual_refs_parallel(
    filepaths=df.filepath.tolist(),
    reader_options={"storage_options": {"token": "anon"}},
    ignore_missing_file=ignore_missing_file,
    cache_dir=cache_dir,
)
df_valid = df[valid_mask].reset_index(drop=True)

if not v_datasets:
    raise FileNotFoundError("No NWM files for specified input configuration were found.")

ds = concat_and_open_virtual_datasets(
    v_datasets=v_datasets,
    concat_dim="time",
    storage_options={"target_options": {"anon": True}},
)
```

**3b. NWM-specific multi-timestep table build** stays in `point_utils.py`:

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

The `drop_overlapping_assimilation_values` check, filename construction, and `write_timeseries_parquet_file`
call are unchanged.

**3c.** `file_chunk_loop` and `@dask.delayed` are kept but not called from the new path.

**3d. Update `fetch_and_format_nwm_points`:**
- Rename param `json_paths` → `file_paths`.
- Add `cache_dir: Optional[Path] = None`; pass it through to `process_chunk_of_files`.
- Detect path type: `.nc` extension → `parse_nwm_forecast_gcs_paths`; otherwise → `parse_nwm_json_paths`.
- `process_by_z_hour`, `stepsize`, groupby/split logic are unchanged.

### Phase 4 — `nwm_points.py` (depends on Phase 3)

Replace the `generate_json_paths()` call with `resolve_nwm_file_paths()`:

```python
# Replaces: json_paths = generate_json_paths(kerchunk_method, gcs_component_paths, json_dir, ...)
file_paths = resolve_nwm_file_paths(
    gcs_paths=gcs_component_paths,
    kerchunk_method=kerchunk_method,
    json_dir=Path(json_dir),
    ignore_missing_file=ignore_missing_file,
)

fetch_and_format_nwm_points(
    file_paths=file_paths,
    cache_dir=Path(json_dir),   # open_virtual_ref writes new .parq refs here for .nc fallback paths
    ...
)
```

`json_dir` signature is unchanged (`Union[str, Path]`, still required). No new parameters added to
`nwm_to_parquet()`. Update imports: add `resolve_nwm_file_paths`, remove `generate_json_paths`.

---

## Verification

1. `uv run pytest tests/` — existing tests pass without modification
2. **Smoke test `kerchunk_method="local"`** — confirm `json_dir` local cache lookup works; new `.parq`
   files created for missing refs; existing `.json` files reused via VirtualiZarr
3. **Smoke test `kerchunk_method="remote"`** — confirm S3 JSON lookup via `check_if_files_exist`
   works; files missing from S3 are skipped; output matches baseline
4. **Smoke test `kerchunk_method="auto"`** — confirm S3 → local `.parq`/`.json` → GCS fallback chain
5. **Re-run test** — confirm second run reuses cached `.parq` refs (no GCS header scans)
6. Profile wall-clock time for 100+ files vs. baseline

---

## Decisions

| Item | Decision |
|---|---|
| `json_dir` | Always required; single cache dir for `.json` and `.parq` reference files |
| `ref_dir` | Not added — merged into `json_dir` |
| `process_by_z_hour` + `stepsize` | Unchanged; potentially revisit in the future |
| `"remote"` missing-file behavior | Skipped — no GCS fallback (consistent with current) |
| `check_if_files_exist` | Untouched; used inside `resolve_nwm_file_paths` |
| `generate_json_paths` | No longer called from `nwm_points.py`; kept in `utils.py` untouched |
| `build_zarr_references`, `gen_json` | Kept in `utils.py` untouched |
| `build_remote_nwm_filelist` | Untouched |
| `_parse_hourly_metadata` / `build_hourly_timeseries` | Removed (experimental block) |
| `file_chunk_loop` + `@dask.delayed` | Kept, not called from new path |
| New utilities | Public, NWM-agnostic, fully typed and documented — importable externally |

---

## Further Considerations

1. **In-memory temp file**: If `combined_vds.vz.to_kerchunk()` accepts `io.BytesIO`, the one
   temp-file-per-chunk write in `concat_and_open_virtual_datasets` can be eliminated — easy follow-up.
2. **`xr.concat` options**: May need `compat='override'` and/or `data_vars='minimal'` for edge cases
   with mismatched coordinate attributes across NWM configurations.
3. **VirtualiZarr `reader_options` for S3 JSON**: Confirm the correct anonymous-access kwarg for
   `filetype="kerchunk"` with S3 paths — may differ from GCS `{"token": "anon"}`.
4. **Function Deprecation**: All functions that are kept but no longer called by the updated workflow will get a message added to the docstring denoting this so they may be removed in the future to clean up to the code base.
