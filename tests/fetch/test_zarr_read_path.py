"""Pin the behaviour the zarr-direct NWM read path relies on.

These build their own NetCDF files and kerchunk references, so they run
offline and are fast enough to say something about correctness that the
live-fetch tests -- a few timesteps against GCS -- cannot.
"""
import asyncio
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import ujson
import xarray as xr
import zarr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import from_url
from virtualizarr.parsers import HDFParser

import teehr.fetching.utils as fetch_utils
import teehr.fetching.nwm.point_utils as point_utils
from teehr.fetching.nwm.point_utils import chunk_output_filename
from teehr.fetching.const import NWM_BUCKET, NWM_S3_JSON_PATH
from teehr.fetching.utils import (
    INLINE_COORDINATES,
    LIST_INSTEAD_OF_HEAD_MIN_KEYS,
    _plan_existence_checks,
    check_if_files_exist,
    _fix_fill_values,
    _fix_scalar_chunk_keys,
    _FeatureIdPositions,
    _manifest_store_from_refs,
    build_kerchunk_registry,
    combine_and_open_kerchunk_refs,
    open_kerchunk_grid_window,
)

N_FEATURES = 400
FILL = -999900
SCALE = 0.01


def _write_point_netcdf(path: Path, feature_ids: np.ndarray, seed: int = 0):
    """Write an NWM-shaped point file: scaled ints, a fill value, one time."""
    rng = np.random.default_rng(seed)
    raw = rng.integers(0, 500000, feature_ids.size).astype("int32")
    raw[::37] = FILL  # some missing values to exercise masking
    ds = xr.Dataset(
        {"streamflow": (("feature_id",), raw)},
        coords={
            "feature_id": ("feature_id", feature_ids.astype("int64")),
            "time": ("time", np.array([0], dtype="int32")),
        },
    )
    ds["streamflow"].attrs = {
        "units": "m3 s-1",
        "scale_factor": SCALE,
        "add_offset": 0.0,
        "_FillValue": FILL,
        "missing_value": FILL,
    }
    ds["time"].attrs = {"units": "minutes since 1970-01-01 00:00:00 UTC"}
    # Compressed and chunked like real NWM output.
    codec = {"zlib": True, "complevel": 2, "shuffle": True}
    ds.to_netcdf(path, engine="h5netcdf", encoding={
        "streamflow": {
            "dtype": "int32", "chunksizes": (N_FEATURES // 4,), **codec
        },
        "feature_id": {
            "dtype": "int64", "chunksizes": (N_FEATURES // 4,), **codec
        },
    })


def _write_grid_netcdf(path: Path, n_y: int = 40, n_x: int = 40):
    """Write a gridded file chunked into tiles, like NWM forcing."""
    rng = np.random.default_rng(1)
    raw = rng.integers(20000, 32000, (1, n_y, n_x)).astype("int32")
    ds = xr.Dataset(
        {"T2D": (("time", "y", "x"), raw)},
        coords={
            "time": ("time", np.array([0], dtype="int32")),
            "y": ("y", np.arange(n_y, dtype="float64")),
            "x": ("x", np.arange(n_x, dtype="float64")),
        },
    )
    ds["T2D"].attrs = {
        "units": "K", "scale_factor": SCALE, "add_offset": 0.0,
        "_FillValue": FILL, "missing_value": FILL,
    }
    ds["time"].attrs = {"units": "minutes since 1970-01-01 00:00:00 UTC"}
    ds.to_netcdf(path, engine="h5netcdf", encoding={
        "T2D": {"dtype": "int32", "chunksizes": (1, n_y // 4, n_x // 4)},
    })


def _build_reference(nc_path: Path, json_path: Path) -> str:
    """Write a kerchunk reference for ``nc_path``, as teehr's builder does."""
    registry = ObjectStoreRegistry(
        {nc_path.parent.as_uri() + "/": from_url(nc_path.parent.as_uri() + "/")}
    )
    store = HDFParser()(url=nc_path.as_uri(), registry=registry)
    vds = store.to_virtual_dataset(loadable_variables=INLINE_COORDINATES)
    refs = _fix_fill_values(
        _fix_scalar_chunk_keys(vds.virtualize.to_kerchunk(format="dict"))
    )
    json_path.write_text(ujson.dumps(refs))
    return str(json_path)


@pytest.fixture
def point_reference(tmp_path):
    """One reference file over ascending NWM-like feature ids."""
    feature_ids = np.arange(101, 101 + N_FEATURES * 3, 3, dtype="int64")
    nc = tmp_path / "point.nc"
    _write_point_netcdf(nc, feature_ids)
    return _build_reference(nc, tmp_path / "point.json"), feature_ids


def test_orthogonal_selection_matches_numpy_fancy_indexing(point_reference):
    """Unsorted ids and duplicates must come back in the order asked for.

    zarr's docs describe orthogonal indexing more strictly than it behaves,
    so a version bump could silently start sorting or rejecting these.
    """
    json_path, _ = point_reference
    registry = build_kerchunk_registry([json_path])
    content = Path(json_path).read_bytes()
    group = zarr.open_group(_manifest_store_from_refs(content, registry), mode="r")
    full = group["streamflow"][:]

    rng = np.random.default_rng(3)
    for size in (1, 5, 200):
        positions = rng.integers(0, N_FEATURES, size)
        selected = group["streamflow"].get_orthogonal_selection((positions,))
        np.testing.assert_array_equal(selected, full[positions])


def test_decode_after_select_matches_decode_then_select(point_reference):
    """Moving CF decoding after the selection must not change any value.

    Covers scale_factor and fill-value masking, which is the whole reason
    decoding stays with xarray rather than being reimplemented.
    """
    json_path, feature_ids = point_reference
    registry = build_kerchunk_registry([json_path])
    group = zarr.open_group(
        _manifest_store_from_refs(Path(json_path).read_bytes(), registry), mode="r"
    )
    decoded_whole = xr.conventions.decode_cf_variable(
        "streamflow",
        xr.Variable(
            ("feature_id",),
            group["streamflow"][:],
            attrs=dict(group["streamflow"].attrs),
        ),
    ).values

    rng = np.random.default_rng(4)
    positions = rng.integers(0, N_FEATURES, 50)
    ds, _ = combine_and_open_kerchunk_refs(
        [json_path], "streamflow", feature_ids[positions], registry=registry
    )

    np.testing.assert_array_equal(
        ds["streamflow"].values[0], decoded_whole[positions]
    )
    assert np.isnan(decoded_whole).any(), "fixture should exercise masking"
    np.testing.assert_array_equal(ds["feature_id"].values, feature_ids[positions])


def test_missing_location_id_raises(point_reference):
    """A requested id that isn't in the file is a bad request, not a skip."""
    json_path, feature_ids = point_reference
    with pytest.raises(ValueError, match="not found in"):
        combine_and_open_kerchunk_refs(
            [json_path], "streamflow", np.array([feature_ids[0], -12345])
        )


def test_same_shape_different_ids_is_caught(tmp_path):
    """Same shape must not be enough to reuse positions across files.

    ``ids + 1`` keeps the shape and the dtype, so the cache key matches. The
    per-file sampled re-read is what has to catch it.
    """
    ids_a = np.arange(101, 101 + N_FEATURES * 3, 3, dtype="int64")
    ids_b = ids_a + 1
    refs = []
    for name, ids in (("a", ids_a), ("b", ids_b)):
        nc = tmp_path / f"{name}.nc"
        _write_point_netcdf(nc, ids)
        refs.append(_build_reference(nc, tmp_path / f"{name}.json"))

    # Reading both in one call must fail rather than label file b's values
    # with file a's ids.
    with pytest.raises(ValueError):
        combine_and_open_kerchunk_refs(refs, "streamflow", ids_a[:10])


def test_positions_are_resolved_once_per_shape(point_reference):
    """feature_id must be read once for a run, not once per file."""
    json_path, feature_ids = point_reference
    registry = build_kerchunk_registry([json_path])
    group = zarr.open_group(
        _manifest_store_from_refs(Path(json_path).read_bytes(), registry), mode="r"
    )
    cache = _FeatureIdPositions(feature_ids[:10])

    first = cache.resolve(group)
    assert len(cache._positions) == 1
    np.testing.assert_array_equal(cache.resolve(group), first)
    assert len(cache._positions) == 1, "same shape must not add a second entry"

    # A differently sized coordinate must key apart rather than reuse.
    other = np.arange(101, 101 + (N_FEATURES - 1) * 3, 3, dtype="int64")
    nc = Path(json_path).parent / "other.nc"
    _write_point_netcdf(nc, other)
    other_group = zarr.open_group(
        _manifest_store_from_refs(
            Path(_build_reference(nc, nc.with_suffix(".json"))).read_bytes(),
            build_kerchunk_registry([str(nc.with_suffix(".json"))]),
        ),
        mode="r",
    )
    cache.resolve(other_group)
    assert len(cache._positions) == 2


def test_sampled_check_catches_a_swapped_feature_id(point_reference):
    """A file of the same shape holding other ids must be caught."""
    json_path, feature_ids = point_reference
    registry = build_kerchunk_registry([json_path])
    cache = _FeatureIdPositions(feature_ids[:10])
    group = zarr.open_group(
        _manifest_store_from_refs(Path(json_path).read_bytes(), registry), mode="r"
    )
    assert group["feature_id"].nchunks > 1, "fixture must be chunked to sample"
    cache.resolve(group)

    # Same shape, different expectation: the sampled re-read must notice.
    cache.location_ids = cache.location_ids + 1
    with pytest.raises(ValueError, match="does not match"):
        cache.resolve(group)


def test_variable_and_feature_id_length_mismatch_raises(point_reference):
    """Reject a file whose variable and feature_id lengths disagree."""
    json_path, feature_ids = point_reference
    refs = ujson.loads(Path(json_path).read_bytes())
    meta = ujson.loads(refs["refs"]["streamflow/.zarray"])
    meta["shape"] = [N_FEATURES - 1]
    refs["refs"]["streamflow/.zarray"] = ujson.dumps(meta)

    registry = build_kerchunk_registry([json_path])
    cache = _FeatureIdPositions(feature_ids[:5])
    with pytest.raises(ValueError, match="disagrees with itself"):
        fetch_utils._read_point_values(
            ujson.dumps(refs).encode(), registry, "streamflow", cache
        )


def test_grid_window_reads_only_the_window(tmp_path):
    """The window must match a full read sliced the same way."""
    nc = tmp_path / "grid.nc"
    _write_grid_netcdf(nc)
    json_path = _build_reference(nc, tmp_path / "grid.json")
    registry = build_kerchunk_registry([json_path])

    group = zarr.open_group(
        _manifest_store_from_refs(Path(json_path).read_bytes(), registry), mode="r"
    )
    decoded_whole = xr.conventions.decode_cf_variable(
        "T2D",
        xr.Variable(("time", "y", "x"), group["T2D"][:], attrs=dict(group["T2D"].attrs)),
    ).values

    window, times, units = open_kerchunk_grid_window(
        json_path, "T2D", 12, 19, 5, 11,
        ignore_missing_file=False, registry=registry,
    )
    assert window.shape == (1, 8, 7)
    assert units == "K"
    assert times.size == 1
    np.testing.assert_array_equal(window, decoded_whole[:, 12:20, 5:12])


def test_existence_checks_list_big_prefixes_and_head_small_ones():
    """One listing per crowded prefix; small ones stay file by file."""
    big = [f"s3://b/day1/f{i:04d}.json"
           for i in range(LIST_INSTEAD_OF_HEAD_MIN_KEYS)]
    small = [f"s3://b/day2/f{i:04d}.json" for i in range(3)]

    by_prefix, listable, heads = _plan_existence_checks(big + small)

    assert listable == ["s3://b/day1/"]
    assert heads == small
    assert set(by_prefix) == {"s3://b/day1/", "s3://b/day2/"}
    assert by_prefix["s3://b/day1/"] == big


def test_existence_checks_just_below_the_threshold_use_heads():
    """The threshold is inclusive, so one fewer file must not list."""
    paths = [f"s3://b/day1/f{i:04d}.json"
             for i in range(LIST_INSTEAD_OF_HEAD_MIN_KEYS - 1)]
    _, listable, heads = _plan_existence_checks(paths)
    assert listable == []
    assert heads == paths


def test_check_if_files_exist_via_listing():
    """The listing path must agree with reality, including absent files.

    Exercises the >= threshold branch end to end; the single-file case in
    ``test_generate_json_paths`` only ever reaches the head branch.
    """
    prefix = (
        f"{NWM_S3_JSON_PATH}/{NWM_BUCKET}/nwm.20240222/short_range/"
        "nwm.t{h:02d}z.short_range.channel_rt.f{f:03d}.conus.nc.json"
    )
    real = [prefix.format(h=h, f=f) for h in range(12) for f in range(1, 19)]
    assert len(real) >= LIST_INSTEAD_OF_HEAD_MIN_KEYS
    absent = [p.replace(".conus.", ".not-a-domain.") for p in real[:5]]

    result = check_if_files_exist(real + absent)

    assert list(result) == real + absent, "order must follow the request"
    assert all(result[p] for p in real)
    assert not any(result[p] for p in absent)


def _point_chunk(day="20240222", z_hour="t00z", n=3):
    """Build a chunk dataframe shaped like build_file_chunks output."""
    return pd.DataFrame({
        "day": [day] * n,
        "z_hour": [z_hour] * n,
        "filepath": [
            f"/refs/nwm.{day}.nwm.{z_hour}.short_range.channel_rt.f{i:03d}"
            ".conus.nc.json" for i in range(1, n + 1)
        ],
    })


def test_chunk_output_filename_by_z_hour():
    """Grouped by z-hour, a chunk is named for its reference time."""
    name = chunk_output_filename(_point_chunk(), "short_range", True)
    assert name == "20240222T00.parquet"


def test_chunk_output_filename_spans_forecast_hours():
    """Chunked by stepsize, the name carries the first and last hour."""
    name = chunk_output_filename(_point_chunk(n=3), "short_range", False)
    assert name == "20240222T00F001_20240222T00F003.parquet"


def test_existing_chunk_is_not_refetched(tmp_path, monkeypatch):
    """A written chunk must be skipped before any data is read."""
    chunk = _point_chunk()
    written = tmp_path / chunk_output_filename(chunk, "short_range", True)
    written.write_bytes(b"pretend parquet")

    calls = []
    monkeypatch.setattr(
        point_utils, "process_chunk_of_files",
        lambda *a, **k: calls.append(a) or None,
    )
    monkeypatch.setattr(
        point_utils, "build_file_chunks", lambda *a, **k: [chunk]
    )
    monkeypatch.setattr(
        point_utils, "build_kerchunk_registry", lambda *a, **k: None
    )

    result = point_utils.fetch_and_format_nwm_points(
        file_paths=list(chunk.filepath),
        location_ids=[101],
        configuration="short_range",
        variable_name="streamflow",
        output_parquet_dir=str(tmp_path),
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=True,
        overwrite_output=False,
        nwm_version="nwm30",
        variable_mapper=None,
        timeseries_type="secondary",
        drop_overlapping_assimilation_values=True,
    )

    assert calls == [], "the chunk must not be fetched at all"
    assert result == [written], "the existing file still counts as written"


def test_existing_chunk_is_refetched_when_overwriting(tmp_path, monkeypatch):
    """overwrite_output=True must still do the work."""
    chunk = _point_chunk()
    (tmp_path / chunk_output_filename(chunk, "short_range", True)).write_bytes(b"x")

    calls = []
    monkeypatch.setattr(
        point_utils, "process_chunk_of_files",
        lambda *a, **k: calls.append(a) or None,
    )
    monkeypatch.setattr(point_utils, "build_file_chunks", lambda *a, **k: [chunk])
    monkeypatch.setattr(point_utils, "build_kerchunk_registry", lambda *a, **k: None)

    point_utils.fetch_and_format_nwm_points(
        file_paths=list(chunk.filepath),
        location_ids=[101],
        configuration="short_range",
        variable_name="streamflow",
        output_parquet_dir=str(tmp_path),
        process_by_z_hour=True,
        stepsize=100,
        ignore_missing_file=True,
        overwrite_output=True,
        nwm_version="nwm30",
        variable_mapper=None,
        timeseries_type="secondary",
        drop_overlapping_assimilation_values=True,
    )
    assert len(calls) == 1, "overwrite_output=True must fetch the chunk"


def test_existence_checks_respect_one_concurrency_cap(monkeypatch):
    """Listings and heads together must stay within io_concurrency.

    They used to run as two gather_bounded calls under one asyncio.gather,
    and a semaphore each meant twice the intended load on the store.
    """
    limit = 4
    state = {"now": 0, "max": 0}

    async def _tracked(fn):
        state["now"] += 1
        state["max"] = max(state["max"], state["now"])
        try:
            await asyncio.sleep(0.01)
            return fn()
        finally:
            state["now"] -= 1

    class _Store:
        async def head_async(self, key):
            return await _tracked(lambda: None)

    monkeypatch.setattr(fetch_utils, "_public_store", lambda url: _Store())
    monkeypatch.setattr(
        fetch_utils, "_list_prefix",
        lambda store, prefix: _tracked(lambda: [f"big/f{i:04d}" for i in range(200)]),
    )

    big = [f"s3://b/big/f{i:04d}" for i in range(200)]
    small = [f"s3://b/small/f{i:04d}" for i in range(20)]
    _, listable, heads = _plan_existence_checks(big + small)
    assert listable and heads, "both branches must be exercised"

    result = check_if_files_exist(big + small, io_concurrency=limit)

    assert state["max"] <= limit, (
        f"{state['max']} network calls in flight with io_concurrency={limit}"
    )
    assert all(result[p] for p in big), "listed keys must resolve to True"
    assert list(result) == big + small
