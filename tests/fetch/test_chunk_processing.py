"""Test how chunks are written and spread across processes."""
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor


import pandas as pd
import pyarrow as pa
import pytest

import teehr.utils.concurrency as concurrency
from teehr.fetching.models.utils import TimeseriesTypeEnum
from teehr.fetching.utils import (
    _write_parquet_atomically,
    write_timeseries_parquet_file,
)
from teehr.utils.concurrency import in_worker_process, use_process_pool


@pytest.fixture(autouse=True)
def clear_spawn_safety_cache():
    """The cached spawn-safety answer must not leak between tests."""
    concurrency.main_module_is_spawn_safe.cache_clear()
    yield
    concurrency.main_module_is_spawn_safe.cache_clear()


def _timeseries_table():
    """A minimal valid secondary timeseries table."""
    return pa.table({
        "value": pa.array([1.0, 2.0], type=pa.float32()),
        "reference_time": pa.array(
            pd.to_datetime(["2024-01-01", "2024-01-01"]), type=pa.timestamp("ms")),
        "location_id": ["nwm30-1", "nwm30-2"],
        "value_time": pa.array(
            pd.to_datetime(["2024-01-01T01", "2024-01-01T02"]),
            type=pa.timestamp("ms")),
        "configuration_name": ["nwm30_short_range"] * 2,
        "variable_name": ["streamflow_hourly_inst"] * 2,
        "unit_name": ["m^3/s"] * 2,
        "member": [None, None],
    })


def test_written_file_is_readable(tmp_path):
    """The happy path still produces a readable parquet file."""
    out = tmp_path / "chunk.parquet"
    result = write_timeseries_parquet_file(
        out, True, _timeseries_table(), TimeseriesTypeEnum.secondary)
    assert result == out
    assert pd.read_parquet(out).shape[0] == 2


def test_no_temp_files_left_behind(tmp_path):
    """A successful write leaves only the parquet file."""
    out = tmp_path / "chunk.parquet"
    write_timeseries_parquet_file(
        out, True, _timeseries_table(), TimeseriesTypeEnum.secondary)
    assert [p.name for p in tmp_path.iterdir()] == ["chunk.parquet"]


def test_failed_write_leaves_no_partial_file(tmp_path, monkeypatch):
    """An interrupted write must not leave a truncated file at the real path.

    Otherwise a later run skips it as already written and then can't read it.
    """
    out = tmp_path / "chunk.parquet"

    def boom(*args, **kwargs):
        # Stand in for a cancelled chunk, an OOM kill, or a Ctrl-C.
        raise KeyboardInterrupt("interrupted mid-write")

    monkeypatch.setattr("teehr.fetching.utils.writer.to_cache", boom)
    with pytest.raises(KeyboardInterrupt):
        _write_parquet_atomically(
            pd.DataFrame({"a": [1]}), out, pa.schema([("a", pa.int64())]))

    assert not out.exists()
    assert list(tmp_path.iterdir()) == []


def test_existing_file_is_not_overwritten_when_asked(tmp_path):
    """overwrite_output=False leaves the existing file alone."""
    out = tmp_path / "chunk.parquet"
    write_timeseries_parquet_file(
        out, True, _timeseries_table(), TimeseriesTypeEnum.secondary)
    before = out.stat().st_mtime_ns

    result = write_timeseries_parquet_file(
        out, False, _timeseries_table(), TimeseriesTypeEnum.secondary)

    assert result == out
    assert out.stat().st_mtime_ns == before


def test_not_in_worker_process_in_main():
    """The main process is not a worker."""
    assert in_worker_process() is False


def _detect(_):
    return in_worker_process()


def test_in_worker_process_inside_pool():
    """A spawned worker recognizes that it is one."""
    with ProcessPoolExecutor(1, mp_context=mp.get_context("spawn")) as pool:
        assert list(pool.map(_detect, [0])) == [True]


def _would_nest(_):
    # A worker must not start a pool of its own, however much work it has.
    return use_process_pool(n_items=1000, processes=8, min_items=1)


def test_process_pool_declined_inside_worker():
    """Nested pools are refused, leaving parallelism to whatever started us."""
    assert use_process_pool(n_items=1000, processes=8, min_items=1) is True
    with ProcessPoolExecutor(1, mp_context=mp.get_context("spawn")) as pool:
        assert list(pool.map(_would_nest, [0])) == [False]


def test_min_items_gates_the_process_pool(monkeypatch):
    """Chunky work can opt into processes sooner than leaner work."""
    monkeypatch.setattr(concurrency, "main_module_is_spawn_safe", lambda: True)
    assert use_process_pool(n_items=4, processes=8, min_items=32) is False
    assert use_process_pool(n_items=4, processes=8, min_items=3) is True


def test_concurrency_args_reach_the_chunk_loop(monkeypatch):
    """io_concurrency and cpu_workers must survive the trip from
    nwm_to_parquet down to the chunk loop, and to the planning step."""
    import teehr.fetching.nwm.nwm_points as nwm_points

    seen = {}
    planned = {}

    def fake_fetch(**kwargs):
        seen.update(kwargs)
        return []

    def fake_json_paths(*args):
        # Positional, matching generate_json_paths' signature tail.
        planned["io"], planned["cpu"] = args[4], args[5]
        return [f"{gcs}.json"]

    monkeypatch.setattr(nwm_points, "fetch_and_format_nwm_points", fake_fetch)
    gcs = ("gcs://national-water-model/nwm.20240101/short_range/"
           "nwm.t00z.short_range.channel_rt.f001.conus.nc")
    monkeypatch.setattr(nwm_points, "generate_json_paths", fake_json_paths)
    monkeypatch.setattr(
        nwm_points, "build_remote_nwm_filelist", lambda *a, **k: [gcs])

    nwm_points.nwm_to_parquet(
        configuration="short_range",
        output_type="channel_rt",
        variable_name="streamflow",
        location_ids=[101],
        json_dir="/tmp/json",
        output_parquet_dir="/tmp/out",
        nwm_version="nwm30",
        start_date="2024-01-01",
        ingest_days=1,
        io_concurrency=7,
        cpu_workers=3,
    )

    assert seen["io_concurrency"] == 7
    assert seen["cpu_workers"] == 3
    # Reference building and the s3 check happen before any chunk exists, so
    # they take the budget too.
    assert (planned["io"], planned["cpu"]) == (7, 3)


def test_zarr_version_lookup_is_warmed_before_the_readers_start(monkeypatch):
    """xarray resolves zarr's version inside every open_zarr, and that walk of
    sys.path is slow while cold. It has to be done before the pool exists: a
    pool's worth of workers all missing the cold cache at once is what turned
    it into a multi-minute stall."""
    import teehr.fetching.utils as fetch_utils

    order = []

    fetch_utils._warm_zarr_version_lookup.cache_clear()
    monkeypatch.setattr(
        fetch_utils, "_warm_zarr_version_lookup", lambda: order.append("warm")
    )

    def fake_thread_pool(*args, **kwargs):
        order.append("pool")
        raise RuntimeError("stop here")

    monkeypatch.setattr(fetch_utils, "thread_pool", fake_thread_pool)

    with pytest.raises(RuntimeError, match="stop here"):
        concurrency.run_sync(
            fetch_utils._combine_and_open_kerchunk_refs_async(
                json_paths=["s3://bucket/a.json"],
                variable_name="streamflow",
                location_ids=[101],
                registry=object(),
            )
        )

    assert order == ["warm", "pool"], (
        "the version lookup must be resolved before the readers start"
    )


def test_zarr_version_lookup_runs_once():
    """It is only worth doing up front if repeat calls are free."""
    import teehr.fetching.utils as fetch_utils

    fetch_utils._warm_zarr_version_lookup.cache_clear()
    fetch_utils._warm_zarr_version_lookup()
    fetch_utils._warm_zarr_version_lookup()
    info = fetch_utils._warm_zarr_version_lookup.cache_info()
    assert (info.misses, info.hits) == (1, 1)


def test_remote_stores_retry_for_longer_than_two_seconds():
    """obstore's default backoff spends all 10 retries in ~2s, which is not
    enough to ride out an object store resetting a connection under load. Every
    remote store must carry the longer backoff; a new from_url that forgets it
    would fail a task on a blip that clears seconds later."""
    from datetime import timedelta

    from teehr.fetching.utils import (
        REMOTE_RETRY_CONFIG,
        _build_gcs_source_registry,
        _public_store,
        build_kerchunk_registry,
    )

    assert REMOTE_RETRY_CONFIG["backoff"]["init_backoff"] >= timedelta(seconds=1)

    registry = build_kerchunk_registry(["s3://ciroh-nwm-zarr-copy/some.json"])
    remote_urls = [
        "gs://national-water-model/a",
        "https://storage.googleapis.com/a",
        "s3://ciroh-nwm-zarr-copy/a",
    ]
    stores = [registry.resolve(url)[0] for url in remote_urls]
    stores.append(_build_gcs_source_registry().resolve("gcs://national-water-model/a")[0])
    stores.append(_public_store("s3://ciroh-nwm-zarr-copy/"))
    stores.append(_public_store("gs://national-water-model/"))

    for store in stores:
        assert store.retry_config is not None, f"{store} has obstore's 2s default"
        assert store.retry_config["backoff"]["init_backoff"] >= timedelta(seconds=1)


def test_public_buckets_are_pinned_to_their_own_region():
    """Every public bucket teehr reads has to be addressed in its own region.
    A wrong or missing pin surfaces as "Received redirect without LOCATION",
    which says nothing about regions -- so the map is worth asserting. The
    v2.0 retrospective bucket is the one that is not us-east-1."""
    from teehr.fetching.const import S3_BUCKET_REGIONS
    from teehr.fetching.utils import _s3_region

    assert S3_BUCKET_REGIONS["noaa-nwm-retro-v2-zarr-pds"] == "us-west-2"
    assert _s3_region("s3://noaa-nwm-retro-v2-zarr-pds") == "us-west-2"
    assert _s3_region(
        "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr"
    ) == "us-east-1"
    assert _s3_region("s3://ciroh-nwm-zarr-copy/some/key.json") == "us-east-1"
    # An unknown bucket still gets a region rather than the ambient one.
    assert _s3_region("s3://some-other-bucket/x") == "us-east-1"


def test_public_zarr_store_carries_region_and_retries():
    """The retrospective reads go through the same store as everything else."""
    from datetime import timedelta

    from teehr.fetching.utils import _public_store

    store = _public_store("s3://noaa-nwm-retro-v2-zarr-pds/")
    assert store.retry_config["backoff"]["init_backoff"] >= timedelta(seconds=1)
