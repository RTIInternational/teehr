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


def test_min_items_argument_overrides_default(monkeypatch):
    """Chunky work can opt into processes sooner than the default threshold."""
    monkeypatch.setattr(concurrency, "main_module_is_spawn_safe", lambda: True)
    assert use_process_pool(n_items=4, processes=8) is False
    assert use_process_pool(n_items=4, processes=8, min_items=3) is True


def test_chunk_workers_reaches_the_chunk_loop(monkeypatch):
    """chunk_workers must survive the trip from nwm_to_parquet downwards."""
    import teehr.fetching.nwm.nwm_points as nwm_points

    seen = {}

    def fake_fetch(**kwargs):
        seen.update(kwargs)
        return []

    monkeypatch.setattr(nwm_points, "fetch_and_format_nwm_points", fake_fetch)
    gcs = ("gcs://national-water-model/nwm.20240101/short_range/"
           "nwm.t00z.short_range.channel_rt.f001.conus.nc")
    monkeypatch.setattr(
        nwm_points, "generate_json_paths", lambda *a, **k: [f"{gcs}.json"])
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
        chunk_workers=4,
    )

    assert seen["chunk_workers"] == 4


def test_chunk_workers_defaults_to_sequential():
    """Left alone, chunks are processed one at a time."""
    import inspect
    from teehr.fetching.nwm.point_utils import fetch_and_format_nwm_points

    default = inspect.signature(
        fetch_and_format_nwm_points).parameters["chunk_workers"].default
    assert default is None  # resolved to 1 inside
    assert use_process_pool(n_items=1000, processes=1) is False
