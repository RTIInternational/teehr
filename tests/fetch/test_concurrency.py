"""Test how concurrency is configured and bounded."""
from pathlib import Path

import sys
import types

import pytest

import teehr.utils.concurrency as concurrency
from teehr.fetching.utils import build_zarr_references_virtualizarr
from teehr.utils.concurrency import (
    DEFAULT_IO_CONCURRENCY,
    DEFAULT_MAX_PROCESSES,
    MAX_IO_CONCURRENCY,
    MAX_CPU_WORKERS,
    available_cpus,
    get_concurrency,
    reset_concurrency,
    resolve,
    resolve_io_concurrency,
    resolve_cpu_processes,
    resolve_cpu_workers,
    set_concurrency,
    _cgroup_cpu_quota,
    _has_main_guard,
    use_process_pool,
)

ENV_VARS = [
    "TEEHR_IO_CONCURRENCY",
    "TEEHR_IO_MAX_WORKERS",
    "TEEHR_CPU_WORKERS",
    "TEEHR_CPU_MAX_WORKERS",
    "TEEHR_PROCESS_MIN_ITEMS",
    "TEEHR_MAX_PROCESSES",
]


@pytest.fixture(autouse=True)
def clean_concurrency_state(monkeypatch):
    """Keep per-process concurrency settings from leaking between tests."""
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    reset_concurrency()
    yield
    reset_concurrency()


def test_concurrency_defaults():
    """Defaults are used when nothing is set."""
    assert resolve_io_concurrency() == DEFAULT_IO_CONCURRENCY
    assert resolve_cpu_workers() == available_cpus()
    assert get_concurrency() == resolve()


def test_available_cpus_is_positive():
    """The CPU count used for defaults is always usable."""
    assert available_cpus() >= 1


def test_cgroup_v2_quota_is_read(tmp_path):
    """A cgroup v2 CPU limit is reported in whole cores, rounded up."""
    (tmp_path / "cpu.max").write_text("350000 100000")
    assert _cgroup_cpu_quota(tmp_path) == 4


def test_cgroup_v2_unlimited_quota(tmp_path):
    """An unlimited cgroup v2 quota reports no limit."""
    (tmp_path / "cpu.max").write_text("max 100000")
    assert _cgroup_cpu_quota(tmp_path) is None


def test_cgroup_v1_quota_is_read(tmp_path):
    """A cgroup v1 CPU limit is read when no v2 file is present."""
    (tmp_path / "cpu").mkdir()
    (tmp_path / "cpu" / "cpu.cfs_quota_us").write_text("200000")
    (tmp_path / "cpu" / "cpu.cfs_period_us").write_text("100000")
    assert _cgroup_cpu_quota(tmp_path) == 2


def test_missing_cgroup_files_report_no_limit(tmp_path):
    """No cgroup files (e.g. macOS) means no limit rather than an error."""
    assert _cgroup_cpu_quota(tmp_path) is None


def test_env_vars_are_read_at_call_time(monkeypatch):
    """Environment changes take effect without re-importing teehr."""
    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "12")
    monkeypatch.setenv("TEEHR_CPU_WORKERS", "3")
    assert resolve_io_concurrency() == 12
    assert resolve_cpu_workers() == 3

    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "20")
    assert resolve_io_concurrency() == 20


def test_legacy_env_var_names_still_work(monkeypatch):
    """The pre-rename environment variables are still honored."""
    monkeypatch.setenv("TEEHR_IO_MAX_WORKERS", "7")
    monkeypatch.setenv("TEEHR_CPU_MAX_WORKERS", "5")
    assert resolve_io_concurrency() == 7
    assert resolve_cpu_workers() == 5


def test_new_env_var_wins_over_legacy(monkeypatch):
    """The current name takes precedence over its legacy alias."""
    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "9")
    monkeypatch.setenv("TEEHR_IO_MAX_WORKERS", "7")
    assert resolve_io_concurrency() == 9


def test_unparseable_env_var_falls_back(monkeypatch):
    """A non-integer environment value is ignored rather than raising."""
    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "lots")
    assert resolve_io_concurrency() == DEFAULT_IO_CONCURRENCY


def test_set_concurrency_overrides_env(monkeypatch):
    """set_concurrency() takes precedence over the environment."""
    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "12")
    monkeypatch.setenv("TEEHR_CPU_WORKERS", "3")
    set_concurrency(io=4, cpu=2)
    assert resolve_io_concurrency() == 4
    assert resolve_cpu_workers() == 2


def test_explicit_argument_wins(monkeypatch):
    """An explicit argument takes precedence over everything else."""
    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "12")
    set_concurrency(io=4)
    assert resolve_io_concurrency(2) == 2


def test_set_concurrency_leaves_unset_values_alone():
    """Passing one setting doesn't disturb the other."""
    set_concurrency(io=4, cpu=2)
    set_concurrency(io=6)
    assert resolve_io_concurrency() == 6
    assert resolve_cpu_workers() == 2


def test_reset_concurrency_restores_defaults():
    """reset_concurrency() clears values set programmatically."""
    set_concurrency(io=4, cpu=2)
    reset_concurrency()
    assert resolve_io_concurrency() == DEFAULT_IO_CONCURRENCY


def test_values_are_clamped_to_supported_range():
    """Out-of-range values are clamped rather than used as given."""
    assert resolve_io_concurrency(0) == 1
    assert resolve_io_concurrency(10_000) == MAX_IO_CONCURRENCY
    assert resolve_cpu_workers(-5) == 1
    assert resolve_cpu_workers(10_000) == MAX_CPU_WORKERS


def test_process_count_is_capped_below_worker_count():
    """Processes get their own, lower ceiling because they cost memory."""
    set_concurrency(cpu=MAX_CPU_WORKERS)
    assert resolve_cpu_workers() == MAX_CPU_WORKERS
    assert resolve_cpu_processes() == DEFAULT_MAX_PROCESSES


def test_process_count_never_exceeds_worker_count():
    """Lowering the parse budget lowers the process count with it."""
    set_concurrency(cpu=2)
    assert resolve_cpu_processes() == 2


def test_process_cap_is_configurable(monkeypatch):
    """The process ceiling can be raised for machines with the memory."""
    monkeypatch.setenv("TEEHR_MAX_PROCESSES", "16")
    set_concurrency(cpu=32)
    assert resolve_cpu_processes() == 16


def test_guarded_script_is_detected(tmp_path):
    """A script with an __name__ guard is safe to re-import in a worker."""
    script = tmp_path / "guarded.py"
    script.write_text(
        "import teehr\n"
        "def main():\n"
        "    pass\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )
    assert _has_main_guard(script) is True


def test_unguarded_script_is_detected(tmp_path):
    """Top-level work with no guard would re-run inside every worker."""
    script = tmp_path / "unguarded.py"
    script.write_text("import teehr\nteehr.fetching\n")
    assert _has_main_guard(script) is False


def test_unreadable_script_is_treated_as_unguarded(tmp_path):
    """An unparseable or missing script errs toward the safe path."""
    assert _has_main_guard(tmp_path / "nope.py") is False
    broken = tmp_path / "broken.py"
    broken.write_text("def (:\n")
    assert _has_main_guard(broken) is False


def test_module_run_main_is_spawn_safe(monkeypatch):
    """`python -m pkg` re-imports by name, so there is nothing to re-run."""
    monkeypatch.setattr(concurrency, "_MAIN_IS_SPAWN_SAFE", None)
    main = types.SimpleNamespace(__spec__=object(), __file__="/tmp/whatever.py")
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert concurrency.main_module_is_spawn_safe() is True


def test_interactive_main_is_spawn_safe(monkeypatch):
    """A notebook or REPL has no script behind __main__."""
    monkeypatch.setattr(concurrency, "_MAIN_IS_SPAWN_SAFE", None)
    main = types.SimpleNamespace(__spec__=None)
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert concurrency.main_module_is_spawn_safe() is True


def test_unguarded_main_is_not_spawn_safe(monkeypatch, tmp_path):
    """An unguarded script must not start workers that would re-run it."""
    script = tmp_path / "unguarded.py"
    script.write_text("print('side effect')\n")
    monkeypatch.setattr(concurrency, "_MAIN_IS_SPAWN_SAFE", None)
    main = types.SimpleNamespace(__spec__=None, __file__=str(script))
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert concurrency.main_module_is_spawn_safe() is False


def test_process_pool_refused_for_unguarded_main(monkeypatch, tmp_path):
    """The gate declines processes rather than re-running the caller."""
    script = tmp_path / "unguarded.py"
    script.write_text("print('side effect')\n")
    monkeypatch.setattr(concurrency, "_MAIN_IS_SPAWN_SAFE", None)
    main = types.SimpleNamespace(__spec__=None, __file__=str(script))
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert use_process_pool(n_items=64, processes=8) is False


def test_process_pool_is_skipped_for_small_jobs():
    """Small batches stay in threads, where they are faster."""
    assert use_process_pool(n_items=8, processes=8) is False
    assert use_process_pool(n_items=1, processes=8) is False


def test_process_pool_is_used_for_large_jobs():
    """Large batches amortize the cost of starting workers."""
    assert use_process_pool(n_items=64, processes=8) is True


def test_process_pool_needs_more_than_one_worker():
    """A single worker gains nothing from a separate process."""
    assert use_process_pool(n_items=64, processes=1) is False


def test_process_pool_threshold_is_configurable(monkeypatch):
    """The threshold can be tuned, including forcing processes on."""
    monkeypatch.setenv("TEEHR_PROCESS_MIN_ITEMS", "0")
    assert use_process_pool(n_items=2, processes=8) is True

    monkeypatch.setenv("TEEHR_PROCESS_MIN_ITEMS", "1000000")
    assert use_process_pool(n_items=64, processes=8) is False


def test_building_references_in_processes(tmpdir, monkeypatch, caplog):
    """References build correctly when the process pool is used."""
    monkeypatch.setenv("TEEHR_PROCESS_MIN_ITEMS", "0")
    remote_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc",  # noqa
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f002.alaska.nc",  # noqa
    ]

    with caplog.at_level("INFO", logger="teehr"):
        built_files = build_zarr_references_virtualizarr(
            remote_paths=remote_paths,
            json_dir=tmpdir,
            ignore_missing_file=False,
            parse_workers=2,
        )

    assert len(built_files) == 2
    assert all(Path(f).is_file() for f in built_files)
    assert all(Path(f).stat().st_size > 0 for f in built_files)
    # The pool was really used, rather than quietly falling back to threads.
    assert "in 2 processes" in caplog.text
    assert "using threads" not in caplog.text


def test_already_built_references_are_reused(tmpdir):
    """Cached reference files short-circuit the build entirely."""
    remote_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc",  # noqa
    ]
    first = build_zarr_references_virtualizarr(
        remote_paths=remote_paths,
        json_dir=tmpdir,
        ignore_missing_file=False,
    )
    mtime = Path(first[0]).stat().st_mtime_ns

    second = build_zarr_references_virtualizarr(
        remote_paths=remote_paths,
        json_dir=tmpdir,
        ignore_missing_file=False,
    )

    assert second == first
    assert Path(second[0]).stat().st_mtime_ns == mtime
