"""Test how concurrency is configured and bounded.

Inside a container, ``os.cpu_count()`` and ``/proc/meminfo`` report the *host*
machine, not the slice the container is allowed to use -- so teehr would happily
start 64 threads on a 4-CPU pod. The real limits live in the kernel's cgroup
files, which is what ``_cgroup_cpu_quota`` and ``_cgroup_memory_limit`` read.
There are two on-disk layouts (cgroup v1 and v2) and each has its own way of
saying "no limit", hence the cases below.
"""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import multiprocessing
import sys
import types

import pytest

import teehr.utils.concurrency as concurrency
from teehr.fetching.utils import build_zarr_references_virtualizarr
from teehr.utils.concurrency import (
    DEFAULT_IO_CONCURRENCY,
    MAX_IO_CONCURRENCY,
    MAX_CPU_WORKERS,
    available_cpus,
    available_memory,
    reset_concurrency,
    resolve_budget,
    resolve_io_concurrency,
    resolve_cpu_processes,
    resolve_cpu_workers,
    set_concurrency,
    _cgroup_cpu_quota,
    _cgroup_memory_limit,
    _has_main_guard,
    use_process_pool,
)

ENV_VARS = [
    "TEEHR_IO_CONCURRENCY",
    "TEEHR_CPU_WORKERS",
]


@pytest.fixture(autouse=True)
def clean_concurrency_state(monkeypatch):
    """Keep per-process concurrency settings from leaking between tests.

    The spawn-safety answer is cached, and a cached value survives monkeypatch
    undoing whatever made it, so clear it too.
    """
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    reset_concurrency()
    concurrency.main_module_is_spawn_safe.cache_clear()
    yield
    reset_concurrency()
    concurrency.main_module_is_spawn_safe.cache_clear()


def test_concurrency_defaults():
    """Defaults are used when nothing is set."""
    assert available_cpus() >= 1
    assert resolve_io_concurrency() == DEFAULT_IO_CONCURRENCY
    assert resolve_cpu_workers() == available_cpus()
    assert resolve_budget().cpu == available_cpus()


@pytest.mark.parametrize("files, expected_cores", [
    # v2: "quota period" in microseconds -> 3.5 cores, rounded up.
    ({"cpu.max": "350000 100000"}, 4),
    # v2 says unlimited with the word "max".
    ({"cpu.max": "max 100000"}, None),
    # v1 splits quota and period across two files.
    ({"cpu/cpu.cfs_quota_us": "200000",
      "cpu/cpu.cfs_period_us": "100000"}, 2),
    # No cgroup files at all (macOS, bare metal) is not an error.
    ({}, None),
])
def test_cpu_limit_is_read_from_cgroup(tmp_path, files, expected_cores):
    """The container's CPU allowance, or None when it is unlimited."""
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    assert _cgroup_cpu_quota(tmp_path) == expected_cores


def test_env_vars_are_read_at_call_time(monkeypatch):
    """Environment changes take effect without re-importing teehr."""
    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "12")
    monkeypatch.setenv("TEEHR_CPU_WORKERS", "3")
    assert resolve_io_concurrency() == 12
    assert resolve_cpu_workers() == 3

    monkeypatch.setenv("TEEHR_IO_CONCURRENCY", "20")
    assert resolve_io_concurrency() == 20


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


def test_process_count_is_bounded_by_cpu_not_a_fixed_ceiling(monkeypatch):
    """With memory to spare, processes follow the cpu budget."""
    monkeypatch.setattr(concurrency, "available_memory", lambda: 512 * 1024**3)
    set_concurrency(cpu=MAX_CPU_WORKERS)
    assert resolve_cpu_workers() == MAX_CPU_WORKERS
    assert resolve_cpu_processes(memory_per_process=1400 * 1024**2) == (
        MAX_CPU_WORKERS
    )


def test_memory_limits_the_process_count(monkeypatch):
    """A machine short on memory runs fewer workers, not the full CPU count."""
    monkeypatch.setattr(concurrency, "available_memory", lambda: 4 * 1024**3)
    set_concurrency(cpu=32)
    # 4GB at the 1.4GB the caller budgets per worker -> 2
    assert resolve_cpu_processes(memory_per_process=1400 * 1024**2) == 2


def test_no_memory_budget_means_no_memory_cap(monkeypatch):
    """A caller that does not say what a worker costs is bounded by cpu."""
    monkeypatch.setattr(concurrency, "available_memory", lambda: 1 * 1024**3)
    set_concurrency(cpu=8)
    assert resolve_cpu_processes() == 8


def test_bigger_machine_gets_more_processes(monkeypatch):
    """The large notebook profile should use more workers than the small one."""
    budget = 1400 * 1024**2
    monkeypatch.setattr(concurrency, "available_memory", lambda: 127 * 1024**3)
    set_concurrency(cpu=16)
    large = resolve_cpu_processes(memory_per_process=budget)
    monkeypatch.setattr(concurrency, "available_memory", lambda: 32 * 1024**3)
    set_concurrency(cpu=4)
    small = resolve_cpu_processes(memory_per_process=budget)
    assert large == 16 and small == 4


@pytest.mark.parametrize("files, expected_bytes", [
    # v2 states the limit in bytes.
    ({"memory.max": "34359738368\n"}, 34359738368),
    # v2 says unlimited with the word "max".
    ({"memory.max": "max\n"}, None),
    # v1 uses a different filename.
    ({"memory/memory.limit_in_bytes": "8589934592"}, 8589934592),
    # v1 says unlimited with a huge sentinel number instead of a word.
    ({"memory/memory.limit_in_bytes": str(2**63 - 1)}, None),
    # No cgroup files at all.
    ({}, None),
])
def test_memory_limit_is_read_from_cgroup(tmp_path, files, expected_bytes):
    """The container's memory allowance, or None when it is unlimited."""
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    assert _cgroup_memory_limit(tmp_path) == expected_bytes


def test_available_memory_is_positive_or_unknown():
    """Whatever this machine reports must be usable or absent."""
    memory = available_memory()
    assert memory is None or memory > 0


def test_process_count_follows_the_cpu_budget():
    """Fewer CPU workers means fewer processes; a process needs a worker."""
    set_concurrency(cpu=2)
    assert resolve_cpu_processes() == 2


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
    concurrency.main_module_is_spawn_safe.cache_clear()
    main = types.SimpleNamespace(__spec__=object(), __file__="/tmp/whatever.py")
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert concurrency.main_module_is_spawn_safe() is True


def test_interactive_main_is_spawn_safe(monkeypatch):
    """A notebook or REPL has no script behind __main__."""
    concurrency.main_module_is_spawn_safe.cache_clear()
    main = types.SimpleNamespace(__spec__=None)
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert concurrency.main_module_is_spawn_safe() is True


def test_unguarded_main_is_not_spawn_safe(monkeypatch, tmp_path):
    """An unguarded script must not start workers that would re-run it."""
    script = tmp_path / "unguarded.py"
    script.write_text("print('side effect')\n")
    concurrency.main_module_is_spawn_safe.cache_clear()
    main = types.SimpleNamespace(__spec__=None, __file__=str(script))
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert concurrency.main_module_is_spawn_safe() is False


def test_process_pool_refused_for_unguarded_main(monkeypatch, tmp_path):
    """The gate declines processes rather than re-running the caller."""
    script = tmp_path / "unguarded.py"
    script.write_text("print('side effect')\n")
    concurrency.main_module_is_spawn_safe.cache_clear()
    main = types.SimpleNamespace(__spec__=None, __file__=str(script))
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert use_process_pool(n_items=64, processes=8, min_items=32) is False


def test_process_pool_is_skipped_for_small_jobs():
    """Small batches stay in threads, where they are faster."""
    assert use_process_pool(n_items=8, processes=8, min_items=32) is False
    assert use_process_pool(n_items=1, processes=8, min_items=32) is False


def test_process_pool_is_used_for_large_jobs():
    """Large batches amortize the cost of starting workers."""
    assert use_process_pool(n_items=64, processes=8, min_items=32) is True


def test_process_pool_needs_more_than_one_worker():
    """A single worker gains nothing from a separate process."""
    assert use_process_pool(n_items=64, processes=1, min_items=32) is False


def test_process_pool_threshold_is_the_callers_to_set():
    """The caller's min_items decides, including forcing processes on."""
    assert use_process_pool(n_items=2, processes=8, min_items=0) is True
    assert use_process_pool(n_items=64, processes=8, min_items=1000000) is False


def test_building_references_in_processes(tmpdir, monkeypatch, caplog):
    """References build correctly when the process pool is used."""
    monkeypatch.setattr(
        "teehr.fetching.utils.REFERENCE_BUILD_MIN_ITEMS", 0
    )
    remote_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc",  # noqa
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f002.alaska.nc",  # noqa
    ]

    set_concurrency(cpu=2)
    with caplog.at_level("INFO", logger="teehr"):
        built_files = build_zarr_references_virtualizarr(
            remote_paths=remote_paths,
            json_dir=tmpdir,
            ignore_missing_file=False,
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


def _worker_memory_report(args):
    """Build references, reporting RSS after import and at peak.

    Runs in a spawned worker, so it must be importable at module level.
    """
    paths, json_dir = args
    from teehr.fetching.utils import gen_json_virtualizarr

    def vmhwm():
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
        return 0

    after_import = vmhwm()
    for path in paths:
        gen_json_virtualizarr(path, json_dir, True)
    return after_import, vmhwm()


# Generous next to the ~80MB measured, but a re-inlined coordinate would add
# tens of MB per file and blow straight through it.
MAX_WORK_MEMORY = 300 * 1024**2


@pytest.mark.skipif(
    not Path("/proc/self/status").exists(), reason="needs procfs"
)
def test_reference_work_memory_is_bounded(tmpdir):
    """Building a reference must not hold much beyond the import footprint.

    Asserts the *delta*, not peak RSS: the import footprint swings with the
    interpreter and wheel set (~360MB on py3.14, far more elsewhere), so an
    absolute bound would only measure the runner. The delta is the part teehr
    controls, and it is what a regression -- inlining a large coordinate,
    say -- would show up in. REFERENCE_WORKER_MEMORY has to cover
    import + delta on the *deployment* image, which no CI run can check.
    """
    remote_paths = [
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f001.alaska.nc",  # noqa
        "gcs://national-water-model/nwm.20231101/short_range_alaska/nwm.t00z.short_range.channel_rt.f002.alaska.nc",  # noqa
    ]
    with ProcessPoolExecutor(
        max_workers=2, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        reports = list(pool.map(
            _worker_memory_report,
            [([path], str(tmpdir)) for path in remote_paths],
        ))
    worst = max(peak - after_import for after_import, peak in reports)

    assert worst < MAX_WORK_MEMORY, (
        f"building a reference added {worst / 1024**2:.0f}MB per worker, over"
        f" the {MAX_WORK_MEMORY / 1024**2:.0f}MB allowed; peaks were"
        f" {[f'{p / 1024**2:.0f}MB after {a / 1024**2:.0f}MB import' for a, p in reports]}"
    )


def test_spawn_safety_answer_is_cached(monkeypatch, tmp_path):
    """The script is parsed once, not on every call."""
    script = tmp_path / "guarded.py"
    script.write_text("if __name__ == '__main__':\n    pass\n")
    concurrency.main_module_is_spawn_safe.cache_clear()
    main = types.SimpleNamespace(__spec__=None, __file__=str(script))
    monkeypatch.setitem(sys.modules, "__main__", main)

    assert concurrency.main_module_is_spawn_safe() is True
    script.unlink()  # gone, but the cached answer stands
    assert concurrency.main_module_is_spawn_safe() is True
