"""How much work runs at once, and where it runs.

Any job that touches many things has two limits worth keeping separate:

* **I/O** -- work that mostly waits (network requests, object-store reads).
  Waiting is cheap, so hundreds can be in flight at once.
* **CPU** -- work that mostly computes (parsing, regridding, statistics).
  Bounded by cores, and by the GIL when the work holds it.

:func:`resolve_budget` turns those into a :class:`ConcurrencyBudget` for one
operation; the helpers below do the running. Nothing here is specific to
fetching -- any teehr code wanting parallelism can use it.

Run a blocking function over many items::

    from teehr.utils.concurrency import map_blocking, resolve_budget

    budget = resolve_budget()
    results = await map_blocking(convert_one, paths, workers=budget.cpu)

The same thing from ordinary, non-async code::

    from functools import partial
    from teehr.utils.concurrency import resolve_budget, run_concurrent_map

    results = run_concurrent_map(
        partial(convert_one, out_dir=out), paths, resolve_budget().cpu
    )

Run many coroutines, N at a time::

    from teehr.utils.concurrency import gather_bounded, resolve_budget

    rows = await gather_bounded(fetch_one, urls, limit=resolve_budget().io)

Mix the two -- download many at once, parse only a few::

    budget = resolve_budget()
    with thread_pool(budget.cpu, len(urls)) as pool:
        datasets = await gather_bounded(
            lambda url: download_and_parse(url, pool), urls, limit=budget.io
        )

For CPU-heavy work, worker processes beat threads because they don't share a
GIL -- but each costs seconds to start and hundreds of MB, so it only pays
off on big jobs. Let :func:`use_process_pool` decide, and tell
:func:`resolve_budget` what a worker costs so the count fits in memory::

    budget = resolve_budget(memory_per_process=700 * 1024**2)
    results = await map_blocking(
        parse_one, items, workers=budget.cpu,
        processes=budget.processes
        if use_process_pool(len(items), budget.processes, min_items=32) else 0,
    )

Two things to know before using processes. ``min_items`` is yours to
measure -- it is what repays the startup cost for *your* per-item work. And
anything expensive a worker needs (a client, a registry) should be built
lazily inside the function and cached with ``@lru_cache``, so every process
builds its own once instead of pickling one across.

Users can change the defaults for a whole session::

    from teehr.utils.concurrency import set_concurrency
    set_concurrency(io=8, cpu=2)

**Calling teehr from something that already runs work in parallel** -- a
Prefect flow mapping tasks, say -- means the budgets multiply: six callers at
``io=48`` puts 288 requests in flight. Divide first::

    set_concurrency(io=48 // n_tasks)
"""
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from pickle import PicklingError
from typing import Any, Callable, Coroutine, Iterable, List, Optional, TypeVar, Union
import ast
import asyncio
import logging
import multiprocessing
import os
import sys
import threading

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Network waits, so this can be far above the core count. A round default
# rather than a measured one, and the only budget that never consults the
# machine -- pass io= if you know what your link and object store can take.
DEFAULT_IO_CONCURRENCY = 48
MAX_IO_CONCURRENCY = 256

# Sanity ceilings on an explicit override, not tuning -- they only guard
# against a mistyped budget. MAX_CPU_WORKERS bounds worker *processes* too,
# since resolve_cpu_processes never exceeds the cpu budget.
MAX_CPU_WORKERS = 64

# Set by set_concurrency(); None means "check the environment, then the
# default". Resolved per call, not at import, so a notebook can change these
# after `import teehr`.
_io_concurrency: Optional[int] = None
_cpu_workers: Optional[int] = None


@dataclass(frozen=True)
class ConcurrencyBudget:
    """How much of each kind of work may run at once.

    Attributes
    ----------
    io : int
        Network operations in flight.
    cpu : int
        Compute-bound calls running at once, in threads or processes.
    processes : int
        Of ``cpu``, how many may be worker processes.
    """

    io: int
    cpu: int
    processes: int


def _cgroup_cpu_quota(root: Union[str, Path] = "/sys/fs/cgroup") -> Optional[int]:
    """Return this cgroup's CPU limit in whole cores, or None if unlimited.

    Kubernetes caps CPU with a quota rather than by restricting which cores a
    process may use, so nothing else reports it.

    Parameters
    ----------
    root : str or Path
        Root of the cgroup filesystem. Exposed for testing.
    """
    root = Path(root)
    try:  # cgroup v2
        quota, period = (root / "cpu.max").read_text().split()
        if quota != "max":
            return max(1, -(-int(quota) // int(period)))
        return None
    except (OSError, ValueError):
        pass
    try:  # cgroup v1
        quota = int((root / "cpu" / "cpu.cfs_quota_us").read_text())
        period = int((root / "cpu" / "cpu.cfs_period_us").read_text())
        if quota > 0 and period > 0:
            return max(1, -(-quota // period))
    except (OSError, ValueError):
        pass
    return None


def _cgroup_memory_limit(
    root: Union[str, Path] = "/sys/fs/cgroup",
) -> Optional[int]:
    """Return this cgroup's memory limit in bytes, or None if unlimited.

    Parameters
    ----------
    root : str or Path
        Root of the cgroup filesystem. Exposed for testing.
    """
    root = Path(root)
    try:  # cgroup v2
        raw = (root / "memory.max").read_text().strip()
        return None if raw == "max" else int(raw)
    except (OSError, ValueError):
        pass
    try:  # cgroup v1, which uses a huge sentinel rather than "max"
        value = int((root / "memory" / "memory.limit_in_bytes").read_text())
        if 0 < value < 2**62:
            return value
    except (OSError, ValueError):
        pass
    return None


def _meminfo_available() -> Optional[int]:
    """Return MemAvailable in bytes, or None if it can't be read."""
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def available_memory() -> Optional[int]:
    """Return the memory this process can expect to use, in bytes.

    Takes the smaller of any cgroup limit and what the machine reports free,
    so a container gets its own allowance rather than the host's. Returns None
    when neither can be read (e.g. macOS), in which case callers should fall
    back to bounding by CPU alone.
    """
    candidates = [
        value for value in (_cgroup_memory_limit(), _meminfo_available())
        if value
    ]
    return min(candidates) if candidates else None


def available_cpus() -> int:
    """Return how many CPUs this process can really use.

    ``os.cpu_count()`` reports the whole machine, which over-counts badly
    inside a container, so this also takes the affinity mask and cgroup quota
    into account.
    """
    try:
        cpus = len(os.sched_getaffinity(0))
    except AttributeError:
        # sched_getaffinity is Linux-only.
        cpus = os.cpu_count() or 4

    quota = _cgroup_cpu_quota()
    return min(cpus, quota) if quota else cpus


def _clamp(value: int, lo: int, hi: int, name: str) -> int:
    """Clamp ``value`` into ``[lo, hi]``, warning if it was out of range."""
    if value < lo or value > hi:
        logger.warning(
            f"{name}={value} is outside the supported range [{lo}, {hi}];"
            f" using {min(max(value, lo), hi)} instead."
        )
    return min(max(value, lo), hi)


def _int_from_env(name: str) -> Optional[int]:
    """Return ``name`` from the environment as an integer, or None."""
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning(f"Ignoring {name}={raw!r}; expected an integer.")
        return None


def resolve_io_concurrency(override: Optional[int] = None) -> int:
    """Resolve how many network operations may be in flight.

    In order: ``override``, then :func:`set_concurrency`, then
    ``TEEHR_IO_CONCURRENCY``, then
    :data:`DEFAULT_IO_CONCURRENCY`.
    """
    value = override
    if value is None:
        value = _io_concurrency
    if value is None:
        value = _int_from_env("TEEHR_IO_CONCURRENCY")
    if value is None:
        value = DEFAULT_IO_CONCURRENCY
    return _clamp(int(value), 1, MAX_IO_CONCURRENCY, "io_concurrency")


def resolve_cpu_workers(override: Optional[int] = None) -> int:
    """Resolve how many compute-bound calls may run at once.

    In order: ``override``, then :func:`set_concurrency`, then
    ``TEEHR_CPU_WORKERS``, then
    :func:`available_cpus`.
    """
    value = override
    if value is None:
        value = _cpu_workers
    if value is None:
        value = _int_from_env("TEEHR_CPU_WORKERS")
    if value is None:
        value = available_cpus()
    return _clamp(int(value), 1, MAX_CPU_WORKERS, "cpu_workers")


def resolve_cpu_processes(
    workers: Optional[int] = None,
    memory_per_process: Optional[int] = None,
) -> int:
    """Resolve how many of the CPU workers may be separate processes.

    Never more than :func:`resolve_cpu_workers`. Callers who know what a
    worker costs pass ``memory_per_process`` and get a count that also fits in
    :func:`available_memory`, so a bigger machine gets more workers without
    anyone configuring it and a memory-starved one gets fewer.

    Parameters
    ----------
    workers : Optional[int]
        CPU budget to cap; resolved from the process-wide setting if omitted.
    memory_per_process : Optional[int]
        Bytes one worker is expected to peak at, measured by the caller for
        its own work. Omit to bound by CPU alone.
    """
    workers = resolve_cpu_workers(workers)
    memory = available_memory()
    if memory_per_process and memory:
        # Scale down on a small machine rather than risking an OOM kill.
        workers = min(workers, max(1, memory // memory_per_process))
    return max(1, workers)


def resolve_budget(
    io: Optional[int] = None,
    cpu: Optional[int] = None,
    memory_per_process: Optional[int] = None,
) -> ConcurrencyBudget:
    """Work out the budget for one operation.

    Call this once at the top of an operation and pass the result down, rather
    than re-resolving in each helper.

    Parameters
    ----------
    io : Optional[int]
        Override for network concurrency.
    cpu : Optional[int]
        Override for compute concurrency.
    memory_per_process : Optional[int]
        Bytes one worker process is expected to peak at. Only affects
        ``processes``; omit unless you intend to use them.

    Examples
    --------
    >>> budget = resolve_budget()          # process-wide defaults
    >>> budget = resolve_budget(io=8)      # ...only 8 requests in flight
    """
    return ConcurrencyBudget(
        io=resolve_io_concurrency(io),
        cpu=resolve_cpu_workers(cpu),
        processes=resolve_cpu_processes(cpu, memory_per_process),
    )


def set_concurrency(
    io: Optional[int] = None,
    cpu: Optional[int] = None,
) -> None:
    """Change the defaults for the rest of this session.

    Applies everywhere, so callers don't have to pass arguments through.
    Anything left as ``None`` is unchanged; :func:`reset_concurrency` undoes it.

    Parameters
    ----------
    io : Optional[int]
        Network operations in flight. Lower this when something else is
        already fetching in parallel -- the budgets multiply.
    cpu : Optional[int]
        Compute-bound calls running at once.

    Examples
    --------
    >>> set_concurrency(io=8, cpu=2)
    """
    global _io_concurrency, _cpu_workers
    if io is not None:
        _io_concurrency = _clamp(int(io), 1, MAX_IO_CONCURRENCY, "io")
    if cpu is not None:
        _cpu_workers = _clamp(int(cpu), 1, MAX_CPU_WORKERS, "cpu")


def reset_concurrency() -> None:
    """Undo :func:`set_concurrency`, going back to the defaults."""
    global _io_concurrency, _cpu_workers
    _io_concurrency = None
    _cpu_workers = None


def _has_main_guard(path: Union[str, Path]) -> bool:
    """Whether a script keeps its work behind ``if __name__ == "__main__"``.

    Parameters
    ----------
    path : str or Path
        Script to inspect.
    """
    try:
        tree = ast.parse(Path(path).read_text())
    except (OSError, SyntaxError, ValueError, UnicodeDecodeError):
        # Can't prove it's guarded, so assume it isn't.
        return False
    return any(
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
        for node in tree.body
    )


@lru_cache(maxsize=1)
def main_module_is_spawn_safe() -> bool:
    """Whether starting worker processes would re-run the caller's own code.

    Every worker re-imports whatever module was run as ``__main__``. That is
    fine for a notebook, for ``python -m something``, or for a script that
    keeps its work behind ``if __name__ == "__main__":``. In a plain unguarded
    script it is not: the script would run again inside every worker. When that
    is the case teehr stays on threads and says so once.
    """
    main = sys.modules.get("__main__")
    path = getattr(main, "__file__", None)
    if getattr(main, "__spec__", None) is not None or path is None:
        return True
    if _has_main_guard(path):
        return True
    logger.info(
        f"Using threads instead of worker processes: {path} has no"
        " `if __name__ == \"__main__\":` guard, so workers would re-run it."
        " Add one to get the faster path."
    )
    return False


def in_worker_process() -> bool:
    """Whether this code is already running inside a worker process.

    Used to avoid nesting a pool inside a pool: if something above us -- an
    orchestrator's process-based task runner, or another teehr pool -- already
    put us in a worker, spawning more processes here multiplies the load
    instead of dividing the work.
    """
    return multiprocessing.parent_process() is not None


def use_process_pool(
    n_items: int,
    processes: int,
    min_items: int,
) -> bool:
    """Whether a job this size is worth handing to worker processes.

    Processes escape the GIL, but each costs seconds to start, so the trade
    only pays off once there is enough work to repay that. Says no as well
    when we are already inside a worker (:func:`in_worker_process`), or when
    workers would re-run the caller's script
    (:func:`main_module_is_spawn_safe`).

    Parameters
    ----------
    n_items : int
        How many items the job has.
    processes : int
        Worker processes available, from :attr:`ConcurrencyBudget.processes`.
    min_items : int
        Fewest items worth starting processes for. No default: it is
        ``startup_cost / per_item_cost``, which only the caller can measure.
    """
    if processes < 2 or n_items < 2:
        return False
    if n_items < min_items:
        return False
    if in_worker_process():
        logger.debug(
            "Already inside a worker process; leaving parallelism to whatever"
            " started it."
        )
        return False
    return main_module_is_spawn_safe()


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine to completion from ordinary, non-async code.

    Works whether or not the calling thread already has an event loop running
    (a notebook, say), so callers never have to think about it.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop running in this thread; safe to drive the coroutine directly.
        return asyncio.run(coro)

    result: list = []
    error: list = []

    def _runner():
        try:
            result.append(asyncio.run(coro))
        except BaseException as exc:
            error.append(exc)

    thread = threading.Thread(target=_runner)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return result[0]


@contextmanager
def thread_pool(workers: int, n_items: int, name: str = "teehr-worker"):
    """A thread pool sized for the job at hand.

    Never makes more threads than there are items, and never borrows the event
    loop's default pool, whose size ignores both our budget and the CPUs this
    process was given.

    Parameters
    ----------
    workers : int
        Upper bound on threads, usually :attr:`ConcurrencyBudget.cpu`.
    n_items : int
        How many items will be submitted.
    name : str
        Thread name prefix, to make stack traces readable.
    """
    with ThreadPoolExecutor(
        max_workers=max(1, min(workers, n_items)), thread_name_prefix=name
    ) as pool:
        yield pool


async def run_in_executor(fn: Callable[[], T], executor: Optional[Executor] = None) -> T:
    """Run one blocking call off the event loop.

    Parameters
    ----------
    fn : Callable
        Blocking callable taking no arguments; bind arguments with
        ``functools.partial``.
    executor : Optional[Executor]
        Pool to run in, usually from :func:`thread_pool`. Without one this
        falls back to the event loop's default pool.
    """
    if executor is None:
        return await asyncio.to_thread(fn)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, fn)


async def gather_bounded(
    factory: Callable[[Any], Coroutine[Any, Any, T]],
    items: Iterable[Any],
    *,
    limit: int,
) -> List[T]:
    """Await ``factory(item)`` for every item, at most ``limit`` at a time.

    Results come back in the order the items were given.

    Parameters
    ----------
    factory : Callable
        Called with each item; returns the coroutine to await.
    items : Iterable
        Items to process.
    limit : int
        How many may be in flight at once, usually
        :attr:`ConcurrencyBudget.io`.

    Examples
    --------
    >>> await gather_bounded(fetch_one, urls, limit=budget.io)
    """
    items = list(items)
    if not items:
        return []
    semaphore = asyncio.Semaphore(max(1, min(limit, len(items))))

    async def _one(item: Any) -> T:
        async with semaphore:
            return await factory(item)

    return list(await asyncio.gather(*[_one(item) for item in items]))


async def map_blocking(
    fn: Callable[..., T],
    items: Iterable[Any],
    *,
    workers: int,
    args: tuple = (),
    processes: int = 0,
    initializer: Optional[Callable[..., None]] = None,
    initargs: tuple = (),
    on_complete: Optional[Callable[[int, T], None]] = None,
) -> List[T]:
    """Run ``fn(item, *args)`` for every item, off the event loop.

    Threads by default. Pass ``processes`` to run in that many worker processes
    instead, which is worth it for CPU-heavy work on big jobs -- ask
    :func:`use_process_pool` rather than deciding by hand. ``fn`` and ``args``
    must be picklable in that case, and anything expensive that a worker needs
    (a client, a registry) is best built lazily inside ``fn`` and cached, so
    each worker makes its own once.

    Falls back to threads if worker processes can't be started at all; errors
    raised by ``fn`` itself propagate as usual.

    Parameters
    ----------
    fn : Callable
        Blocking callable, called as ``fn(item, *args)``.
    items : Iterable
        Items to process.
    workers : int
        Thread count, usually :attr:`ConcurrencyBudget.cpu`.
    args : tuple
        Extra arguments passed to every call.
    processes : int
        Worker processes to use instead of threads; 0 means threads.
    initializer : Optional[Callable]
        Run once in each worker process before any item, e.g. to give it its
        share of the budget. Must be picklable, as must ``initargs``.
    initargs : tuple
        Arguments for ``initializer``.
    on_complete : Optional[Callable]
        Called in *this* process as each item finishes, with its index and
        result. Worker logs don't reach the parent, so this is how a caller
        reports progress.

    Returns
    -------
    List
        Results, in the order the items were given.

    Examples
    --------
    >>> budget = resolve_budget()
    >>> await map_blocking(
    ...     build_one, paths, workers=budget.cpu,
    ...     processes=budget.processes if use_process_pool(
    ...         len(paths), budget.processes, min_items=32) else 0,
    ... )
    """
    items = list(items)
    if not items:
        return []
    loop = asyncio.get_running_loop()

    async def _run(pool: Executor) -> List[T]:
        # The pool's size is the concurrency bound; no semaphore needed.
        async def _one(index: int, item: Any) -> T:
            result = await loop.run_in_executor(pool, fn, item, *args)
            if on_complete is not None:
                # Report as this item lands rather than going quiet until the
                # whole batch finishes; gather still returns items in order.
                on_complete(index, result)
            return result

        return list(await asyncio.gather(
            *[_one(index, item) for index, item in enumerate(items)]
        ))

    if processes > 1:
        try:
            # "spawn", not "fork": callers often have threads running already
            # (obstore, a Spark JVM, Jupyter), and forking those can deadlock.
            pool = ProcessPoolExecutor(
                max_workers=min(processes, len(items)),
                mp_context=multiprocessing.get_context("spawn"),
                initializer=initializer,
                initargs=initargs,
            )
        except OSError as e:
            logger.warning(f"Could not start worker processes ({e}); using threads.")
        else:
            try:
                result = await _run(pool)
            except (BrokenProcessPool, PicklingError) as e:
                pool.shutdown(wait=False, cancel_futures=True)
                logger.warning(
                    f"Worker processes were unusable"
                    f" ({type(e).__name__}: {e}); using threads instead."
                    " A worker killed for memory reports as BrokenProcessPool"
                    " -- check the pod for an OOMKilled event."
                )
            except BaseException:
                # Fail fast: drop queued work rather than making the caller
                # wait out the rest of a job whose result they can't use.
                # Items already running can't be interrupted, so this stops
                # within one item per busy worker.
                pool.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                pool.shutdown(wait=True)
                return result

    with thread_pool(workers, len(items)) as pool:
        return await _run(pool)


def run_concurrent_map(
    func: Callable[[Any], T],
    items: Iterable[Any],
    max_workers: Optional[int] = None,
) -> List[T]:
    """Run ``func(item)`` for every item in threads, from non-async code.

    The blocking-code counterpart to :func:`map_blocking`; bind extra arguments
    with ``functools.partial``.

    Parameters
    ----------
    func : Callable
        Blocking callable taking one item.
    items : Iterable
        Items to process.
    max_workers : Optional[int]
        Thread count. Defaults to the cpu budget, which is what callers
        wanting anything other than a deliberate override should use.
    """
    return run_sync(
        map_blocking(func, items, workers=resolve_cpu_workers(max_workers))
    )
