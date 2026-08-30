"""Module contains utility functions."""
from pathlib import Path
import shutil
from typing import Union, TypeVar, Coroutine, Callable, Iterable, List, Any
import asyncio
import threading
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine to completion from synchronous code.

    Works whether or not an event loop is already running in the calling
    thread (e.g. inside Jupyter, or when called from within another async
    framework), by falling back to running the coroutine in a fresh thread
    with its own event loop in that case.
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


def run_concurrent_map(
    func: Callable[[Any], T],
    items: Iterable[Any],
    max_workers: int,
) -> List[T]:
    """Run ``func(item)`` for each item concurrently, bounded by ``max_workers``.

    Each call is offloaded to a thread via ``asyncio.to_thread`` (so ``func``
    can be an ordinary blocking function -- use ``functools.partial`` to bind
    any additional fixed arguments), gathered in the same order as ``items``,
    and driven synchronously via :func:`run_sync` so callers don't need to
    manage an event loop themselves.
    """
    items = list(items)
    if not items:
        return []
    semaphore = asyncio.Semaphore(min(max_workers, len(items)))

    async def _bounded(item: Any) -> T:
        async with semaphore:
            return await asyncio.to_thread(func, item)

    async def _run_all() -> List[T]:
        return await asyncio.gather(*[_bounded(item) for item in items])

    return list(run_sync(_run_all()))


def path_to_spark(path: Union[str, Path], pattern: str = None) -> str:
    """Convert a Path to a string or list of strings."""
    if isinstance(path, Path):
        if path.is_dir():
            if pattern is None:
                path = str(path)
            else:
                path = [str(f) for f in path.glob(pattern)]
        else:
            path = str(path)

    return path


def remove_dir_if_exists(path: Union[str, Path]):
    """Remove directory if it exists."""
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        shutil.rmtree(path)
