"""Module contains utility functions."""
from pathlib import Path
import shutil
from typing import Union
import logging

logger = logging.getLogger(__name__)


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
