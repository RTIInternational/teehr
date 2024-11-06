"""Utility functions for the evaluation class."""
import logging
import fnmatch
from typing import List
from pathlib import Path

from teehr.fetching.const import NWM_VARIABLE_MAPPER, VARIABLE_NAME

logger = logging.getLogger(__name__)


def get_schema_variable_name(variable_name: str) -> str:
    """Get the variable name from the Evaluation schema."""
    logger.info(f"Getting schema variable name for {variable_name}.")
    return NWM_VARIABLE_MAPPER[VARIABLE_NAME]. \
        get(variable_name, variable_name)


def print_tree(path, prefix="", exclude_patterns: List[str] = [""]):
    """Print the directory tree structure."""

    # Get all files and directories in the current path
    paths = list(Path(path).glob("*"))

    # Exclude specific files and directories
    filtered_files = [f for f in paths if not any(fnmatch.fnmatch(f.name, p) for p in exclude_patterns)]

    # Print the directory tree structure
    for full_path in filtered_files:
        name = full_path.relative_to(path)
        if full_path.is_dir():
            print(f"{prefix}├── {name}")
            print_tree(path=full_path, prefix=f"{prefix}│   ", exclude_patterns=exclude_patterns)
        else:
            print(f"{prefix}├── {name}")
