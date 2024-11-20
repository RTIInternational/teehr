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

PIPE = "│"
ELBOW = "└──"
TEE = "├──"
PIPE_PREFIX = "│   "
SPACE_PREFIX = "    "

def print_tree(
    path,
    prefix="",
    exclude_patterns: List[str] = [""],
    max_depth: int = -1,
    current_depth: int = 0
):
    """Print the directory tree structure."""

    if max_depth != -1 and current_depth > max_depth:
        return

    # Get all files and directories in the current path
    paths = list(Path(path).glob("*"))

    # Exclude specific files and directories
    filtered_files = [f for f in paths if not any(fnmatch.fnmatch(f.name, p) for p in exclude_patterns)]

    # Print the directory tree structure
    entries_count = len(filtered_files)

    for index, full_path in enumerate(filtered_files):
        name = full_path.relative_to(path)
        connector = ELBOW if index == entries_count - 1 else TEE

        if full_path.is_dir():
            print(f"{prefix}{connector} {name}")
            new_prefix = prefix + (PIPE_PREFIX if index != entries_count - 1 else SPACE_PREFIX)
            print_tree(
                path=full_path,
                prefix=new_prefix,
                exclude_patterns=exclude_patterns,
                max_depth=max_depth,
                current_depth=current_depth + 1
            )
        else:
            print(f"{prefix}{connector} {name}")
