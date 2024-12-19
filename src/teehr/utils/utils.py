"""Module contains utility functions."""
from pathlib import Path
from typing import Union, Tuple
from teehr.utils.s3path import S3Path

def to_path_or_s3path(path: Union[str, Path, S3Path], *args: Tuple[str, ...]) -> Union[Path, S3Path]:
    """Concatenate string(s) to Path or S3Path.

    Check if the path is a string, Path or S3Path.
    If it is a string, check if it is an S3 path (based on beginning of string) and return an S3Path.
    If it is a Path, return a Path.
    If it is an S3Path, return an S3Path.
    Concatenate the strings to the path and return the concatenated path.
    """

    for arg in args:
        if not isinstance(arg, str):
            raise ValueError("Only strings can be concatenated to S3Path or Path")

    if isinstance(path, str):
        # check if s3 path, return an S3Path
        if path.startswith("s3://") or path.startswith("s3a://") or path.startswith("s3n://"):
            path = S3Path(path, *args)
            return path

        # else, try regular path, return a Path
        path = Path(path, *args)
        return path

    if isinstance(path, S3Path):
        path = S3Path(path, *args)
        return path

    # try to treat as path
    path = Path(path, *args)
    return path

def path_to_spark(path: Union[str, Path, S3Path], pattern: str = None) -> str:
        """Convert a Path or S3Path to a string or list of strings."""
        if isinstance(path, S3Path):
            path = str(path)

        if isinstance(path, Path):
            if path.is_dir():
                if pattern is None:
                    path = str(path)
                else:
                    path = [str(f) for f in path.glob(pattern)]
            else:
                path = str(path)

        return path