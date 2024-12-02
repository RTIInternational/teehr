"""Module contains S3Path.
A class that represents an S3 path and allows for easy concatenation of paths.
"""
from typing import Tuple

class S3Path():
    """Class that represents an S3 path and allows for easy concatenation of paths."""

    def __init__(self, path: str, *args: Tuple[str, ...]):
        self.path = str(path)

        # check that the path starts with "s3://", "s3a://" or "s3n://"
        if (
            not self.path.startswith("s3://") and
            not self.path.startswith("s3a://") and
            not self.path.startswith("s3n://")
        ):
            raise ValueError("Invalid S3 path")

        if args:
            for arg in args:
                if not isinstance(arg, str):
                    raise ValueError("Only strings can be concatenated to S3Path")
                self.path = self.path + "/" + arg

    def __truediv__(self, other):
        if not isinstance(other, str):
            raise ValueError("Only strings can be concatenated to S3Path")
        return S3Path(self.path + "/" + other)

    # string representation of the path
    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"S3Path({self.path})"