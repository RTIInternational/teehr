"""TEEHR: the Python SDK and CLI for TEEHR Cloud."""

from importlib.metadata import PackageNotFoundError, version

from teehr.client import Client, NotSignedIn, TeehrError

try:
    __version__ = version("teehr")
except PackageNotFoundError:  # running from a checkout
    __version__ = "0+unknown"

__all__ = ["Client", "NotSignedIn", "TeehrError", "__version__"]
