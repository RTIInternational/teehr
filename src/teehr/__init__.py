"""TEEHR logger implementation."""
# Set default logging handler to avoid "No handler found" warnings.
import logging
from logging import NullHandler

__all__ = ["add_stderr_logger"]

# https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
# We don't want the TEEHR logger to interfere with any application using TEEHR.
logging.getLogger(__name__).addHandler(NullHandler())


def add_stderr_logger(level: int = logging.DEBUG) -> logging.StreamHandler:
    """
    Helper for quickly adding a StreamHandler to the logger. Useful for
    debugging.

    Returns the handler after adding it.

    Taken from the urllib3 package.
    """
    # This method needs to be in this __init__.py to get the __name__ correct
    # even if TEEHR is vendored within another package.
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))  # noqa
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.debug("Added a stderr logging handler to logger: %s", __name__)
    return handler
