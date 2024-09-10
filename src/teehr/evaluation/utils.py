"""Utility functions for the evaluation class."""
import logging

from teehr.fetching.const import NWM_VARIABLE_MAPPER, VARIABLE_NAME

logger = logging.getLogger(__name__)


def get_schema_variable_name(variable_name: str) -> str:
    """Get the variable name from the Evaluation schema."""
    logger.info(f"Getting schema variable name for {variable_name}.")
    return NWM_VARIABLE_MAPPER[VARIABLE_NAME]. \
        get(variable_name, variable_name)
