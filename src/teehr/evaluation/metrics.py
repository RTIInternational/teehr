"""Module for generating metrics."""
from typing import Union
from teehr.evaluation.tables.base_table import Table

import logging

logger = logging.getLogger(__name__)


class Metrics(Table):
    """Component class for calculating metrics."""

    def __init__(self, ev) -> None:
        """Initialize the Metrics class."""
        super().__init__(ev=ev)
        super().__call__(
            table_name="joined_timeseries",
        )
        self._check_load_table()
