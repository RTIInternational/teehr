"""Module for generating metrics."""
from teehr.evaluation.tables.generic_table import Table

import logging

logger = logging.getLogger(__name__)


class Metrics(Table):
    """Component class for calculating metrics."""

    def __init__(self, ev) -> None:
        """Initialize the Metrics class."""
        super().__init__(ev=ev)
        tbl = super().__call__(
            table_name="joined_timeseries",
        )
        self.__dict__.update(tbl.__dict__)
        self._check_load_table()

