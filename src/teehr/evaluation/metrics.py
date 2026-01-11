"""Module for generating metrics."""
from teehr.evaluation.tables.generic_table import Table
from teehr.evaluation.tables.joined_timeseries_table import JoinedTimeseriesTable

import logging

logger = logging.getLogger(__name__)


class Metrics(JoinedTimeseriesTable, Table):
    """Component class for calculating metrics.

    Notes
    -----
    This is essentially a wrapper around the Table class but is initialized
    as a 'joined_timeseries' table by default. Since the JoinedTimeseriesTable
    class is inherited first, its methods will override those in the Table class,
    such as to_geopandas().
    """

    def __init__(self, ev) -> None:
        """Initialize the Metrics class."""
        super().__init__(ev=ev)
        tbl = super().__call__(
            table_name="joined_timeseries",
        )
        self.__dict__.update(tbl.__dict__)