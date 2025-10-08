"""Secondary timeseries table class."""
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from typing import Union
import logging


logger = logging.getLogger(__name__)


class SecondaryTimeseriesTable(TimeseriesTable):
    """Access methods to secondary timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)

    def __call__(
        self,
        table_name: str = "secondary_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the secondary timeseries table.

        Note
        ----
        Creates an instance of a Table class with 'secondary_timeseries'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )

