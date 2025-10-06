"""Primary timeseries table class."""
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from typing import Union
import logging


logger = logging.getLogger(__name__)


class PrimaryTimeseriesTable(TimeseriesTable):
    """Access methods to primary timeseries table.

    Note
    ----
    The TimeseriesTable class is used as a base class for both primary and
    secondary timeseries tables, as they share the same loading methods.
    """

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)

    def __call__(
        self,
        table_name: str = "primary_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the primary timeseries table.

        Note
        ----
        Creates an instance of a Table class with 'primary_timeseries'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )
