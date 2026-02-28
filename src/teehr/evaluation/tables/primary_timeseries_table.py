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

    def __init__(
        self,
        ev,
        table_name: str = "primary_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on. Defaults to 'primary_timeseries'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)
