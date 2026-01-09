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
        _ = super().__call__(
            table_name="joined_timeseries",
        )
        self._check_load_table()

    def __call__(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ) -> "Metrics":
        """Initialize the Metrics class.

        Parameters
        ----------
        table_name : str
            The name of the table to use for metrics calculations.
        namespace_name : Union[str, None], optional
            The namespace of the table, by default None in which case the
            namespace_name of the active catalog is used.
        catalog_name : Union[str, None], optional
            The catalog of the table, by default None in which case the
            catalog_name of the active catalog is used.

        Example
        -------
        By default, the Metrics class operates on the "joined_timeseries" table.
        This can be changed by specifying a different table name.

        >>> import teehr
        >>> ev = teehr.Evaluation()
        >>> metrics = ev.metrics(table_name="primary_timeseries")
        """
        logger.info(f"Initializing Metrics for table: {table_name}.{namespace_name or ''}{'.' if namespace_name else ''}{catalog_name or ''}")

        self.table_name = table_name
        self.table = self._ev.table(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name,
        )
        self.sdf = self.table.to_sdf()

        return self