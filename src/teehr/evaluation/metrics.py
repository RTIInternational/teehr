"""Module for generating metrics."""
import logging
from teehr.evaluation.tables import BaseTable

logger = logging.getLogger(__name__)


class Metrics(BaseTable):
    """Component class for calculating metrics.

    .. deprecated:: 0.6.0
        The ``Metrics`` class (accessed via ``ev.metrics``) is deprecated and
        will be removed in a future version. Use the ``query`` method on the
        table directly with the ``include_metrics`` argument instead.

        For example:

        .. code-block:: python

            ev.table("joined_timeseries").query(
                include_metrics=[...],
                group_by=[...],
                order_by=[...],
            )

    Notes
    -----
    This is essentially a wrapper around the JoinedTimeseriesTable class but
    is initialized as a 'joined_timeseries' table by default.
    """

    def __init__(self, ev) -> None:
        """Initialize the Metrics class."""
        super().__init__(ev=ev, table_name="joined_timeseries")
