"""Base class for computed views."""
from abc import abstractmethod
import logging

from teehr.evaluation.dataframe_base import DataFrameBase
import pyspark.sql as ps

logger = logging.getLogger(__name__)


class View(DataFrameBase):
    """Base class for computed views.

    A View represents a computed DataFrame that is evaluated lazily.
    Views can be chained with operations like filter(), query(),
    add_calculated_fields(), and ultimately materialized to an iceberg
    table via write().

    Unlike Tables (which read from persisted iceberg tables), Views
    compute their data on-the-fly when accessed.
    """

    def __init__(self, ev):
        """Initialize the View.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related operations.
        """
        super().__init__(ev)
        self._computed = False

    @abstractmethod
    def _compute(self) -> ps.DataFrame:
        """Compute the view's DataFrame.

        Override this method in subclasses to define the computation logic.

        Returns
        -------
        ps.DataFrame
            The computed Spark DataFrame.
        """
        pass

    def _ensure_computed(self):
        """Ensure the view has been computed."""
        if not self._computed:
            logger.debug(f"Computing view: {self.__class__.__name__}")
            self._sdf = self._compute()
            self._computed = True

    @property
    def sdf(self) -> ps.DataFrame:
        """Get the computed Spark DataFrame.

        Triggers computation if not already computed.
        """
        self._ensure_computed()
        return self._sdf

    @sdf.setter
    def sdf(self, value: ps.DataFrame):
        """Set the Spark DataFrame (for chained operations)."""
        self._sdf = value
        self._computed = True
