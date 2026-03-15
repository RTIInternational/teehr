"""Base class for computed views."""
from abc import abstractmethod
import logging
from typing import Union

from teehr.evaluation.dataframe_base import TeehrDataFrameBase
import pyspark.sql as ps

logger = logging.getLogger(__name__)


class View(TeehrDataFrameBase):
    """Base class for computed views.

    A View represents a computed DataFrame that is evaluated lazily.
    Views can be chained with operations like filter(), query(),
    add_calculated_fields(), and ultimately materialized to an iceberg
    table via write().

    Unlike Tables (which read from persisted iceberg tables), Views
    compute their data on-the-fly when accessed.
    """

    def __init__(
        self,
        ev,
        catalog_name: Union[str, None] = None,
        namespace_name: Union[str, None] = None,
    ):
        """Initialize the View.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related operations.
        catalog_name : Union[str, None], optional
            The catalog containing the source tables. If None, uses the
            active catalog.
        namespace_name : Union[str, None], optional
            The namespace containing the source tables. If None, uses the
            active catalog's namespace.
        """
        super().__init__(ev)
        self._computed = False
        self._catalog_name = catalog_name
        self._namespace_name = namespace_name

    def _get_table(self, table_name: str):
        """Get a table using the stored catalog/namespace if provided.

        Parameters
        ----------
        table_name : str
            The name of the table to access.

        Returns
        -------
        BaseTable
            The table instance from the specified or active catalog/namespace.
        """
        return self._ev.table(
            table_name,
            catalog_name=self._catalog_name,
            namespace_name=self._namespace_name,
        )

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

    def to_sdf(self) -> ps.DataFrame:
        """Return the computed PySpark DataFrame.

        Triggers computation if not already computed. The PySpark DataFrame
        can be further processed using PySpark. Note, PySpark DataFrames are
        lazy and will not be executed until an action is called (e.g.,
        show(), collect(), toPandas()).

        Returns
        -------
        ps.DataFrame
            The computed Spark DataFrame.
        """
        self._ensure_computed()
        return self._sdf
