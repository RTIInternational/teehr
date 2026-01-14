"""Variable table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.pydantic_table_models import Variable
from typing import List, Union
import logging

logger = logging.getLogger(__name__)


class VariableTable(DomainTable):
    """Access methods to variables table."""

    def __init__(self, ev):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        """
        super().__init__(ev)

    def __call__(
        self,
        table_name: str = "variables",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ) -> "DomainTable":
        """Initialize the Table class for a specific table.

        Parameters
        ----------
        table_name : str
            The name of the table to operate on. Defaults to 'variables'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.

        Returns
        -------
        "DomainTable"
            The initialized Table instance ready for operations.

        Note
        ----
        Creates an instance of a Table class with 'variables'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )

    def add(
        self,
        variable: Union[Variable, List[Variable]]
    ):
        """Add a variable to the evaluation.

        Parameters
        ----------
        variable : Union[Variable, List[Variable]]
            The variable domain to add.

        Example
        -------
        >>> from teehr import Variable
        >>> variable = Variable(
        >>>     name="streamflow_hourly_inst",
        >>>     long_name="Instantaneous streamflow"
        >>> )
        >>> ev.load.add_variable(variable)
        """
        self._add(variable)