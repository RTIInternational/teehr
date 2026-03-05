"""Variable table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.pydantic_table_models import Variable
from teehr.models.pandera_dataframe_schemas import variable_schema
from typing import List, Union
import logging

logger = logging.getLogger(__name__)


class VariableTable(DomainTable):
    """Access methods to variables table."""

    # Table metadata
    table_name = "variables"
    uniqueness_fields = ["name"]
    schema_func = staticmethod(variable_schema)

    def __init__(
        self,
        ev,
        table_name: str = "variables",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBaseModel
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on. Defaults to 'variables'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)

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
