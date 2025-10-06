"""Variable table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.table_enums import VariableFields
from teehr.models.pydantic_table_models import Variable
from typing import List, Union
import logging


logger = logging.getLogger(__name__)


class VariableTable(DomainTable):
    """Access methods to variables table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)

    def __call__(
        self,
        table_name: str = "variables",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the variables table.

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

    def field_enum(self) -> VariableFields:
        """Get the variable fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return VariableFields(
            "VariableFields",
            {field: field for field in fields}
        )

    def add(
        self,
        variable: Union[Variable, List[Variable]]
    ):
        """Add a unit to the evaluation.

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