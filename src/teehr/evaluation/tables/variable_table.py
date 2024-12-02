from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.filters import VariableFilter
from teehr.models.table_enums import VariableFields
from teehr.models.pydantic_table_models import Variable
import teehr.models.pandera_dataframe_schemas as schemas
from typing import List, Union
import logging
from teehr.utils.utils import to_path_or_s3path


logger = logging.getLogger(__name__)


class VariableTable(DomainTable):
    """Access methods to variables table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "variables"
        # self.dir = ev.variables_dir
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.filter_model = VariableFilter
        self.schema_func = schemas.variable_schema

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
        >>> from teehr.models.domain_tables import Variable
        >>> variable = Variable(
        >>>     name="streamflow_hourly_inst",
        >>>     long_name="Instantaneous streamflow"
        >>> )
        >>> ev.load.add_variable(variable)
        """
        self._add(variable)