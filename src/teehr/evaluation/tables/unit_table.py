from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.filters import UnitFilter
from teehr.models.table_enums import UnitFields
from teehr.models.pydantic_table_models import Unit
import teehr.models.pandera_dataframe_schemas as schemas
from typing import List, Union
import logging
from teehr.utils.utils import to_path_or_s3path


logger = logging.getLogger(__name__)


class UnitTable(DomainTable):
    """Access methods to units table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "units"
        # self.dir = ev.units_dir
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.filter_model = UnitFilter
        self.schema_func = schemas.unit_schema

    def field_enum(self) -> UnitFields:
        """Get the unit fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return UnitFields(
            "UnitFields",
            {field: field for field in fields}
        )

    def add(
        self,
        unit: Union[Unit, List[Unit]]
    ):
        """Add a unit to the evaluation.

        Parameters
        ----------
        unit : Union[Unit, List[Unit]]
            The unit domain to add.

        Example
        -------
        >>> from teehr.models.domain_tables import Unit
        >>> unit = Unit(
        >>>     name="m^3/s",
        >>>     long_name="Cubic meters per second"
        >>> )
        >>> ev.load.add_unit(unit)
        """
        self._add(unit)