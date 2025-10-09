"""Unit table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.table_enums import UnitFields
from teehr.models.pydantic_table_models import Unit
from typing import List, Union
import logging


logger = logging.getLogger(__name__)


class UnitTable(DomainTable):
    """Access methods to units table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)

    def __call__(
        self,
        table_name: str = "units",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the units table.

        Note
        ----
        Creates an instance of a Table class with 'units'
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
        unit: Union[Unit, List[Unit]]
    ):
        """Add a unit to the evaluation.

        Parameters
        ----------
        unit : Union[Unit, List[Unit]]
            The unit domain to add.

        Example
        -------
        >>> from teehr import Unit
        >>> unit = Unit(
        >>>     name="m^3/s",
        >>>     long_name="Cubic meters per second"
        >>> )
        >>> ev.load.add_unit(unit)
        """
        self._add(unit)