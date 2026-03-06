"""Unit table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.pydantic_table_models import Unit
from teehr.models.pandera_dataframe_schemas import unit_schema
from typing import List, Union
import logging

logger = logging.getLogger(__name__)


class UnitTable(DomainTable):
    """Access methods to units table."""

    # Table metadata
    table_name = "units"
    uniqueness_fields = ["name"]
    schema_func = staticmethod(unit_schema)

    def __init__(
        self,
        ev,
        table_name: str = "units",
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
            The name of the table to operate on. Defaults to 'units'.
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
