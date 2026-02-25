"""Attribute table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.pydantic_table_models import Attribute
from typing import List, Union


class AttributeTable(DomainTable):
    """Access methods to attributes table."""

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
        table_name: str = "attributes",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ) -> "DomainTable":
        """Initialize the Table class for a specific table.

        Parameters
        ----------
        table_name : str
            The name of the table to operate on. Defaults to 'attributes'.
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
        Creates an instance of a Table class with 'attributes'
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
        attribute: Union[Attribute, List[Attribute]]
    ):
        """Add an attribute to the evaluation.

        Parameters
        ----------
        attribute : Union[Attribute, List[Attribute]]
            The attribute domain to add.

        Example
        -------
        >>> from teehr import Attribute
        >>> attribute = Attribute(
        >>>     name="drainage_area",
        >>>     type="continuous",
        >>>     description="Drainage area in square kilometers"
        >>> )
        >>> ev.load.add_attribute(attribute)
        """
        self._add(attribute)
