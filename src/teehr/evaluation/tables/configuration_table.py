"""Configuration table class."""
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.table_enums import ConfigurationFields
from teehr.models.pydantic_table_models import Configuration
import teehr.models.pandera_dataframe_schemas as schemas
import teehr.models.filters as table_filters
from typing import List, Union


class ConfigurationTable(DomainTable):
    """Access methods to configurations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.filter_model = table_filters.ConfigurationFilter
        self.schema_func = schemas.configuration_schema
        self.field_enum_model = ConfigurationFields

    def __call__(
        self,
        table_name: str = "configurations",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the configurations table.

        Note
        ----
        Creates an instance of a Table class with 'configurations'
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
        configuration: Union[Configuration, List[Configuration]]
    ):
        """Add a configuration domain to the evaluation.

        Parameters
        ----------
        configuration : Union[Configuration, List[Configuration]]
            The configuration domain to add.

        Example
        -------
        >>> from teehr import Configuration
        >>> configuration = Configuration(
        >>>     name="usgs_observations",
        >>>     type="primary",
        >>>     description="USGS observations",
        >>> )
        >>> ev.load.add_configuration(configuration)

        """
        self._add(configuration)