from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.filters import ConfigurationFilter
from teehr.models.table_enums import ConfigurationFields
from teehr.models.pydantic_table_models import Configuration
import teehr.models.pandera_dataframe_schemas as schemas


from typing import List, Union


class ConfigurationTable(DomainTable):
    """Access methods to configurations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.dir = ev.configurations_dir
        # self.table_model = Configuration
        self.filter_model = ConfigurationFilter
        self.schema_func = schemas.configuration_schema
        # self._load_table()

    def field_enum(self) -> ConfigurationFields:
        """Get the configuration fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return ConfigurationFields(
            "ConfigurationFields",
            {field: field for field in fields}
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
        >>> from teehr.models.domain_tables import Configuration
        >>> configuration = Configuration(
        >>>     name="usgs_observations",
        >>>     type="primary",
        >>>     description="USGS observations",
        >>> )
        >>> ev.load.add_configuration(configuration)

        """
        self._add(configuration)