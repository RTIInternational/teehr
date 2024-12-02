from teehr.evaluation.tables.domain_table import DomainTable
from teehr.models.filters import AttributeFilter
from teehr.models.table_enums import AttributeFields
from teehr.models.pydantic_table_models import Attribute
import teehr.models.pandera_dataframe_schemas as schemas
from typing import List, Union
from teehr.utils.utils import to_path_or_s3path


class AttributeTable(DomainTable):
    """Access methods to attributes table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "attributes"
        # self.dir = ev.attributes_dir
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.filter_model = AttributeFilter
        self.schema_func = schemas.attribute_schema


    def field_enum(self) -> AttributeFields:
        """Get the attribute fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return AttributeFields(
            "AttributeFields",
            {field: field for field in fields}
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
        >>> from teehr.models.domain_tables import Attribute
        >>> attribute = Attribute(
        >>>     name="drainage_area",
        >>>     type="continuous",
        >>>     description="Drainage area in square kilometers"
        >>> )
        >>> ev.load.add_attribute(attribute)
        """
        self._add(attribute)