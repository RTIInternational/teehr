"""Generic table class implementation."""
import logging
from typing import Union

from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.table_properties import TBLPROPERTIES
from teehr.evaluation.tables.primary_timeseries_table import PrimaryTimeseriesTable
from teehr.evaluation.tables.secondary_timeseries_table import SecondaryTimeseriesTable
from teehr.evaluation.tables.joined_timeseries_table import JoinedTimeseriesTable
from teehr.evaluation.tables.location_table import LocationTable
from teehr.evaluation.tables.location_attribute_table import LocationAttributeTable
from teehr.evaluation.tables.location_crosswalk_table import LocationCrosswalkTable
from teehr.evaluation.tables.unit_table import UnitTable
from teehr.evaluation.tables.variable_table import VariableTable
from teehr.evaluation.tables.configuration_table import ConfigurationTable

logger = logging.getLogger(__name__)

TBL_CLASS_LOOKUP = {
    "primary_timeseries": PrimaryTimeseriesTable,
    "secondary_timeseries": SecondaryTimeseriesTable,
    "joined_timeseries": JoinedTimeseriesTable,
    "locations": LocationTable,
    "location_attributes": LocationAttributeTable,
    "location_crosswalks": LocationCrosswalkTable,
    "units": UnitTable,
    "variables": VariableTable,
    "configurations": ConfigurationTable,
}


class Table(BaseTable):
    """Generic table class that can represent any existing or user-defined table."""

    def __call__(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ) -> "Table":
        """Initialize the Table class."""
        logger.info(f"Initializing Table for table: {table_name}.{namespace_name or ''}{'.' if namespace_name else ''}{catalog_name or ''}")

        if table_name in TBL_CLASS_LOOKUP:
            return TBL_CLASS_LOOKUP[table_name](self._ev)(
                table_name=table_name,
                namespace_name=namespace_name,
                catalog_name=catalog_name
            )
        else:
            self.table_name = table_name
            self.sdf = None
            tbl_props = TBLPROPERTIES.get(table_name)
            if tbl_props is None:
                logger.warning(
                    f"No table properties found for table: '{table_name}'."
                    " Proceeding without table properties."
                )
            else:
                self.uniqueness_fields = tbl_props.get("uniqueness_fields")
                self.foreign_keys = tbl_props.get("foreign_keys")
                self.schema_func = tbl_props.get("schema_func")
                self.filter_model = tbl_props.get("filter_model")
                self.strict_validation = tbl_props.get("strict_validation")
                self.validate_filter_field_types = tbl_props.get("validate_filter_field_types")
                self.field_enum_model = tbl_props.get("field_enum_model")
                self.extraction_func = tbl_props.get("extraction_func")

            if namespace_name is None:
                self.table_namespace_name = self._ev.active_catalog.namespace_name
            else:
                self.table_namespace_name = namespace_name
            if catalog_name is None:
                self.catalog_name = self._ev.active_catalog.catalog_name
            else:
                self.catalog_name = catalog_name
            return self
