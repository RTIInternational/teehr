"""Tables module - persisted iceberg tables."""
from teehr.evaluation.tables.base_table import BaseTable
from teehr.evaluation.tables.domain_table import DomainTable
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.evaluation.tables.primary_timeseries_table import (
    PrimaryTimeseriesTable
)
from teehr.evaluation.tables.secondary_timeseries_table import (
    SecondaryTimeseriesTable
)
from teehr.evaluation.tables.location_table import LocationTable
from teehr.evaluation.tables.location_attribute_table import (
    LocationAttributeTable
)
from teehr.evaluation.tables.location_crosswalk_table import (
    LocationCrosswalkTable
)
from teehr.evaluation.tables.unit_table import UnitTable
from teehr.evaluation.tables.variable_table import VariableTable
from teehr.evaluation.tables.configuration_table import ConfigurationTable
from teehr.evaluation.tables.attribute_table import AttributeTable
from teehr.evaluation.tables.generic_table import get_table

__all__ = [
    "BaseTable",
    "DomainTable",
    "TimeseriesTable",
    "PrimaryTimeseriesTable",
    "SecondaryTimeseriesTable",
    "LocationTable",
    "LocationAttributeTable",
    "LocationCrosswalkTable",
    "UnitTable",
    "VariableTable",
    "ConfigurationTable",
    "AttributeTable",
    "get_table",
]
