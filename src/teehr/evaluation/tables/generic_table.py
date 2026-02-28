"""Generic table class implementation."""
import logging
from typing import Union

from teehr.evaluation.tables.base_table import BaseTable
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


def get_table(
    ev,
    table_name: str,
    namespace_name: Union[str, None] = None,
    catalog_name: Union[str, None] = None
) -> BaseTable:
    """Factory function to get the appropriate table class for a table name.

    This function returns the specialized table class if one exists for the
    given table_name, otherwise returns a generic BaseTable instance.

    Parameters
    ----------
    ev : EvaluationBase
        The parent Evaluation instance.
    table_name : str
        The name of the table to operate on.
    namespace_name : Union[str, None], optional
        The namespace containing the table. If None, uses the
        active catalog's namespace.
    catalog_name : Union[str, None], optional
        The catalog containing the table. If None, uses the
        active catalog name.

    Returns
    -------
    BaseTable
        The appropriate table instance for the given table name.

    Examples
    --------
    >>> # Get a known table type (returns specialized class)
    >>> pts = ev.table("primary_timeseries")

    >>> # Get an unknown/user-defined table (returns BaseTable)
    >>> custom = ev.table("my_custom_table")
    """
    logger.info(
        f"Getting table: {table_name}"
        f".{namespace_name or ''}"
        f"{'.' if namespace_name else ''}{catalog_name or ''}"
    )

    if table_name in TBL_CLASS_LOOKUP:
        return TBL_CLASS_LOOKUP[table_name](
            ev,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )
    else:
        return BaseTable(
            ev,
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )
