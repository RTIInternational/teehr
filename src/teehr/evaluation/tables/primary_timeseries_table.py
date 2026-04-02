"""Primary timeseries table class."""
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.models.pandera_dataframe_schemas import primary_timeseries_schema
from typing import List, Dict, Union
import logging

logger = logging.getLogger(__name__)


class PrimaryTimeseriesTable(TimeseriesTable):
    """Access methods to primary timeseries table.

    Note
    ----
    The TimeseriesTable class is used as a base class for both primary and
    secondary timeseries tables, as they share the same loading methods.
    """

    # Table metadata
    table_name = "primary_timeseries"
    uniqueness_fields = [
        "location_id",
        "value_time",
        "reference_time",
        "variable_name",
        "unit_name",
        "configuration_name"
    ]
    nullable_fields = ["reference_time"]  # Fields that can be NULL
    foreign_keys: List[Dict[str, str]] = [
        {
            "column": "variable_name",
            "domain_table": "variables",
            "domain_column": "name",
        },
        {
            "column": "unit_name",
            "domain_table": "units",
            "domain_column": "name",
        },
        {
            "column": "configuration_name",
            "domain_table": "configurations",
            "domain_column": "name",
        },
        {
            "column": "location_id",
            "domain_table": "locations",
            "domain_column": "id",
        }
    ]
    schema_func = staticmethod(primary_timeseries_schema)

    def __init__(
        self,
        ev,
        table_name: str = "primary_timeseries",
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
            The name of the table to operate on. Defaults to 'primary_timeseries'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)
