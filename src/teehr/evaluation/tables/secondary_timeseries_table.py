"""Secondary timeseries table class."""
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.models.pandera_dataframe_schemas import secondary_timeseries_schema
from typing import List, Dict, Union
import logging
from teehr.querying.utils import df_to_gdf

logger = logging.getLogger(__name__)


class SecondaryTimeseriesTable(TimeseriesTable):
    """Access methods to secondary timeseries table."""

    # Table metadata
    table_name = "secondary_timeseries"
    uniqueness_fields = [
        "location_id",
        "value_time",
        "reference_time",
        "variable_name",
        "unit_name",
        "configuration_name",
        "member"
    ]
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
            "domain_table": "location_crosswalks",
            "domain_column": "secondary_location_id",
        }
    ]
    schema_func = staticmethod(secondary_timeseries_schema)

    def __init__(
        self,
        ev,
        table_name: str = "secondary_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on. Defaults to 'secondary_timeseries'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)

    def add_geometry(self):
        """Join geometry via the crosswalk."""
        logger.debug("Joining locations geometry via the crosswalk.")
        catalog_name = self._ev.active_catalog.catalog_name
        namespace_name = self._ev.active_catalog.namespace_name
        sql = f"""
            SELECT
                sf.*,
                lf.geometry as geometry
            FROM {catalog_name}.{namespace_name}.secondary_timeseries sf
            JOIN {catalog_name}.{namespace_name}.location_crosswalks cf
                on cf.secondary_location_id = sf.location_id
            JOIN {catalog_name}.{namespace_name}.locations lf
                on cf.primary_location_id = lf.id
        """
        gdf = self._ev.spark.sql(sql)
        self._sdf = gdf
        self._has_geometry = True
        return self
