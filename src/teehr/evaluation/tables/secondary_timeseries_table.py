"""Secondary timeseries table class."""
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from typing import Union
import logging

logger = logging.getLogger(__name__)


class SecondaryTimeseriesTable(TimeseriesTable):
    """Access methods to secondary timeseries table."""

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

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = self._join_geometry_using_crosswalk()
        gdf.attrs['table_type'] = self.table_name
        gdf.attrs['fields'] = self.to_sdf().columns
        return gdf