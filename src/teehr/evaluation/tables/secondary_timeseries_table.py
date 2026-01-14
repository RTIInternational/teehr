"""Secondary timeseries table class."""
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from typing import Union
import logging

logger = logging.getLogger(__name__)


class SecondaryTimeseriesTable(TimeseriesTable):
    """Access methods to secondary timeseries table."""

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
        table_name: str = "secondary_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ) -> "TimeseriesTable":
        """Initialize the Table class for a specific table.

        Parameters
        ----------
        table_name : str
            The name of the table to operate on. Defaults to 'secondary_timeseries'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.

        Returns
        -------
        "TimeseriesTable"
            The initialized Table instance ready for operations.

        Note
        ----
        Creates an instance of a Table class with 'secondary_timeseries'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = self._join_geometry_using_crosswalk()
        gdf.attrs['table_type'] = self.table_name
        gdf.attrs['fields'] = self.fields()
        return gdf