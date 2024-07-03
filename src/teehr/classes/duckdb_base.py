"""Base class for DuckBD queries."""
import teehr.queries.duckdb_database as tqu_db
import teehr.queries.utils as tqu

from teehr.classes.dataframe_accessor import TEEHRDataFrameAccessor # noqa
from teehr.classes.series_accessor import TEEHRSeriesAccessor # noqa

import geopandas as gpd
import pandas as pd

import re
import duckdb
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Union


class DuckDBBase(ABC):
    """The TEEHR-DuckDB base class."""

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the class."""
        pass

    # @abstractmethod  # Not needed in DuckDBParquet
    def _validate_query_model(self) -> Any:
        """Validate the query model."""
        pass

    def get_joined_timeseries_schema(self) -> pd.DataFrame:
        """Get the schema from the joined_timeseries table or file(s)."""
        pass

    def load_https_extension(self):
        """Load the HTTPFS extension for DuckDB."""
        query = """
            INSTALL https;
            LOAD https;
        """
        self.query(query)

    def profile_query(self, query: str):
        """Profile query performance.

        EXPLAIN ANALYZE is pre-pended to the query and
        results are printed to the screen.
        """
        query = "EXPLAIN ANALYZE " + query
        print(self.query(query, format="df").explain_value.values[0])

    def query(
        self,
        query: str,
        format: str = None,
    ):
        """Run an SQL query against the class's database.

        Return formats include:
        * A pandas dataframe (format='df')
        * Results printed to the screen (format='raw')
        """
        with duckdb.connect() as con:
            resp = con.sql(query)
            if format == "df":
                return resp.df()
            elif format == "raw":
                return resp.show()
            return None

    def _execute_query(
        self,
        query: str,
        return_query: bool = False,
        include_geometry: bool = False,
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Execute a query and return the results or the query itself."""
        if return_query:
            return tqu.remove_empty_lines(query)
        elif include_geometry:
            self._check_geometry_available()
            df = self.query(query, format="df")
            return tqu.df_to_gdf(df)
        else:
            df = self.query(query, format="df")
        return df

    @staticmethod
    def _sanitize_field_name(field_name: str) -> str:
        """Replace unallowed characters from user-defined field names with an
        underscore.
        """
        # I think we will need this
        allowed_chars = r"[^a-zA-Z0-9_]"
        search = re.compile(allowed_chars).search

        if bool(search(field_name)):
            sub = re.compile(allowed_chars).sub
            return str(sub("_", field_name))

        return field_name

    @staticmethod
    def describe_inputs(
        primary_filepath: Union[str, Path, List[Union[str, Path]]],
        secondary_filepath: Union[str, Path, List[Union[str, Path]]]
    ) -> pd.DataFrame:
        """Get descriptive statistics on the primary and secondary
        timeseries by reading the parquet files.

        Parameters
        ----------
        primary_filepath : Union[str, Path, List[Union[str, Path]]]
            Path to the primary time series parquet file.
        secondary_filepath : Union[str, Path, List[Union[str, Path]]]
            Path to the primary time series parquet file.

        Returns
        -------
        pd.DataFrame
            A dataframe of descriptive statistics for each time series.

            Output includes:

            * Number of unique location IDs
            * Total number of rows
            * Start date
            * End date
            * Number of duplicate rows
            * Number of location IDs with duplicate value times
            * Number of location IDs with missing time steps
        """
        primary_dict = tqu_db.describe_timeseries(
            timeseries_filepath=primary_filepath
        )

        secondary_dict = tqu_db.describe_timeseries(
            timeseries_filepath=secondary_filepath
        )

        df = pd.DataFrame(
            {
                "primary": [primary_dict[key] for key in primary_dict.keys()],
                "secondary": [
                    secondary_dict[key] for key in secondary_dict.keys()
                ],
            },
            index=primary_dict.keys(),
        )

        return df

    def _get_metrics(
        self,
        mq: Any
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Calculate performance metrics using database queries.

        Parameters
        ----------
        mq : Any
            Pydantic model containing query parameters.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame, str]
            A DataFrame or optionally a GeoDataFrame containing query results,
            or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_metrics_query : \
            Create the get metrics query.
        """
        query = tqu_db.create_get_metrics_query(
            mq,
            self.from_joined_timeseries_clause,
            self.join_geometry_clause
        )

        return self._execute_query(
            query,
            mq.return_query,
            mq.include_geometry
        )

    def _get_joined_timeseries(
        self,
        jtq: Any,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Retrieve joined timeseries using database query.

        Parameters
        ----------
        jtq : Any
            Pydantic model containing query parameters.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame, str]
            A DataFrame or GeoDataFrame of query results
            or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_joined_timeseries_query : \
            Create the get joined timeseries query.
        """
        query = tqu_db.create_get_joined_timeseries_query(
            jtq,
            self.from_joined_timeseries_clause,
            self.join_geometry_clause
        )

        return self._execute_query(
            query,
            jtq.return_query,
            jtq.include_geometry
        )

    def _get_timeseries_chars(
        self,
        tcq: Any
    ) -> Union[str, pd.DataFrame]:
        """Retrieve timeseries characteristics using database query.

        Parameters
        ----------
        tcq : Any
            Pydantic model containing query parameters.

        Returns
        -------
        Union[str, pd.DataFrame]
            A DataFrame of time series characteristics including:

            - location_id
            - count
            - min
            - max
            - average
            - sum
            - variance

            or, the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_timeseries_char_query : \
            Create the get timeseries characteristics query.
        """
        query = tqu_db.create_get_timeseries_char_query(
            tcq,
            self.from_joined_timeseries_clause
        )

        return self._execute_query(
            query=query,
            return_query=tcq.return_query
        )

    def _get_timeseries(
        self,
        tq: Any
    ) -> Union[pd.DataFrame, str]:
        """Retrieve timeseries using database query.

        Parameters
        ----------
        tq : Any
            Pydantic model containing query parameters.

        Returns
        -------
        Union[pd.DataFrame, str]
            A DataFrame of query results or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_timeseries_query : \
            Create the get timeseries query.
        """
        query = tqu_db.create_get_timeseries_query(
            tq,
            self.from_joined_timeseries_clause
        )

        return self._execute_query(
            query=query,
            return_query=tq.return_query
        )

    def _get_unique_field_values(
        self,
        fn: Any
    ) -> pd.DataFrame:
        """Get unique values for a given field.

        Parameters
        ----------
        fn : Any
            Pydantic model containing the joined_timeseries table field name.

        Returns
        -------
        pd.DataFrame
            A dataframe containing unique values for the given field.

        See Also
        --------
        teehr.queries.duckdb_database.create_unique_field_values_query : \
            Create the get unique field values query.
        """
        query = tqu_db.create_unique_field_values_query(
            fn,
            self.from_joined_timeseries_clause
        )
        df = self.query(query, format="df")
        return df
