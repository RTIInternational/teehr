"""Class based queries based on duckdb database for API."""
import teehr.models.queries_database as tmqd
from teehr.classes.duckdb_base import DuckDBBase


import duckdb
import geopandas as gpd
import pandas as pd


from pathlib import Path
from typing import Any, Union


class DuckDBDatabaseAPI(DuckDBBase):
    """Create an instance of the API class.

    Methods
    -------
    __init__(database_filepath: Union[str, Path])
        Establish a connection to an existing study area database.

    profile_query(query: str)
        A helper function to profile query performance (runs EXPLAIN ANALYZE).

    query(query: str, format: str = None, create_function_args: Dict = None)
        Submit an SQL query string against the database.

    get_joined_timeseries_schema()
        Get field names and field data types from the joined_timeseries,
        table as a pandas dataframe.

    describe_inputs(primary_filepath: Union[str, Path], \
    secondary_filepath: Union[str, Path])
        Get descriptive statistics on the primary and secondary
        timeseries by reading the parquet files as a pandas dataframe.

    get_metrics(mq: MetricQuery)
        Calculate performance metrics using database queries.

    get_timeseries(tq: TimeseriesQuery)
        Retrieve timeseries using a database query.

    get_timeseries_chars(tcq: TimeseriesCharQuery)
        Retrieve timeseries characteristics using database query.

    get_unique_field_values(fn: JoinedTimeseriesFieldName)
        Get unique values for a given field as a pandas dataframe.
    """

    def __init__(
        self,
        database_filepath: Union[str, Path],
    ):
        """Set the path to the pre-existing study area database.

        Establish a read-only database connection.

        Parameters
        ----------
        database_filepath : Union[str, Path]
            Filepath to the database.
        """
        self.database_filepath = str(database_filepath)
        self.from_joined_timeseries_clause = """
            FROM joined_timeseries sf
        """
        self.join_geometry_clause = """
            JOIN geometry gf on primary_location_id = gf.id
        """
        self.read_only = True

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
        with duckdb.connect(
            database=self.database_filepath,
            read_only=self.read_only
        ) as con:
            resp = con.sql(query)
            if format == "df":
                return resp.df()
            elif format == "raw":
                return resp.show()
            return None

    def get_metrics(
        self,
        mq: tmqd.MetricQuery,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Calculate performance metrics using database queries.

        Parameters
        ----------
        mq : MetricQuery
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
        mq = self._validate_query_model(mq)

        return self._get_metrics(mq)

    def get_joined_timeseries(
        self,
        jtq: tmqd.JoinedTimeseriesQuery
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Retrieve joined timeseries using database query.

        Parameters
        ----------
        jtq : JoinedTimeseriesQuery
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
        jtq = self._validate_query_model(jtq)

        return self._get_joined_timeseries(jtq)

    def get_timeseries(
        self,
        tq: tmqd.TimeseriesQuery
    ) -> Union[pd.DataFrame, str]:
        """Retrieve timeseries using database query.

        Parameters
        ----------
        tq : TimeseriesQuery
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
        tq = self._validate_query_model(tq)

        return self._get_timeseries(tq)

    def get_timeseries_chars(
        self, tcq: tmqd.TimeseriesCharQuery
    ) -> Union[str, pd.DataFrame]:
        """Retrieve timeseries characteristics using database query.

        Parameters
        ----------
        tcq : TimeseriesCharQuery
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
        tcq = self._validate_query_model(tcq)

        return self._get_timeseries_chars(tcq)

    def get_unique_field_values(
        self,
        fn: tmqd.JoinedTimeseriesFieldName
    ) -> pd.DataFrame:
        """Get unique values for a given field.

        Parameters
        ----------
        fn : JoinedTimeseriesFieldName
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
        fn = self._validate_query_model(fn)

        return self._get_unique_field_values(fn)

    def get_joined_timeseries_schema(self) -> pd.DataFrame:
        """Get field names and field data types from the joined_timeseries \
        table.

        Returns
        -------
        pd.DataFrame
            Includes column_name, column_type, null, key, default,
            and extra columns.
        """
        qry = """DESCRIBE SELECT * FROM joined_timeseries;"""
        schema = self.query(qry, format="df")

        return schema

    # Private methods
    def _check_geometry_available(self):
        """Make sure the geometry table is not empty."""
        df = self.query("SELECT COUNT(geometry) FROM geometry;", format="df")
        if df["count(geometry)"].values == 0:
            raise ValueError(
                "The geometry table is empty! Please insert geometry first"
            )

    def _validate_query_model(self, query_model: Any) -> Any:
        """Validate pydantic query models based on the existing fields
        in the joined_timeseries table.
        """
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            query_model.model_dump(),
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model