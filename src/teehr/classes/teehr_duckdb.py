"""Defines the TEEHR dataset class and pre-processing methods."""
from typing import Union, List, Callable, Dict, Any
from pathlib import Path
import logging
from abc import ABC, abstractmethod

import re
import duckdb
import pandas as pd
import geopandas as gpd

import teehr.queries.duckdb_database as tqu_db

import teehr.queries.utils as tqu
import teehr.models.queries_database as tmqd
from teehr.models.queries import MetricEnum

logger = logging.getLogger(__name__)


class DuckDBBase(ABC):
    """Test base class.

    This could contain:

    - __init__(): @abstractmethod
    - query(): Use the API method, assuming the connection is a class attribute
    - profile_query()
    - get_joined_timeseries_schema() --> not for un-joined parquet files
    - _sanitize_field_name()
    - describe_inputs()
    - _check_if_geometry_is_inserted() --> not for un-joined parquet files

    """

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
        query = "INSTALL https;"
        self.query(query)
        query = "LOAD https;"
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
        * Results bprinted to the screen (format='raw')
        * A DuckDBPyRelation, a symbolic representation of the SQL query
          (format='relation').
        """
        if format == "df":
            return self.con.sql(query).df()
        elif format == "raw":
            return self.con.sql(query).show()
        elif format == "relation":
            return self.con.sql(query)
        self.con.sql(query)
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
            self._check_if_geometry_is_inserted()
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
        primary_filepath: Union[str, Path],
        secondary_filepath: Union[str, Path],
    ) -> pd.DataFrame:
        """Get descriptive statistics on the primary and secondary
        timeseries by reading the parquet files.

        Parameters
        ----------
        primary_filepath : Union[str, Path]
            Path to the primary time series parquet file.
        secondary_filepath : Union[str, Path]
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

    # @abstractmethod
    # def _get_joined_timeseries_clause(self, qm: Any) -> str:
    #     """Get the initial joined_timeseries clause."""
    #     pass

    # def _get_metrics_calculation_clause(self, qm: Any) -> str:
    #     """Get the metrics calculation clause."""
    #     return tqu.metrics_calculation_clause(qm)

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
        # query = self._get_joined_timeseries_clause(mq) + \
        #     self._get_metrics_calculation_clause(mq)
        query = tqu_db.create_get_metrics_query(
            mq,
            self.from_joined_timeseries_clause
        )

        return self._execute_query(
            query,
            mq.return_query,
            mq.include_geometry
        )

    def _get_joined_timeseries(
        self,
        jtq: Any
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
            self.from_joined_timeseries_clause
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

        return self._execute_query(query, tcq.return_query)

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

        return self._execute_query(query, tq.return_query)

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


class DuckDBAPI(DuckDBBase):
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
        self.from_joined_timeseries_clause = "FROM joined_timeseries sf"
        self.con = duckdb.connect(self.database_filepath, read_only=True)

    def _get_intial_joined_timeseries_clause(self, qm: Any) -> str:
        """Get the initial joined_timeseries clause."""
        return f"""
            WITH joined as (
                SELECT
                    *
                FROM joined_timeseries sf
                {tqu.filters_to_sql(qm.filters)}
            )
        """

    def _check_if_geometry_is_inserted(self):
        """Make sure the geometry table is not empty."""
        df = self.query("SELECT COUNT(geometry) FROM geometry;", format="df")
        if df["count(geometry)"].values == 0:
            raise ValueError(
                "The geometry table is empty! Please insert geometry first"
            )

    def get_joined_timeseries_schema(self) -> pd.DataFrame:
        """Get field names and field data types from the joined_timeseries \
        table.

        Returns
        -------
        pd.DataFrame
            Includes column_name, column_type, null, key, default,
            and extra columns.
        """
        desc = """DESCRIBE SELECT * FROM joined_timeseries;"""
        joined_df = self.query(desc, format="df")

        return joined_df

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


class DuckDBDatabase(DuckDBAPI):
    """Extends TEEHRDatasetAPI class with additional functionality \
    for local database-based queries.

    NOTE: Inheriting from TEEHRDatasetAPI provides access to
    _check_if_geometry_is_inserted() and get_joined_timeseries_schema().

    Methods
    -------
    insert_geometry(geometry_filepath: Union[str, Path])
        Inserts geometry from a parquet file into a separate
        database table named 'geometry'.

    insert_joined_timeseries(primary_filepath: Union[str, Path], \
        secondary_filepath: Union[str, Path], \
        crosswalk_filepath: Union[str, Path], \
        order_by: List[str] = [ \
            "reference_time", \
            "primary_location_id", \
        ], \
        drop_added_fields=False, \
    )
        Joins the primary and secondary timeseries from parquet files
        and inserts into the database as the joined_timeseries table.

    join_attributes(attributes_filepath: Union[str, Path])
        Joins attributes from the provided attribute table(s) to new
        fields in the joined_timeseries table

    calculate_field(new_field_name: str, new_field_type: str, \
        parameter_names: List[str], \
        user_defined_function: Callable, \
        replace: bool = True, \
    )
        Calculate a new field in joined_timeseries based on existing
        fields and a user-defined function.

    profile_query(query: str)
        A helper function to profile query performance (runs EXPLAIN ANALYZE).
        Inherited from TEEHRDatasetAPI.

    query(query: str, format: str = None, create_function_args: Dict = None)
        Submit an SQL query string against the database.
        Inherited from TEEHRDatasetAPI.

    get_joined_timeseries_schema() -> pd.DataFrame
        Get field names and field data types from the joined_timeseries,
        table as a pandas dataframe. Inherited from TEEHRDatasetAPI.

    describe_inputs( \
        primary_filepath: Union[str, Path], \
        secondary_filepath: Union[str, Path] \
    ) -> pd.DataFrame
        Get descriptive statistics on the primary and secondary
        timeseries by reading the parquet files as a pandas dataframe.
        Inherited from TEEHRDatasetAPI.

    get_metrics( \
        group_by: List[str], \
        order_by: List[str], \
        include_metrics: Union[List[MetricEnum], "all"], \
        filters: Union[List[dict], None] = None, \
        include_geometry: bool = False, \
        return_query: bool = False, \
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        Calculate performance metrics using database queries
        Overrides TEEHRDatasetAPI.

    get_timeseries( \
        order_by: List[str], \
        timeseries_name: str, \
        filters: Union[List[dict], None] = None, \
        return_query: bool = False, \
    ) -> Union[pd.DataFrame, str] \
        Retrieve timeseries using a database query.
        Overrides TEEHRDatasetAPI.

    get_timeseries_chars( \
        group_by: List[str], \
        order_by: List[str], \
        timeseries_name: str, \
        filters: Union[List[dict], None] = None, \
        return_query: bool = False, \
    ) -> Union[str, pd.DataFrame]
        Retrieve timeseries characteristics using database query
        Overrides TEEHRDatasetAPI.

    get_unique_field_values(fn: JoinedTimeseriesFieldName) -> pd.DataFrame
        Get unique values for a given field as a pandas dataframe.
        Overrides TEEHRDatasetAPI.
        Use get_joined_timeseries_schema() to see existing table fields.
    """

    def __init__(
        self,
        database_filepath: Union[str, Path],
    ):
        """Initialize a study area database. Create the joined_timeseries
        and geometry tables with a fixed schemas if they do not already
        exist.

        Parameters
        ----------
        database_filepath : Union[str, Path]
            Filepath to the database.
        """
        self.database_filepath = str(database_filepath)
        self.from_joined_timeseries_clause = "FROM joined_timeseries sf"
        self._initialize_database_tables()

    # def _get_intial_joined_timeseries_clause(self, qm: Any) -> str:
    #     """Get the initial joined_timeseries clause."""
    #     return f"""
    #         WITH joined as (
    #             SELECT
    #                 *
    #             FROM joined_timeseries sf
    #             {tqu.filters_to_sql(qm.filters)}
    #         )
    #     """

    def query(
        self,
        query: str,
        read_only: bool = False,
        format: str = None,
        create_function_args: Dict = None
    ):
        """Run query against the class's database."""
        if not create_function_args:
            with duckdb.connect(
                self.database_filepath, read_only=read_only
            ) as con:
                if format == "df":
                    return con.sql(query).df()
                elif format == "raw":
                    return con.sql(query).show()
                elif format == "relation":
                    return con.sql(query)
                con.sql(query)
                con.close()
                return None
        else:
            user_defined_function = create_function_args["function"]
            function_name = create_function_args["function_name"]
            parameter_types = create_function_args["parameter_types"]
            new_field_type = create_function_args["new_field_type"]
            with duckdb.connect(self.database_filepath) as con:
                # Register the function
                con.create_function(
                    function_name,
                    user_defined_function,
                    parameter_types,
                    new_field_type,
                )
                # Call the function and add the results to joined_timeseries
                con.sql(query)
                con.close()

    def _execute_query(
        self,
        query: str,
        read_only: bool = False,
        return_query: bool = False,
        include_geometry: bool = False
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Execute a query and return the results or the query itself."""
        if return_query:
            return tqu.remove_empty_lines(query)
        elif include_geometry:
            self._check_if_geometry_is_inserted()
            df = self.query(query, read_only=read_only, format="df")
            return tqu.df_to_gdf(df)
        else:
            df = self.query(query, read_only=read_only, format="df")
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
            self.from_joined_timeseries_clause
        )

        return self._execute_query(
            query=query,
            read_only=True,
            return_query=mq.return_query,
            include_geometry=mq.include_geometry
        )

    def _get_joined_timeseries(
        self,
        jtq: Any
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
            self.from_joined_timeseries_clause
        )

        return self._execute_query(
            query=query,
            read_only=True,
            return_query=jtq.return_query,
            include_geometry=jtq.include_geometry
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
            read_only=True,
            return_query=tcq.return_query,
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
            read_only=True,
            return_query=tq.return_query,
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
        df = self.query(query, read_only=True, format="df")
        return df

    def _initialize_database_tables(self):
        """Create the persistent study database and empty table(s)."""
        create_timeseries_table = """
            CREATE TABLE IF NOT EXISTS joined_timeseries(
                reference_time DATETIME,
                value_time DATETIME,
                secondary_location_id VARCHAR,
                secondary_value FLOAT,
                configuration VARCHAR,
                measurement_unit VARCHAR,
                variable_name VARCHAR,
                primary_value FLOAT,
                primary_location_id VARCHAR,
                lead_time INTERVAL,
                absolute_difference FLOAT
                );"""

        self.query(create_timeseries_table)

        # Also initialize the geometry table (what if multiple geometry types?)
        create_geometry_table = """
            CREATE TABLE IF NOT EXISTS geometry(
                id VARCHAR,
                name VARCHAR,
                geometry BLOB
                );"""
        self.query(create_geometry_table)

    def _drop_joined_timeseries_field(self, field_name: str):
        """Drop the specified field by name from joined_timeseries table."""
        query = f"""
            ALTER TABLE joined_timeseries
            DROP COLUMN {field_name}
        ;"""
        self.query(query)

    def _validate_joined_timeseries_base_fields(self, drop_added_fields: bool):
        """Ensure that no user-defined fields have been added or base fields
        have been dropped. This is necessary in order to add multiple
        configurations into the joined_timeseries table.
        """
        schema_df = self.get_joined_timeseries_schema()
        if schema_df.index.size < len(tmqd.JoinedFieldNameEnum) - 1:
            raise ValueError(
                "There are missing fields in the joined_timeseries schema"
            )
        for field_name in schema_df.column_name.tolist():
            if field_name not in tmqd.JoinedFieldNameEnum.__members__:
                if drop_added_fields:
                    logger.info(f"Dropping added field {field_name}")
                    self._drop_joined_timeseries_field(field_name)
                else:
                    raise ValueError(
                        f"An added field '{field_name}' exists,"
                        "please drop it before joining timeseries"
                    )

    def insert_geometry(self, geometry_filepath: Union[str, Path]):
        """Insert geometry from a parquet file into a table named 'geometry'.

        Parameters
        ----------
        geometry_filepath : Union[str, Path]
            Path to the geometry file.
        """
        # Load the geometry data into a separate table
        if geometry_filepath:
            query = f"""
                INSERT INTO
                    geometry
                SELECT
                    pq.id, pq.name, pq.geometry
                FROM
                    read_parquet('{str(geometry_filepath)}') pq
                WHERE NOT EXISTS (
                    SELECT
                        id, name, geometry
                    FROM
                        geometry
                    WHERE
                        pq.id = id AND pq.name = name
                    AND pq.geometry = geometry
                )
            ;"""
            self.query(query)

    def insert_joined_timeseries(
        self,
        primary_filepath: Union[str, Path],
        secondary_filepath: Union[str, Path],
        crosswalk_filepath: Union[str, Path],
        order_by: List[str] = [
            "primary_location_id",
            "configuration",
            "variable_name",
            "measurement_unit",
            "value_time"
        ],
        drop_added_fields=False,
    ):
        r"""Join the primary and secondary timeseries from parquet files.

        The joined results are insert into the database as a table
        named 'joined_timeseries'.

        Parameters
        ----------
        primary_filepath : Union[str, Path]
            File path to the "observed" data.  String must include path to
            file(s) and can include wildcards.  For example,
            "/path/to/parquet/\\*.parquet".
        secondary_filepath : Union[str, Path]
            File path to the "forecast" data.  String must include path to
            file(s) and can include wildcards.  For example,
            "/path/to/parquet/\\*.parquet".
        crosswalk_filepath : Union[str, Path]
            File path to single crosswalk file.
        order_by : List[str], optional
            List of column/field names to order results by,
            by default ["reference_time", "primary_location_id"].
        drop_added_fields : bool, optional
            A flag to determine whether to drop any user-defined fields that
            have been added to the table (True), or raise an error if added
            fields exist (False). By default False.

        See Also
        --------
        teehr.queries.duckdb_database.create_join_and_save_timeseries_query : \
            Create the join and save timeseries query.
        """
        self._validate_joined_timeseries_base_fields(drop_added_fields)

        jtq = tmqd.InsertJoinedTimeseriesQuery.model_validate(
            {
                "primary_filepath": primary_filepath,
                "secondary_filepath": secondary_filepath,
                "crosswalk_filepath": crosswalk_filepath,
                "order_by": order_by,
            }
        )

        query = tqu_db.create_join_and_save_timeseries_query(jtq)
        self.query(query)

    def _get_unique_attributes(self, attributes_filepath: str) -> List:
        """Get a list of unique attributes and attribute units from the
        provided attribute table(s).
        """
        query = f"""
            SELECT
                DISTINCT attribute_name, attribute_unit
            FROM
                read_parquet('{attributes_filepath}')
        ;"""
        attr_list = self.query(query, format="df").to_dict(orient="records")
        return attr_list

    def _add_field_name_to_joined_timeseries(
        self, field_name: str, field_dtype="VARCHAR"
    ):
        """Add a field name to joined_timeseries
        if it does not already exist.
        """
        query = f"""
            ALTER TABLE
                joined_timeseries
            ADD IF NOT EXISTS
                {field_name} {field_dtype}
        ;"""
        self.query(query)

    def join_attributes(self, attributes_filepath: Union[str, Path]):
        """Join attributes from the provided attribute table(s) to new
        fields in the joined_timeseries table.

        Parameters
        ----------
        attributes_filepath : Union[str, Path]
            File path to the "attributes" data.  String must include path to
            file(s) and can include wildcards.  For example,
            "/path/to/parquet/\\*.parquet".
        """
        attr_list = self._get_unique_attributes(str(attributes_filepath))

        for attr in attr_list:

            if attr["attribute_unit"]:
                field_name = (
                    f"{attr['attribute_name']}_{attr['attribute_unit']}"
                )
                unit_clause = (
                    f"AND attribute_unit = '{attr['attribute_unit']}'"
                )
            else:
                field_name = attr["attribute_name"]
                unit_clause = ""

            field_name = self._sanitize_field_name(field_name)

            self._add_field_name_to_joined_timeseries(field_name)

            query = f"""
                WITH selected_attribute AS (
                    SELECT
                        *
                    FROM
                        read_parquet('{attributes_filepath}')
                    WHERE
                        attribute_name = '{attr['attribute_name']}'
                    {unit_clause}
                )
                UPDATE
                    joined_timeseries
                SET
                    {field_name} = (
                        SELECT
                            CAST(attribute_value AS VARCHAR)
                        FROM
                            selected_attribute
                        WHERE
                            joined_timeseries.primary_location_id =
                                selected_attribute.location_id
                    )
            ;"""

            self.query(query)

    def calculate_field(
        self,
        new_field_name: str,
        new_field_type: str,
        parameter_names: List[str],
        user_defined_function: Callable,
        replace: bool = True,
    ):
        """Calculate a new field in joined_timeseries based on existing
        fields and a user-defined function.

        Parameters
        ----------
        new_field_name : str
            Name of new field to be added to joined_timeseries.
        new_field_type : str
            Data type of the new field.
        parameter_names : List[str]
            Arguments to your user function,
            must be exisiting joined_timeseries fields.
        user_defined_function : Callable
            Function to apply.
        replace : bool
            If replace is True and new_field_name already exists, it is
            dropped before being recalculated and re-added.
        """
        schema_df = self.get_joined_timeseries_schema()

        sanitized_field_name = self._sanitize_field_name(new_field_name)

        cf = tmqd.CalculateField.model_validate(
            {
                "new_field_name": sanitized_field_name,
                "new_field_type": new_field_type,
                "parameter_names": parameter_names,
            },
            context={"existing_fields": schema_df.column_name.tolist()},
        )

        # Get data types of function parameters
        schema_dict = dict(zip(schema_df.column_name, schema_df.column_type))
        parameter_types = [schema_dict[param] for param in parameter_names]

        if replace and (
            sanitized_field_name in schema_df.column_name.tolist()
        ):
            self._drop_joined_timeseries_field(new_field_name)

        self._add_field_name_to_joined_timeseries(
            field_name=sanitized_field_name, field_dtype=new_field_type
        )

        # Build the query using the parameter names
        args = ""
        for name in cf.parameter_names:
            args += f"{name}, "
        args = args[:-2]

        query = f"""
            UPDATE
                joined_timeseries
            SET
                {cf.new_field_name} = (
            SELECT
                user_defined_function({args})
            );
            """

        create_function_args = {
            "function": user_defined_function,
            "function_name": "user_defined_function",
            "parameter_types": parameter_types,
            "new_field_type": cf.new_field_type,
        }

        self.query(query=query, create_function_args=create_function_args)

    def _validate_query_model(self, query_model: Any, data: Dict) -> Any:
        """Validate the query based on existing table fields."""
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            data,
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model

    def get_metrics(
        self,
        group_by: List[str],
        order_by: List[str],
        include_metrics: Union[List[MetricEnum], str] = "all",
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Calculate performance metrics using database queries.

        Parameters
        ----------
        group_by : List[str]
            List of column/field names to group timeseries data by.
            Must provide at least one.
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        include_metrics : Union[List[MetricEnum], str]
            List of metrics (see below) for allowable list, or "all" to return
            all. Placeholder, currently ignored -> returns "all".
        filters : Union[List[dict], None] = None
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics.
        include_geometry : bool, optional
            True joins the geometry to the query results.
            Only works if `primary_location_id`
            is included as a group_by field, by default False.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

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
        data = {
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": include_metrics,
            "filters": filters,
            "include_geometry": include_geometry,
            "return_query": return_query,
        }
        mq = self._validate_query_model(tmqd.MetricQuery, data)

        return self._get_metrics(mq)

    def get_joined_timeseries(
        self,
        order_by: List[str],
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Retrieve joined timeseries using database query.

        Parameters
        ----------
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        filters : Union[List[dict], None] = None
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics.
        include_geometry : bool
            True joins the geometry to the query results.
            Only works if `primary_location_id`
            is included as a group_by field.
        return_query : bool = False
            True returns the query string instead of the data.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame str]
            A DataFrame or GeoDataFrame of query results
            or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_joined_timeseries_query : \
            Create the get joined timeseries query.
        """
        data = {
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
        }
        jtq = self._validate_query_model(tmqd.JoinedTimeseriesQuery, data)

        return self._get_joined_timeseries(jtq)

    def get_timeseries(
        self,
        order_by: List[str],
        timeseries_name: str,
        filters: Union[List[dict], None] = None,
        return_query: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Retrieve timeseries using database query.

        Parameters
        ----------
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        timeseries_name : str
            Name of the time series to query ('primary' or 'secondary').
        filters : Union[List[dict], None], optional
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics, by default None.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

        Returns
        -------
        Union[pd.DataFrame, str]
            A DataFrame of query results or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_timeseries_query : \
            Create the get timeseries query.
        """
        data = {
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "timeseries_name": timeseries_name,
        }
        tq = self._validate_query_model(tmqd.TimeseriesQuery, data)

        return self._get_timeseries(tq)

    def get_timeseries_chars(
        self,
        group_by: List[str],
        order_by: List[str],
        timeseries_name: str,
        filters: Union[List[dict], None] = None,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame]:
        """Retrieve timeseries characteristics using database query.

        Parameters
        ----------
        group_by : List[str]
            List of column/field names to group timeseries data by.
            Must provide at least one.
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        timeseries_name : str
            Name of the time series to query (primary or secondary).
        filters : Union[List[dict], None], optional
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics., by default None.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

        Returns
        -------
        Union[str, pd.DataFrame]
            A DataFrame of time series characteristics including:
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
        data = {
            "order_by": order_by,
            "group_by": group_by,
            "timeseries_name": timeseries_name,
            "filters": filters,
            "return_query": return_query,
        }
        tcq = self._validate_query_model(tmqd.TimeseriesCharQuery, data)

        return self._get_timeseries_chars(tcq)

    def get_unique_field_values(self, field_name: str) -> pd.DataFrame:
        """Get unique values for a given field.

        Parameters
        ----------
        field_name : str
            Name of the joined_timeseries field.

        Returns
        -------
        pd.DataFrame
            A dataframe containing unique values for the given field.

        See Also
        --------
        teehr.queries.duckdb_database.create_unique_field_values_query : \
            Create the get unique field values query.
        """
        data = {"field_name": field_name}
        fn = self._validate_query_model(tmqd.JoinedTimeseriesFieldName, data)

        return self._get_unique_field_values(fn)


class DuckDBJoinedParquet(DuckDBBase):
    """For querying joined parquet files."""

    def __init__(
        self,
        joined_parquet_filepath: Union[str, Path],
    ):
        r"""Set the path to the pre-existing study area database.

        Establish a read-only in-memory connection.

        Parameters
        ----------
        joined_parquet_filepath : Union[str, Path]
            File path to the "observed" data.  String must include path
            to file(s) and can include wildcards.
            For example, "/path/to/parquet/\\*.parquet".
        """
        self.joined_parquet_filepath = str(joined_parquet_filepath)
        # NOTE: Cannot launch a read-only in-memory connection
        self.con = duckdb.connect()
        self.from_joined_timeseries_clause = (
            f"FROM read_parquet('{str(joined_parquet_filepath)}') sf"
        )

    # def _get_joined_timeseries_clause(self, qm: Any) -> str:
    #     """Get the joined_timeseries clause."""
    #     return f"""
    #         WITH joined as (
    #             SELECT
    #                 *
    #             FROM read_parquet('{str(self.joined_parquet_filepath)}') sf
    #             {tqu.filters_to_sql(qm.filters)}
    #         )
    #     """

    def get_joined_timeseries_schema(self) -> pd.DataFrame:
        """Get field names and field data types from the joined \
        parquet files.

        Returns
        -------
        pd.DataFrame
            Includes column_name, column_type, null, key, default,
            and extra columns.
        """
        desc = f"""
        DESCRIBE
        SELECT
            *
        FROM
            read_parquet('{str(self.joined_parquet_filepath)}')
        ;"""
        joined_df = self.query(desc, format="df")

        return joined_df

    def _validate_query_model(self, query_model: Any, data: Dict) -> Any:
        """Validate the query based on existing fields."""
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            data,
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model

    def get_metrics(
        self,
        group_by: List[str],
        order_by: List[str],
        include_metrics: Union[List[MetricEnum], str] = "all",
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Calculate performance metrics using database queries.

        Parameters
        ----------
        group_by : List[str]
            List of column/field names to group timeseries data by.
            Must provide at least one.
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        include_metrics : Union[List[MetricEnum], str]
            List of metrics (see below) for allowable list, or "all" to return
            all. Placeholder, currently ignored -> returns "all".
        filters : Union[List[dict], None] = None
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics.
        include_geometry : bool, optional
            True joins the geometry to the query results.
            Only works if `primary_location_id`
            is included as a group_by field, by default False.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

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
        data = {
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": include_metrics,
            "filters": filters,
            "include_geometry": include_geometry,
            "return_query": return_query,
        }
        mq = self._validate_query_model(tmqd.MetricQuery, data)

        return self._get_metrics(mq)

    def get_joined_timeseries(
        self,
        order_by: List[str],
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame, str]:
        """Retrieve joined timeseries from the joined parquet file(s).

        Parameters
        ----------
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        filters : Union[List[dict], None] = None
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics.
        include_geometry : bool
            True joins the geometry to the query results.
            Only works if `primary_location_id`
            is included as a group_by field.
        return_query : bool = False
            True returns the query string instead of the data.

        Returns
        -------
        Union[pd.DataFrame, gpd.GeoDataFrame str]
            A DataFrame or GeoDataFrame of query results
            or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_joined_timeseries_query : \
            Create the get joined timeseries query.
        """
        data = {
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
        }
        jtq = self._validate_query_model(tmqd.JoinedTimeseriesQuery, data)

        return self._get_joined_timeseries(jtq)

    def get_timeseries(
        self,
        order_by: List[str],
        timeseries_name: str,
        filters: Union[List[dict], None] = None,
        return_query: bool = False,
    ) -> Union[pd.DataFrame, str]:
        """Retrieve timeseries using database query.

        Parameters
        ----------
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        timeseries_name : str
            Name of the time series to query ('primary' or 'secondary').
        filters : Union[List[dict], None], optional
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics, by default None.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

        Returns
        -------
        Union[pd.DataFrame, str]
            A DataFrame of query results or the query itself as a string.

        See Also
        --------
        teehr.queries.duckdb_database.create_get_timeseries_query : \
            Create the get timeseries query.
        """
        data = {
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "timeseries_name": timeseries_name,
        }
        tq = self._validate_query_model(tmqd.TimeseriesQuery, data)

        return self._get_timeseries(tq)

    def get_timeseries_chars(
        self,
        group_by: List[str],
        order_by: List[str],
        timeseries_name: str,
        filters: Union[List[dict], None] = None,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame]:
        """Retrieve timeseries characteristics using database query.

        Parameters
        ----------
        group_by : List[str]
            List of column/field names to group timeseries data by.
            Must provide at least one.
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        timeseries_name : str
            Name of the time series to query (primary or secondary).
        filters : Union[List[dict], None], optional
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics., by default None.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

        Returns
        -------
        Union[str, pd.DataFrame]
            A DataFrame of time series characteristics including:
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
        data = {
            "order_by": order_by,
            "group_by": group_by,
            "timeseries_name": timeseries_name,
            "filters": filters,
            "return_query": return_query,
        }
        tcq = self._validate_query_model(tmqd.TimeseriesCharQuery, data)

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


class DuckDBParquet(DuckDBBase):
    """For querying joined parquet files."""

    def __init__(
        self,
        primary_filepath: Union[str, Path],
        secondary_filepath: Union[str, Path],
        crosswalk_filepath: Union[str, Path],
        geometry_filepath: Union[str, Path, None],
    ):
        r"""Set the path to the pre-existing study area database.

        Establish a read-only in-memory connection.

        Parameters
        ----------
        joined_parquet_filepath : Union[str, Path]
            File path to the "observed" data.  String must include path
            to file(s) and can include wildcards.
            For example, "/path/to/parquet/\\*.parquet".
        """

        self.primary_filepath = str(primary_filepath)
        self.secondary_filepath = str(secondary_filepath)
        self.crosswalk_filepath = str(crosswalk_filepath)
        if geometry_filepath:
            self.geometry_filepath = str(geometry_filepath)

        # NOTE: Cannot launch a read-only in-memory connection
        self.con = duckdb.connect()

        # self.from_joined_timeseries_clause = (
        #     f"FROM read_parquet('{str(joined_parquet_filepath)}') sf"
        # )

    def get_metrics(
        self,
        group_by: List[str],
        order_by: List[str],
        include_metrics: Union[List[MetricEnum], str] = "all",
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Calculate performance metrics using database queries.

        Parameters
        ----------
        group_by : List[str]
            List of column/field names to group timeseries data by.
            Must provide at least one.
        order_by : List[str]
            List of column/field names to order results by.
            Must provide at least one.
        include_metrics : Union[List[MetricEnum], str]
            List of metrics (see below) for allowable list, or "all" to return
            all. Placeholder, currently ignored -> returns "all".
        filters : Union[List[dict], None] = None
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics.
        include_geometry : bool, optional
            True joins the geometry to the query results.
            Only works if `primary_location_id`
            is included as a group_by field, by default False.
        return_query : bool, optional
            True returns the query string instead of the data,
            by default False.

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
        data = {
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": include_metrics,
            "filters": filters,
            "include_geometry": include_geometry,
            "return_query": return_query,
        }
        # mq = self._validate_query_model(tmqd.MetricQuery, data)
        mq = tmqd.MetricQuery.model_validate(data)

        return self._get_metrics(mq)
