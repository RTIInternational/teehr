"""Class based queries based on duckdb database."""
import teehr_v0_3.models.queries_database as tmqd
import teehr_v0_3.queries.duckdb_database as tqu_db
import teehr_v0_3.queries.utils as tqu
from teehr_v0_3.classes.duckdb_database_api import DuckDBDatabaseAPI
from teehr_v0_3.models.queries import MetricEnum

import duckdb
import geopandas as gpd
import pandas as pd

from pathlib import Path
from typing import Any, Callable, Dict, List, Union
import logging

logger = logging.getLogger(__name__)


class DuckDBDatabase(DuckDBDatabaseAPI):
    """Extends TEEHRDatasetAPI class with additional functionality \
    for local database-based queries.

    NOTE: Inheriting from TEEHRDatasetAPI provides access to
    _check_geometry_available() and get_joined_timeseries_schema().

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

    insert_attributes(attributes_filepath: Union[str, Path])
        Joins attributes from the provided attribute table(s) to new
        fields in the joined_timeseries table

    insert_calculated_field(new_field_name: str, new_field_type: str, \
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
        """Initialize a study area database.

        Create the joined_timeseries and geometry tables with a fixed schemas
        if they do not already exist.

        Parameters
        ----------
        database_filepath : Union[str, Path]
            Filepath to the database.
        """
        # Register the pandas DataFrame accessor class.
        from teehr_v0_3.classes.dataframe_accessor import TEEHRDataFrameAccessor # noqa

        self.database_filepath = str(database_filepath)
        self.from_joined_timeseries_clause = "FROM joined_timeseries sf"
        self.join_geometry_clause = """
            JOIN geometry gf on primary_location_id = gf.id
        """
        self.read_only = False
        self._initialize_database_tables()

    # PUBLIC WRITE METHODS
    def insert_geometry(
        self,
        geometry_filepath: Union[str, Path, List[Union[str, Path]]]
    ):
        """Insert geometry from a parquet file into a table named 'geometry'.

        Parameters
        ----------
        geometry_filepath : Union[str, Path, List[Union[str, Path]]]
            Path to the geometry file(s).
        """
        # Load the geometry data into a separate table
        if geometry_filepath:
            query = f"""
                INSERT INTO
                    geometry
                SELECT
                    pq.id, pq.name, pq.geometry
                FROM
                    read_parquet({tqu._format_filepath(geometry_filepath)}) pq
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
        primary_filepath: Union[str, Path, List[Union[str, Path]]],
        secondary_filepath: Union[str, Path, List[Union[str, Path]]],
        crosswalk_filepath: Union[str, Path, List[Union[str, Path]]],
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

    def insert_attributes(
            self,
            attributes_filepath: Union[str, Path, List[Union[str, Path]]]
        ):
        """Join attributes from the provided attribute table(s) to new
        fields in the joined_timeseries table.

        Parameters
        ----------
        attributes_filepath : Union[str, Path, List[Union[str, Path]]]
            File path to the "attributes" data.  String must include path to
            file(s) and can include wildcards.  For example,
            "/path/to/parquet/\\*.parquet".
        """
        attr_list = self._get_unique_attributes(attributes_filepath)

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
                        read_parquet({tqu._format_filepath(attributes_filepath)}, union_by_name=true)
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

    def insert_calculated_field(
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
                {cf.new_field_name}_udf({args})
            );
            """

        with duckdb.connect(
            database=self.database_filepath,
            read_only=self.read_only
        ) as con:
            con.create_function(
                f"{cf.new_field_name}_udf",
                user_defined_function,
                parameter_types,
                cf.new_field_type.value
            )
            con.sql(query)

    # PUBLIC READ METHODS
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
            all.
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

        # if mq.include_geometry:
        #     return self._get_metrics(mq, self.join_geometry_clause)

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

        # if jtq.include_geometry:
        #     return self._get_joined_timeseries(jtq, self.join_geometry_clause)

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

    # PRIVATE METHODS
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
                primary_location_id VARCHAR
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

    def _get_unique_attributes(
            self,
            attributes_filepath: Union[str, Path, List[Union[str, Path]]]
    ) -> List:
        """Get a list of unique attributes and attribute units from the
        provided attribute table(s).
        """
        query = f"""
            WITH attributes AS (
                SELECT
                    attribute_name::varchar as attribute_name
                    , attribute_unit::varchar as attribute_unit
                FROM
                    read_parquet(
                        {tqu._format_filepath(attributes_filepath)}
                        , union_by_name=true
                    )
            )
            SELECT
                DISTINCT attribute_name, attribute_unit
            FROM
                attributes
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

    def _validate_query_model(self, query_model: Any, data: Dict) -> Any:
        """Validate the query based on existing table fields."""
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            data,
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model
