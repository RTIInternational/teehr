"""Class based queries based on joined parquet files."""
import teehr.models.queries_database as tmqd
import teehr.queries.utils as tqu
from teehr.classes.duckdb_base import DuckDBBase
from teehr.models.queries import MetricEnum


import duckdb
import geopandas as gpd
import pandas as pd


from pathlib import Path
from typing import Any, Dict, List, Union, Optional


class DuckDBJoinedParquet(DuckDBBase):
    """For querying joined parquet files."""

    def __init__(
        self,
        joined_parquet_filepath: Union[str, Path, List[Union[str, Path]]],
        geometry_filepath: Optional[
            Union[str, Path, List[Union[str, Path]]]
        ] = None,
    ):
        r"""Initialize the joined parquet database class.

        Parameters
        ----------
        joined_parquet_filepath : Union[str, Path, List[Union[str, Path]]]
            File path to the "joined parquet" data.  String must include path
            to file(s) and can include wildcards.
            For example, "/path/to/parquet/\\*.parquet".
        geometry_filepath : Union[str, Path, List[Union[str, Path]]]
            File path to the "joined parquet" data.  String must include path
            to file(s) and can include wildcards.
            For example, "/path/to/parquet/\\*.parquet".
        """
        self.joined_parquet_filepath = joined_parquet_filepath
        self.geometry_filepath = geometry_filepath
        self.from_joined_timeseries_clause = f"""
            FROM read_parquet(
                {tqu._format_filepath(self.joined_parquet_filepath)}
            ) sf
        """
        self.join_geometry_clause = f"""
            JOIN read_parquet(
                {tqu._format_filepath(self.geometry_filepath)}
            ) gf on primary_location_id = gf.id
        """

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
            all. Must provide at least one.
        filters : Union[List[dict], None] = None
            List of dictionaries describing the "where" clause to limit data
            that is included in metrics.
        include_geometry : bool, optional
            True joins the geometry to the query results. Only works if
            `primary_location_id` is included as a group_by field, and
            'geometry_filepath' was included in instantiation.
            Default is False.
        return_query : bool, optional
            When True, returns the query string instead of the data.
            Default is False.

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
        include_geometry : bool, optional
            True joins the geometry to the query results. Only works if
            `primary_location_id` is included as a group_by field, and
            'geometry_filepath' was included in instantiation.
            Default is False.
        return_query : bool, optional
            True returns the query string instead of the data.
            Default is False.

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
            True returns the query string instead of the data.
            Default is False.

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
            True returns the query string instead of the data.
            Default is False.

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
        """Get unique values in data for a given field.

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

    def get_joined_timeseries_schema(self) -> pd.DataFrame:
        """Get field names and field data types from the joined \
        parquet files.

        Returns
        -------
        pd.DataFrame
            Includes column_name, column_type, null, key, default,
            and extra columns.
        """
        qry = f"""
        DESCRIBE
        SELECT
            *
        FROM
            read_parquet({tqu._format_filepath(self.joined_parquet_filepath)})
        ;"""
        schema = self.query(qry, format="df")

        return schema

    # Private methods
    def _check_geometry_available(self):
        """Make sure the geometry filepath has been specified."""
        if not self.geometry_filepath:
            raise ValueError("Please specify a geometry file path.")
        df = self.query(
            f"""
                SELECT
                    COUNT(geometry)
                FROM read_parquet(
                    {tqu._format_filepath(self.geometry_filepath)}
                );
                """,
            format="df"
        )
        if df["count(geometry)"].values == 0:
            raise ValueError(
                "The geometry file is empty! Please specify a valid file."
            )

    def _validate_query_model(self, query_model: Any, data: Dict) -> Any:
        """Validate the query based on existing fields."""
        schema_df = self.get_joined_timeseries_schema()
        validated_model = query_model.model_validate(
            data,
            context={"existing_fields": schema_df.column_name.tolist()},
        )
        return validated_model