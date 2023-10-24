import duckdb

import pandas as pd
import geopandas as gpd

from typing import List, Union, Dict
from pathlib import Path

from teehr.models.queries import (
    MetricQueryDB,
    JoinedTimeseriesQuery,
    # TimeseriesQuery,
    # TimeseriesCharQuery,
)

import teehr.queries.utils as tqu
import teehr.models.queries as tmq

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_metrics(
    database_filepath: Union[str, Path],
    group_by: List[str],
    order_by: List[str],
    include_metrics: Union[List[tmq.MetricEnum], "all"],
    filters: Union[List[dict], None] = None,
    return_query: bool = False,
    include_geometry: bool = False,
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
    include_metrics = List[str]
        List of metrics (see below) for allowable list, or "all" to return all
        Placeholder, currently ignored -> returns "all"
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query: bool = False
        True returns the query string instead of the data
    include_geometry: bool = True
        True joins the geometry to the query results.
        Only works if `primary_location_id`
        is included as a group_by field.

    Returns
    -------
    results : Union[str, pd.DataFrame, gpd.GeoDataFrame]

    Filter, Order By and Group By Fields
    -----------------------------------
    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name
    * lead_time

    Available Metrics
    -----------------------
    Basic
    * primary_count
    * secondary_count
    * primary_minimum
    * secondary_minimum
    * primary_maximum
    * secondary_maximum
    * primary_average
    * secondary_average
    * primary_sum
    * secondary_sum
    * primary_variance
    * secondary_variance
    * max_value_delta
        max(secondary_value) - max(primary_value)
    * bias
        sum(primary_value - secondary_value)/count(*)

    HydroTools Metrics
    * nash_sutcliffe_efficiency
    * kling_gupta_efficiency
    * coefficient_of_extrapolation
    * coefficient_of_persistence
    * mean_error
    * mean_squared_error
    * root_mean_squared_error

    Time-based Metrics
    * primary_max_value_time
    * secondary_max_value_time
    * max_value_timedelta

    Examples:
        group_by = ["lead_time", "primary_location_id"]
        order_by = ["lead_time", "primary_location_id"]
        filters = [
            {
                "column": "primary_location_id",
                "operator": "=",
                "value": "'123456'"
            },
            {
                "column": "reference_time",
                "operator": "=",
                "value": "'2022-01-01 00:00'"
            },
            {
                "column": "lead_time",
                "operator": "<=",
                "value": "'10 days'"
            }
        ]
    """

    mq = MetricQueryDB.parse_obj(
        {
            "database_filepath": database_filepath,
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": include_metrics,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
        }
    )

    query = f"""
        WITH joined as (
            SELECT
                *
            FROM joined_timeseries
            {tqu.filters_to_sql(mq.filters)}
        )
        {tqu._nse_cte(mq)}
        {tqu._pmxt_cte(mq)}
        {tqu._smxt_cte(mq)}
        , metrics AS (
            SELECT
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
                {tqu._select_primary_count(mq)}
                {tqu._select_secondary_count(mq)}
                {tqu._select_primary_minimum(mq)}
                {tqu._select_secondary_minimum(mq)}
                {tqu._select_primary_maximum(mq)}
                {tqu._select_secondary_maximum(mq)}
                {tqu._select_primary_average(mq)}
                {tqu._select_secondary_average(mq)}
                {tqu._select_primary_sum(mq)}
                {tqu._select_secondary_sum(mq)}
                {tqu._select_primary_variance(mq)}
                {tqu._select_secondary_variance(mq)}
                {tqu._select_max_value_delta(mq)}
                {tqu._select_bias(mq)}
                {tqu._select_nash_sutcliffe_efficiency(mq)}
                {tqu._select_kling_gupta_efficiency(mq)}
                {tqu._select_mean_error(mq)}
                {tqu._select_mean_squared_error(mq)}
                {tqu._select_root_mean_squared_error(mq)}
            FROM
                joined
            {tqu._join_nse_cte(mq)}
            GROUP BY
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
        )
        SELECT
            metrics.*
            {tqu._select_primary_max_value_time(mq)}
            {tqu._select_secondary_max_value_time(mq)}
            {tqu._select_max_value_timedelta(mq)}
        FROM metrics
        {tqu._join_primary_join_max_time(mq)}
        {tqu._join_secondary_join_max_time(mq)}
        ORDER BY
            {",".join([f"metrics.{gb}" for gb in mq.group_by])}
    ;"""

    if mq.return_query:
        return tqu.remove_empty_lines(query)

    with duckdb.connect(str(database_filepath)) as con:
        query_tbl = con.sql(query)  # noqa
        if mq.include_geometry:
            # Join geometry
            query = """
                SELECT
                    geometry.name, geometry.geometry, query_tbl.*
                FROM
                    query_tbl
                JOIN
                    geometry
                ON
                    geometry.id = query_tbl.primary_location_id
            ;"""
            df = con.sql(query).to_df()
            return tqu.df_to_gdf(df)
        else:
            return query_tbl.to_df()


def join_and_save_timeseries(
    primary_filepath: str,
    secondary_filepath: str,
    crosswalk_filepath: str,
    order_by: List[str],
    con: duckdb.DuckDBPyConnection,
    filters: Union[List[dict], None] = None,
    return_query: bool = False,
    geometry_filepath: Union[str, None] = None,
    include_geometry: bool = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    """Load joined timeseries into a duckdb persistent database
    using database query.

    Parameters
    ----------
    primary_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards.  For example, "/path/to/parquet/*.parquet"
    secondary_filepath : str
        File path to the "forecast" data.  String must include path to file(s)
        and can include wildcards.  For example, "/path/to/parquet/*.parquet"
    crosswalk_filepath : str
        File path to single crosswalk file.
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query: bool = False
        True returns the query string instead of the data
    include_geometry: bool = True
        True joins the geometry to the query results.
        Only works if `primary_location_id`.
        is included as a group_by field.

    Returns
    -------
    results : Union[str, pd.DataFrame, gpd.GeoDataFrame]

    Filter and Order By Fields
    --------------------------
    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name
    * lead_time

    Examples:
        order_by = ["lead_time", "primary_location_id"]
        filters = [
            {
                "column": "primary_location_id",
                "operator": "=",
                "value": "'123456'"
            },
            {
                "column": "reference_time",
                "operator": "=",
                "value": "'2022-01-01 00:00'"
            },
            {
                "column": "lead_time",
                "operator": "<=",
                "value": "'10 days'"
            }
        ]
    """

    jtq = JoinedTimeseriesQuery.parse_obj(
        {
            "primary_filepath": primary_filepath,
            "secondary_filepath": secondary_filepath,
            "crosswalk_filepath": crosswalk_filepath,
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
            "geometry_filepath": geometry_filepath
        }
    )

    query = f"""
    WITH filtered_primary AS (
        SELECT * FROM(
            SELECT *,
                row_number() OVER(PARTITION BY value_time, location_id ORDER BY reference_time desc) AS rn
            FROM read_parquet("{str(jtq.primary_filepath)}")
            ) t
        WHERE rn = 1
    ),
    joined as (
        SELECT
            sf.reference_time,
            sf.value_time,
            sf.location_id as secondary_location_id,
            sf.value as secondary_value,
            sf.configuration,
            sf.measurement_unit,
            sf.variable_name,
            pf.value as primary_value,
            pf.location_id as primary_location_id,
            sf.value_time - sf.reference_time as lead_time,
            abs(primary_value - secondary_value) as absolute_difference
            {tqu.geometry_select_clause(jtq)}
        FROM read_parquet('{str(jtq.secondary_filepath)}') sf
        JOIN read_parquet('{str(jtq.crosswalk_filepath)}') cf
            on cf.secondary_location_id = sf.location_id
        JOIN filtered_primary pf
            on cf.primary_location_id = pf.location_id
            and sf.value_time = pf.value_time
            and sf.measurement_unit = pf.measurement_unit
            and sf.variable_name = pf.variable_name
        {tqu.geometry_join_clause(jtq)}
        {tqu.filters_to_sql(jtq.filters)}
    )
    INSERT INTO joined_timeseries
    SELECT
        *
    FROM
        joined
    ORDER BY
        {",".join(jtq.order_by)}
    ;""" # noqa

    # Join primary and secondary and insert into table joined_timeseries
    con.sql(query)

    if jtq.return_query:
        return tqu.remove_empty_lines(query)


def describe_timeseries(timeseries_filepath: str) -> Dict:
    """Retrieve descriptive stats for a time series.

    Parameters
    ----------
    timeseries_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards.  For example, "/path/to/parquet/*.parquet"

    Returns
    -------
    output_report : Union[str, pd.DataFrame, gpd.GeoDataFrame]
    """

    # TEST QUERIES

    # Find number of rows and unique locations
    query = f"""
        SELECT
        COUNT ( DISTINCT
            location_id
        )
        AS num_location_ids,
        COUNT(*) AS num_rows,
        MAX(value_time) as end_date,
        MIN(value_time) as start_date
        FROM read_parquet("{timeseries_filepath}")
        """
    df = duckdb.sql(query).to_df()
    num_location_ids = df["num_location_ids"][0]
    total_num_rows = df["num_rows"][0]
    start_date = df["start_date"][0]
    end_date = df["end_date"][0]

    # Find number of duplicates from all columns
    query = f"""
        SELECT
            value_time,
            location_id,
            value,
            measurement_unit,
            reference_time,
            configuration,
            variable_name,
        COUNT(*)
        FROM read_parquet("{timeseries_filepath}")
        GROUP BY
            value_time,
            location_id,
            value,
            measurement_unit,
            reference_time,
            configuration,
            variable_name,
        HAVING COUNT(*) > 1
        """
    df = duckdb.sql(query).to_df()
    num_duplicate_rows = df.index.size

    # Find number of duplicate value_times per location_id
    query = f"""
        WITH find_duplicates AS (
            SELECT
                value_time,
                location_id,
                measurement_unit,
                configuration,
                variable_name,
            COUNT(*) AS num_duplicates
            FROM read_parquet("{timeseries_filepath}")
            GROUP BY
                value_time,
                location_id,
                measurement_unit,
                configuration,
                variable_name,
            HAVING COUNT(*) > 1
        )
        SELECT
        COUNT( DISTINCT location_id)
        AS num_locations_with_duplicates
        FROM find_duplicates
        WHERE num_duplicates > 1
        """
    df = duckdb.sql(query).to_df()
    num_locations_with_duplicate_value_times = \
        df["num_locations_with_duplicates"][0]

    # Check time step integrity by reference_time and location_id
    query = f"""
        WITH value_time_diff AS (
            SELECT *,
                value_time - LAG(value_time)
            OVER(PARTITION BY
                location_id,
                reference_time
            ORDER BY value_time)
            AS value_time_step
            FROM read_parquet("{timeseries_filepath}")
        ),
        missing_timesteps AS (
            SELECT
                location_id,
                reference_time,
                MAX(value_time_step) AS max_timestep,
                MIN(value_time_step) AS min_timestep,
                max_timestep = min_timestep AS none_missing
            FROM value_time_diff
            GROUP BY location_id, reference_time
        )
            SELECT
                location_id,
                reference_time,
                none_missing
            FROM missing_timesteps
            WHERE none_missing != TRUE
        """
    df = duckdb.sql(query).to_df()
    num_locations_with_missing_timesteps = df.index.size

    output_report = {
        "Number of unique location IDs": num_location_ids,
        "Total number of rows": total_num_rows,
        "Start Date": start_date,
        "End Date": end_date,
        "Number of duplicate rows": num_duplicate_rows,
        "Number of location IDs with duplicate value times":
            num_locations_with_duplicate_value_times,
        "Number of location IDs with missing time steps":
            num_locations_with_missing_timesteps
    }

    return output_report
