import duckdb

import pandas as pd
import geopandas as gpd

from typing import List, Union

from teehr.models.queries import (
    MetricQuery,
    JoinedTimeseriesQuery,
    TimeseriesQuery,
    TimeseriesCharQuery,
)

import teehr.queries.utils as tqu
import teehr.models.queries as tmq

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_metrics(
    primary_filepath: str,
    secondary_filepath: str,
    crosswalk_filepath: str,
    group_by: List[str],
    order_by: List[str],
    include_metrics: Union[List[tmq.MetricEnum], "all"],
    filters: Union[List[dict], None] = None,
    return_query: bool = False,
    geometry_filepath: Union[str, None] = None,
    include_geometry: bool = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    """Calculate performance metrics using database queries.

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

    mq = MetricQuery.parse_obj(
        {
            "primary_filepath": primary_filepath,
            "secondary_filepath": secondary_filepath,
            "crosswalk_filepath": crosswalk_filepath,
            "group_by": group_by,
            "order_by": order_by,
            "include_metrics": include_metrics,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
            "geometry_filepath": geometry_filepath
        }
    )

    query = f"""
        WITH joined as (
            SELECT
                sf.reference_time
                , sf.value_time as value_time
                , sf.location_id as secondary_location_id
                , sf.value as secondary_value
                , sf.configuration
                , sf.measurement_unit
                , sf.variable_name
                , pf.value as primary_value
                , pf.location_id as primary_location_id
                , sf.value_time - sf.reference_time as lead_time
                , abs(pf.value - sf.value) as absolute_difference
            FROM read_parquet('{str(mq.secondary_filepath)}') sf
            JOIN read_parquet('{str(mq.crosswalk_filepath)}') cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet('{str(mq.primary_filepath)}') pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {tqu._filters_to_sql(mq.filters)}
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
            ORDER BY
                {",".join([f"joined.{gb}" for gb in mq.order_by])}
        )
        SELECT
            metrics.*
            {tqu._select_primary_max_value_time(mq)}
            {tqu._select_secondary_max_value_time(mq)}
            {tqu._select_max_value_timedelta(mq)}
            {tqu._geometry_select_clause(mq)}
        FROM metrics
        {tqu._metric_geometry_join_clause(mq)}
        {tqu._join_primary_join_max_time(mq)}
        {tqu._join_secondary_join_max_time(mq)}
    ;"""

    if mq.return_query:
        return query

    df = duckdb.query(query).to_df()

    if mq.include_geometry:
        return tqu.df_to_gdf(df)

    return df


def get_joined_timeseries(
    primary_filepath: str,
    secondary_filepath: str,
    crosswalk_filepath: str,
    order_by: List[str],
    filters: Union[List[dict], None] = None,
    return_query: bool = False,
    geometry_filepath: Union[str, None] = None,
    include_geometry: bool = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    """Retrieve joined timeseries using database query.

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
        WITH joined as (
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
                sf.value_time - sf.reference_time as lead_time
                {tqu._geometry_select_clause(jtq)}
            FROM read_parquet('{str(jtq.secondary_filepath)}') sf
            JOIN read_parquet('{str(jtq.crosswalk_filepath)}') cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet('{str(jtq.primary_filepath)}') pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {tqu._geometry_join_clause(jtq)}
        )
        SELECT * FROM
            joined
        {tqu._filters_to_sql(jtq.filters)}
        ORDER BY
            {",".join(jtq.order_by)}
    ;"""

    if jtq.return_query:
        return query

    df = duckdb.query(query).to_df()

    df["primary_location_id"] = df["primary_location_id"].astype("category")
    df["secondary_location_id"] = df["secondary_location_id"].astype("category")  # noqa
    df["configuration"] = df["configuration"].astype("category")
    df["measurement_unit"] = df["measurement_unit"].astype("category")
    df["variable_name"] = df["variable_name"].astype("category")

    if jtq.include_geometry:
        return tqu.df_to_gdf(df)

    return df


def get_timeseries(
    timeseries_filepath: str,
    order_by: List[str],
    filters: Union[List[dict], None] = None,
    return_query: bool = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    """Retrieve joined timeseries using database query.

    Parameters
    ----------
    timeseries_filepath : str
        File path to the timeseries data.  String must include path to file(s)
        and can include wildcards.  For example, "/path/to/parquet/*.parquet"
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query: bool = False
        True returns the query string instead of the data

    Returns
    -------
    results : Union[str, pd.DataFrame, gpd.GeoDataFrame]

    Examples:
        order_by = ["lead_time", "primary_location_id"]
        filters = [
            {
                "column": "location_id",
                "operator": "in",
                "value": [12345, 54321]
            },
        ]
    """
    tq = TimeseriesQuery.parse_obj(
        {
            "timeseries_filepath": timeseries_filepath,
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query
        }
    )

    query = f"""
        WITH joined as (
            SELECT
                pf.reference_time,
                pf.value_time,
                pf.location_id,
                pf.value,
                pf.configuration,
                pf.measurement_unit,
                pf.variable_name
            FROM
                read_parquet('{str(tq.timeseries_filepath)}') pf
        )
        SELECT * FROM
            joined
        {tqu._filters_to_sql(tq.filters)}
        ORDER BY
            {",".join(tq.order_by)}
    ;"""

    if tq.return_query:
        return query

    df = duckdb.query(query).to_df()

    df["location_id"] = df["location_id"].astype("category")
    df["configuration"] = df["configuration"].astype("category")
    df["measurement_unit"] = df["measurement_unit"].astype("category")
    df["variable_name"] = df["variable_name"].astype("category")

    return df


def get_timeseries_chars(
    timeseries_filepath: str,
    group_by: list[str],
    order_by: List[str],
    filters: Union[List[dict], None] = None,
    return_query: bool = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    """Retrieve joined timeseries using database query.

    Parameters
    ----------
    timeseries_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards.  For example, "/path/to/parquet/*.parquet"
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    group_by : List[str]
        List of column/field names to group timeseries data by.
        Must provide at least one.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query: bool = False
        True returns the query string instead of the data

    Returns
    -------
    results : Union[str, pd.DataFrame, gpd.GeoDataFrame]

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

    tcq = TimeseriesCharQuery.parse_obj(
        {
            "timeseries_filepath": timeseries_filepath,
            "order_by": order_by,
            "group_by": group_by,
            "filters": filters,
            "return_query": return_query
        }
    )

    join_max_time_on = tqu._join_time_on(
        join="mxt",
        join_to="chars",
        join_on=tcq.group_by
    )

    query = f"""
        WITH fts AS (
            SELECT * FROM
            read_parquet('{str(tcq.timeseries_filepath)}') pf
            {tqu._filters_to_sql(tcq.filters)}
        ),
        mxt AS (
            SELECT
                {",".join(tcq.group_by)}
                , value
                , value_time
                , ROW_NUMBER() OVER(
                    PARTITION BY {",".join(tcq.group_by)}
                    ORDER BY value DESC, value_time
                ) as n
            FROM fts
        ),
        chars AS (
            SELECT
                {",".join(tcq.group_by)}
                ,count(fts.value) as count
                ,min(fts.value) as min
                ,max(fts.value) as max
                ,avg(fts.value) as average
                ,sum(fts.value) as sum
                ,var_pop(fts.value) as variance
            FROM
                fts
            GROUP BY
                {",".join(tcq.group_by)}
            ORDER BY
                {",".join(tcq.order_by)}
        )
        SELECT
            chars.*
            ,mxt.value_time as max_value_time
        FROM chars
        {join_max_time_on}

    ;"""

    if tcq.return_query:
        return query

    df = duckdb.query(query).to_df()

    return df
