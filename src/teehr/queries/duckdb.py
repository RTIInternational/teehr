import warnings
import duckdb

import pandas as pd
import geopandas as gpd

from collections.abc import Iterable
from datetime import datetime
from typing import List, Union

from teehr.models.queries import (
    JoinedFilter,
    MetricQuery,
    JoinedTimeseriesQuery,
    TimeseriesFilter,
    TimeseriesQuery,
    TimeseriesCharQuery,
)

from teehr.queries.utils import (
    df_to_gdf
)

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_datetime_list_string(values):
    return [f"'{v.strftime(SQL_DATETIME_STR_FORMAT)}'" for v in values]


def format_iterable_value(
        values: Iterable[Union[str, int, float, datetime]]
) -> str:
    """Returns an SQL formatted string from list of values.

    Parameters
    ----------
    values : Iterable
        Contains values to be formatted as a string for SQL. Only one type of
        value (str, int, float, datetime) should be used. First value in list
        is used to determine value type. Values are not checked for type
        consistency.

    Returns
    -------
    formatted_string : str

    """

    # string
    if isinstance(values[0], str):
        return f"""({",".join([f"'{v}'" for v in values])})"""
    # int or float
    elif (
        isinstance(values[0], int)
        or isinstance(values[0], float)
    ):
        return f"""({",".join([f"{v}" for v in values])})"""
    # datetime
    elif isinstance(values[0], datetime):
        return f"""({",".join(get_datetime_list_string(values))})"""
    else:
        warnings.warn(
            "treating value as string because didn't know what else to do."
        )
        return f"""({",".join([f"'{str(v)}'" for v in values])})"""


def format_filter_item(filter: Union[JoinedFilter, TimeseriesFilter]) -> str:
    """Returns an SQL formatted string for single filter object.

    Parameters
    ----------
    filter: models.*Filter
        A single *Filter object.

    Returns
    -------
    formatted_string : str

    """

    if isinstance(filter.value, str):
        return f"""{filter.column} {filter.operator} '{filter.value}'"""
    elif (
        isinstance(filter.value, int)
        or isinstance(filter.value, float)
    ):
        return f"""{filter.column} {filter.operator} {filter.value}"""
    elif isinstance(filter.value, datetime):
        dt_str = filter.value.strftime(SQL_DATETIME_STR_FORMAT)
        return f"""{filter.column} {filter.operator} '{dt_str}'"""
    elif (
        isinstance(filter.value, Iterable)
        and not isinstance(filter.value, str)
    ):
        value = format_iterable_value(filter.value)
        return f"""{filter.column} {filter.operator} {value}"""
    else:
        warnings.warn(
            "treating value as string because didn't know what else to do."
        )
        return f"""{filter.column} {filter.operator} '{str(filter.value)}'"""


def filters_to_sql(filters: List[JoinedFilter]) -> List[str]:
    """Generate SQL where clause string from filters.

    Parameters
    ----------
    filters : List[MetricFilter]
        A list of MetricFilter objects describing the filters.

    Returns
    -------
    where_clause : str
        A where clause formatted string
    """
    if len(filters) > 0:
        filter_strs = []
        for f in filters:
            filter_strs.append(format_filter_item(f))
        qry = f"""WHERE {f" AND ".join(filter_strs)}"""
        return qry

    return "--no where clause"


def generate_geometry_join_clause(
        q: Union[MetricQuery, JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for"""
    if q.include_geometry:
        return f"""JOIN read_parquet('{str(q.geometry_filepath)}') gf
            on pf.location_id = gf.id
        """
    return ""


def generate_geometry_select_clause(
        q: Union[MetricQuery, JoinedTimeseriesQuery]
) -> str:
    if q.include_geometry:
        return ",gf.geometry as geometry"
    return ""


def generate_metric_geometry_join_clause(
        q: Union[MetricQuery, JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for"""
    if q.include_geometry:
        return f"""JOIN read_parquet('{str(q.geometry_filepath)}') gf
            on primary_location_id = gf.id
        """
    return ""


def _join_time_on(join: str, join_to: str, join_on: List[str]):
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
        AND {join}.n = 1
    """
    return qry


def _join_on(join: str, join_to: str, join_on: List[str]):
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
    """
    return qry


def get_metrics(
    primary_filepath: str,
    secondary_filepath: str,
    crosswalk_filepath: str,
    group_by: List[str],
    order_by: List[str],
    include_metrics: Union[List[str], str],
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

    # if mq.include_geometry:
    #     if "geometry" not in mq.group_by:
    #         mq.group_by.append("geometry")

    query = f"""
        WITH joined as (
            SELECT
                sf.reference_time
                , sf.value_time
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
            {filters_to_sql(mq.filters)}
        ),
        nse AS (
            SELECT
                {",".join(mq.group_by)}
                , pow(
                    primary_value - secondary_value, 2
                ) as primary_minus_secondary_squared
                , pow(
                    primary_value
                    - avg(primary_value)
                    OVER(PARTITION BY {",".join(mq.group_by)}), 2
                ) as primary_minus_primary_mean_squared
            FROM joined
        ),
        metrics AS (
            SELECT
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
                , count(secondary_value) as secondary_count
                , count(primary_value) as primary_count
                , min(secondary_value) as secondary_minimum
                , min(primary_value) as primary_minimum
                , max(secondary_value) as secondary_maximum
                , max(primary_value) as primary_maximum
                , avg(secondary_value) as secondary_average
                , avg(primary_value) as primary_average
                , sum(secondary_value) as secondary_sum
                , sum(primary_value) as primary_sum
                , var_pop(secondary_value) as secondary_variance
                , var_pop(primary_value) as primary_variance
                , max(secondary_value) - max(primary_value) as max_value_delta
                , sum(primary_value - secondary_value)/count(*) as bias
                , 1 - (
                    sum(nse.primary_minus_secondary_squared)
                    /sum(nse.primary_minus_primary_mean_squared)
                ) as nash_sutcliffe_efficiency
                , 1 - sqrt(
                    pow(corr(secondary_value, primary_value) - 1, 2)
                    + pow(stddev(secondary_value)
                        / stddev(primary_value) - 1, 2)
                    + pow(avg(secondary_value) / avg(primary_value) - 1, 2)
                ) AS kling_gupta_efficiency
                , sum(absolute_difference)/count(*) as mean_error
                , sum(power(absolute_difference, 2))/count(*)
                    as mean_squared_error
                , sqrt(sum(power(absolute_difference, 2))/count(*))
                    as root_mean_squared_error
            FROM
                joined
            {_join_on(join="nse", join_to="joined", join_on=mq.group_by)}
            GROUP BY
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
            ORDER BY
                {",".join([f"joined.{gb}" for gb in mq.order_by])}
        )
        SELECT
            metrics.*
            {generate_geometry_select_clause(mq)}
        FROM metrics
        {generate_metric_geometry_join_clause(mq)}
    ;"""

    if mq.return_query:
        return query

    df = duckdb.query(query).to_df()

    if mq.include_geometry:
        return df_to_gdf(df)

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
                {generate_geometry_select_clause(jtq)}
            FROM read_parquet('{str(jtq.secondary_filepath)}') sf
            JOIN read_parquet('{str(jtq.crosswalk_filepath)}') cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet('{str(jtq.primary_filepath)}') pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {generate_geometry_join_clause(jtq)}
        )
        SELECT * FROM
            joined
        {filters_to_sql(jtq.filters)}
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
        return df_to_gdf(df)

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
        {filters_to_sql(tq.filters)}
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

    join_max_time_on = _join_time_on(
        join="mxt",
        join_to="chars",
        join_on=tcq.group_by
    )

    query = f"""
        WITH fts AS (
            SELECT * FROM
            read_parquet('{str(tcq.timeseries_filepath)}') pf
            {filters_to_sql(tcq.filters)}
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


def get_metrics_org(
    primary_filepath: str,
    secondary_filepath: str,
    crosswalk_filepath: str,
    group_by: List[str],
    order_by: List[str],
    include_metrics: Union[List[str], str],
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

    if mq.include_geometry:
        if "geometry" not in mq.group_by:
            mq.group_by.append("geometry")

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
                {generate_geometry_select_clause(mq)}
            FROM read_parquet('{str(mq.secondary_filepath)}') sf
            JOIN read_parquet('{str(mq.crosswalk_filepath)}') cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet('{str(mq.primary_filepath)}') pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {generate_geometry_join_clause(mq)}
        ),
        metrics AS (
            SELECT
                {",".join(mq.group_by)},
                regr_intercept(secondary_value, primary_value) as intercept,
                covar_pop(secondary_value, primary_value) as covariance,
                corr(secondary_value, primary_value) as corr,
                regr_r2(secondary_value, primary_value) as r_squared,
                count(secondary_value) as secondary_count,
                count(primary_value) as primary_count,
                min(secondary_value) as secondary_minimum,
                min(primary_value) as primary_minimum,
                max(secondary_value) as secondary_maximum,
                max(primary_value) as primary_maximum,
                avg(secondary_value) as secondary_average,
                avg(primary_value) as primary_average,
                sum(secondary_value) as secondary_sum,
                sum(primary_value) as primary_sum,
                var_pop(secondary_value) as secondary_variance,
                var_pop(primary_value) as primary_variance,
                max(secondary_value) - max(primary_value) as max_value_delta,
                sum(primary_value - secondary_value)/count(*) as bias
            FROM
                joined
            {filters_to_sql(mq.filters)}
            GROUP BY
                {",".join(mq.group_by)}
            ORDER BY
                {",".join(mq.order_by)}
        )
        SELECT
            metrics.*
        FROM metrics
    ;"""

    if mq.return_query:
        return query

    df = duckdb.query(query).to_df()

    if mq.include_geometry:
        return df_to_gdf(df)

    return df
