"""A module defining duckdb sql queries for parquet files."""
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
    remove_duplicates: bool = True,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    r"""Calculate performance metrics using a parquet query.

    Parameters
    ----------
    primary_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".
    secondary_filepath : str
        File path to the "forecast" data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".
    crosswalk_filepath : str
        File path to single crosswalk file.
    group_by : List[str]
        List of column/field names to group timeseries data by.
        Must provide at least one.
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    include_metrics : List[str]
        List of metrics (see below) for allowable list, or "all" to return all
        Placeholder, currently ignored -> returns "all".
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query : bool = False
        True returns the query string instead of the data.
    include_geometry : bool = True
        True joins the geometry to the query results.
        Only works if `primary_location_id`
        is included as a group_by field.
    remove_duplicates : bool = True
        True (default) removes joined timeseries rows with duplicate primary
        values, where unique values are defined by the value_time,
        secondary_reference_time, location_id, configuration,
        variable_name and measurement_unit fields.
        False does not check for or remove duplicate values.
        This option can be used to improve performance if you are certain you
        do not have duplicate primary_values.

    Returns
    -------
    Union[str, pd.DataFrame, gpd.GeoDataFrame]
        The query string or a DataFrame or GeoDataFrame of query results.

    Notes
    -----
    Filter, Order By and Group By Fields:

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
    * [any user-added fields]

    Metrics:

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
    * mean_error
    * mean_absolute_error
    * mean_squared_error
    * mean_absolute_relative_error
    * root_mean_squared_error
    * relative_bias
    * multiplicative_bias
    * pearson_correlation
    * r_squared
    * nash_sutcliffe_efficiency
    * nash_sutcliffe_efficiency_normalized
    * kling_gupta_efficiency
    * primary_max_value_time
    * secondary_max_value_time
    * max_value_timedelta

    Examples
    --------
    >>> order_by = ["lead_time", "primary_location_id"]

    >>> group_by = ["lead_time", "primary_location_id"]

    >>> filters = [
    >>>     {
    >>>         "column": "primary_location_id",
    >>>         "operator": "=",
    >>>         "value": "gage-A",
    >>>     },
    >>>     {
    >>>         "column": "reference_time",
    >>>         "operator": "=",
    >>>         "value": "2022-01-01 00:00:00",
    >>>     },
    >>>     {"column": "lead_time", "operator": "<=", "value": "10 hours"},
    >>> ]
    """
    mq = MetricQuery.model_validate(
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
            "geometry_filepath": geometry_filepath,
            "remove_duplicates": remove_duplicates
        }
    )

    query = f"""
        WITH initial_joined AS (
            SELECT
                sf.reference_time
                , sf.value_time as value_time
                , sf.location_id as secondary_location_id
                , pf.reference_time as primary_reference_time
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
            JOIN read_parquet("{str(mq.primary_filepath)}") pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {tqu.filters_to_sql(mq.filters)}
        ),
        joined AS (
            {tqu._remove_duplicates_mq_cte(mq)}
        )
        {tqu._nse_cte(mq)}
        {tqu._annual_metrics_cte(mq)}
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
                {tqu._select_mean_error(mq)}
                {tqu._select_nash_sutcliffe_efficiency(mq)}
                {tqu._select_nash_sutcliffe_efficiency_normalized(mq)}
                {tqu._select_kling_gupta_efficiency(mq)}
                {tqu._select_mean_absolute_error(mq)}
                {tqu._select_mean_squared_error(mq)}
                {tqu._select_root_mean_squared_error(mq)}
                {tqu._select_primary_max_value_time(mq)}
                {tqu._select_secondary_max_value_time(mq)}
                {tqu._select_max_value_timedelta(mq)}
                {tqu._select_relative_bias(mq)}
                {tqu._select_multiplicative_bias(mq)}
                {tqu._select_mean_absolute_relative_error(mq)}
                {tqu._select_pearson_correlation(mq)}
                {tqu._select_r_squared(mq)}
            FROM
                joined
            {tqu._join_nse_cte(mq)}
            GROUP BY
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
        )
        SELECT
            {",".join([f"metrics.{ob}" for ob in mq.group_by])}
            {tqu.metrics_select_clause(mq)}
            {tqu.geometry_select_clause(mq)}
        FROM metrics
            {tqu.metric_geometry_join_clause(mq)}
        {tqu._join_annual_metrics_cte(mq)}
        ORDER BY
            {",".join([f"metrics.{ob}" for ob in mq.order_by])}
    ;"""

    if mq.return_query:
        return tqu.remove_empty_lines(query)

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
    remove_duplicates: bool = True,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    r"""Retrieve joined timeseries using a parquet query.

    Parameters
    ----------
    primary_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".
    secondary_filepath : str
        File path to the "forecast" data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".
    crosswalk_filepath : str
        File path to single crosswalk file.
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query : bool = False
        True returns the query string instead of the data.
    include_geometry : bool = True
        True joins the geometry to the query results.
        Only works if `primary_location_id`.
        is included as a group_by field.
    remove_duplicates : bool = True
        True (default) removes joined timeseries rows with duplicate primary
        values, where unique values are defined by the value_time,
        secondary_reference_time, location_id, configuration,
        variable_name and measurement_unit fields.
        False does not check for or remove duplicate values.
        This option can be used to improve performance if you are certain you
        do not have duplicate primary_values.

    Returns
    -------
    Union[str, pd.DataFrame, gpd.GeoDataFrame]
        The query string or a DataFrame or GeoDataFrame of query results.

    Notes
    -----
    Filter and Order By Fields:

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

    Examples
    --------
    >>> order_by = ["lead_time", "primary_location_id"]
    >>> filters = [
    >>>     {
    >>>         "column": "primary_location_id",
    >>>         "operator": "=",
    >>>         "value": "'123456'"
    >>>     },
    >>>     {
    >>>         "column": "reference_time",
    >>>         "operator": "=",
    >>>         "value": "'2022-01-01 00:00'"
    >>>     },
    >>>     {
    >>>         "column": "lead_time",
    >>>         "operator": "<=",
    >>>         "value": "'10 days'"
    >>>     }
    >>> ]
    """
    jtq = JoinedTimeseriesQuery.model_validate(
        {
            "primary_filepath": primary_filepath,
            "secondary_filepath": secondary_filepath,
            "crosswalk_filepath": crosswalk_filepath,
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
            "geometry_filepath": geometry_filepath,
            "remove_duplicates": remove_duplicates
        }
    )

    query = f"""
        WITH initial_joined as (
            SELECT
                sf.reference_time,
                sf.value_time,
                sf.location_id as secondary_location_id,
                sf.value as secondary_value,
                sf.configuration,
                sf.measurement_unit,
                sf.variable_name,
                pf.reference_time as primary_reference_time,
                pf.value as primary_value,
                pf.location_id as primary_location_id,
                sf.value_time - sf.reference_time as lead_time
                {tqu.geometry_select_clause(jtq)}
            FROM read_parquet('{str(jtq.secondary_filepath)}') sf
            JOIN read_parquet('{str(jtq.crosswalk_filepath)}') cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet("{str(jtq.primary_filepath)}") pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {tqu.geometry_join_clause(jtq)}
            {tqu.filters_to_sql(jtq.filters)}
        ),
        joined AS (
            {tqu._remove_duplicates_jtq_cte(jtq)}
        )
        SELECT
            *
        FROM
            joined
        ORDER BY
            {",".join(jtq.order_by)}
    ;"""

    if jtq.return_query:
        return tqu.remove_empty_lines(query)

    df = duckdb.query(query).to_df()

    df["primary_location_id"] = df["primary_location_id"].astype("category")
    df["secondary_location_id"] = df["secondary_location_id"].astype(
        "category"
    )
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
    r"""Retrieve timeseries using a parquet query.

    Parameters
    ----------
    timeseries_filepath : str
        File path to the timeseries data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query : bool = False
        True returns the query string instead of the data.

    Returns
    -------
    Union[str, pd.DataFrame, gpd.GeoDataFrame]
        The query string or a DataFrame or GeoDataFrame of query results.

    Notes
    -----
    Filter and Order By Fields:

    * value_time
    * location_id
    * value
    * measurement_unit
    * reference_time
    * configuration
    * variable_name

    Examples
    --------
    >>> order_by = ["lead_time", "primary_location_id"]
    >>> filters = [
    >>>     {
    >>>         "column": "location_id",
    >>>         "operator": "in",
    >>>         "value": [12345, 54321]
    >>>     },
    >>> ]
    """
    tq = TimeseriesQuery.model_validate(
        {
            "timeseries_filepath": timeseries_filepath,
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
        }
    )

    query = f"""
        WITH joined as (
            SELECT
                sf.reference_time,
                sf.value_time,
                sf.location_id,
                sf.value,
                sf.configuration,
                sf.measurement_unit,
                sf.variable_name
            FROM
                read_parquet("{str(tq.timeseries_filepath)}") sf
            {tqu.filters_to_sql(tq.filters)}
        )
        SELECT * FROM
            joined
        ORDER BY
            {",".join(tq.order_by)}
    ;"""

    if tq.return_query:
        return tqu.remove_empty_lines(query)

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
    r"""Retrieve timeseries characteristics using a parquet query.

    Parameters
    ----------
    timeseries_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".
    group_by : List[str]
        List of column/field names to group timeseries data by.
        Must provide at least one.
    order_by : List[str]
        List of column/field names to order results by.
        Must provide at least one.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query : bool = False
        True returns the query string instead of the data.

    Returns
    -------
    Union[str, pd.DataFrame, gpd.GeoDataFrame]
        The query string or a DataFrame or GeoDataFrame of query results.

    Notes
    -----
    Filter, Group By and Order By Fields

    * value_time
    * location_id
    * value
    * measurement_unit
    * reference_time
    * configuration
    * variable_name

    Examples
    --------
    >>> order_by = ["lead_time", "primary_location_id"]
    >>> filters = [
    >>>     {
    >>>         "column": "primary_location_id",
    >>>         "operator": "=",
    >>>         "value": "'123456'"
    >>>     },
    >>>     {
    >>>         "column": "reference_time",
    >>>         "operator": "=",
    >>>         "value": "'2022-01-01 00:00'"
    >>>     },
    >>>     {
    >>>         "column": "lead_time",
    >>>         "operator": "<=",
    >>>         "value": "'10 days'"
    >>>     }
    >>> ]
    """
    tcq = TimeseriesCharQuery.model_validate(
        {
            "timeseries_filepath": timeseries_filepath,
            "order_by": order_by,
            "group_by": group_by,
            "filters": filters,
            "return_query": return_query,
        }
    )

    join_max_time_on = tqu._join_time_on(
        join="mxt", join_to="chars", join_on=tcq.group_by
    )

    order_by = [f"chars.{val}" for val in tcq.order_by]

    query = f"""
        WITH fts AS (
            SELECT sf.* FROM
            read_parquet('{str(tcq.timeseries_filepath)}') sf
            {tqu.filters_to_sql(tcq.filters)}
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
        )
        SELECT
            chars.*
            ,mxt.value_time as max_value_time
        FROM chars
            {join_max_time_on}
        ORDER BY
            {",".join(order_by)}
    ;"""

    if tcq.return_query:
        return tqu.remove_empty_lines(query)

    df = duckdb.query(query).to_df()

    return df
