"""A module defining duckdb sql queries for a persistent database."""
import duckdb

from typing import Dict, List, Union
from pathlib import Path

from teehr_v0_3.models.queries_database import (
    MetricQuery,
    # InsertJoinedTimeseriesQuery,
    JoinedTimeseriesQuery,
    TimeseriesQuery,
    TimeseriesCharQuery,
    JoinedTimeseriesFieldName
)

import teehr_v0_3.queries.joined as tqj
import teehr_v0_3.queries.metrics as tqm
import teehr_v0_3.queries.utils as tqu

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def create_get_metrics_query(
    mq: MetricQuery,
    from_joined_timeseries_clause: str,
    join_geometry_clause: str
) -> str:
    """Build the query string to calculate performance metrics.

    Parameters
    ----------
    mq : MetricQuery
        Pydantic model containing query parameters.

    Returns
    -------
    str
        The query string.

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
    >>> order_by = ["primary_location_id"]

    >>> group_by = ["primary_location_id"]

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
    >>>     }
    >>> ]
    """
    joined = tqj.get_joined_timeseries_cte(
        mq,
        from_joined_timeseries_clause
    )
    metrics = tqm.get_metrics_clause(
        mq,
        join_geometry_clause
    )

    query = joined + metrics

    return query


def create_join_and_save_timeseries_query(jtq: JoinedTimeseriesQuery) -> str:
    """Load joined timeseries into a duckdb persistent database.

    Parameters
    ----------
    jtq : JoinedTimeseriesQuery
        Pydantic model containing query parameters.

    Returns
    -------
    str
        The query string.
    """

    select_joined_clause = tqj.select_joined_clause(True)
    joined = tqj.get_ind_parq_joined_timeseries_cte(jtq, select_joined_clause)

    insert = f"""
    INSERT INTO joined_timeseries
    SELECT
        *
    FROM
        joined
    ORDER BY
        {",".join(jtq.order_by)}
    ;"""

    query = joined + insert

    return query


def describe_timeseries(
        timeseries_filepath: Union[str, Path, List[Union[str, Path]]]
) -> Dict:
    r"""Retrieve descriptive stats for a time series.

    Parameters
    ----------
    timeseries_filepath : str
        File path to the "observed" data.  String must include path to file(s)
        and can include wildcards. For example, "/path/to/parquet/\\*.parquet".

    Returns
    -------
    Dict
        A dictionary of summary statistics for a timeseries.
    """
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
        FROM read_parquet({tqu._format_filepath(timeseries_filepath)})
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
        FROM read_parquet({tqu._format_filepath(timeseries_filepath)})
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
            FROM read_parquet({tqu._format_filepath(timeseries_filepath)})
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
    num_locations_with_duplicate_value_times = df[
        "num_locations_with_duplicates"
    ][0]

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
            FROM read_parquet({tqu._format_filepath(timeseries_filepath)})
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
            num_locations_with_missing_timesteps,
    }

    return output_report


def create_get_joined_timeseries_query(
    jtq: JoinedTimeseriesQuery,
    from_joined_timeseries_clause: str,
    join_geometry_clause: str
) -> str:
    """Retrieve joined timeseries using database query.

    Parameters
    ----------
    jtq : JoinedTimeseriesQuery
        Pydantic model containing query parameters.

    Returns
    -------
    str
        The query string.

    Notes
    -----
    Filter By Fields:

    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name
    * [any user-added fields]

    Order By Fields:

    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name

    Examples
    --------
    >>> order_by = ["primary_location_id"]

    >>> group_by = ["primary_location_id"]

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
    >>>     }
    >>> ]
    """
    joined = tqj.get_joined_timeseries_cte(jtq, from_joined_timeseries_clause)

    select = f"""
        SELECT
            joined.*
            {tqu.geometry_select_clause(jtq)}
        FROM
            joined
            {join_geometry_clause}
        ORDER BY
            {",".join(jtq.order_by)}
    ;"""

    query = joined + select

    return query


def create_get_timeseries_query(
    tq: TimeseriesQuery,
    from_joined_timeseries_clause: str
) -> str:
    """Retrieve joined timeseries using database query.

    Parameters
    ----------
    tq : TimeseriesQuery
        Pydantic model containing query parameters.

    Returns
    -------
    str
        The query string.

    Notes
    -----
    Filter By Fields:

    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name
    * [any user-added fields]

    Order By Fields:

    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name

    Examples
    --------
    >>> order_by = ["primary_location_id"]

    >>> group_by = ["primary_location_id"]

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
    >>>     }
    >>> ]
    """
    if tq.timeseries_name == "primary":
        query = f"""
            SELECT
                ANY_VALUE(reference_time) AS reference_time,
                ANY_VALUE(primary_value) AS value,
                value_time,
                primary_location_id AS location_id,
                'primary' as configuration,
                measurement_unit,
                variable_name,
            {from_joined_timeseries_clause}
            {tqu.filters_to_sql(tq.filters)}
            GROUP BY
                value_time,
                primary_location_id,
                --configuration,
                measurement_unit,
                variable_name,
            ORDER BY
                {",".join(tq.order_by)}
        ;"""
    else:
        query = f"""
            SELECT
                reference_time,
                value_time,
                secondary_location_id AS location_id,
                secondary_value AS value,
                configuration,
                measurement_unit,
                variable_name,
            {from_joined_timeseries_clause}
            {tqu.filters_to_sql(tq.filters)}
            ORDER BY
                {",".join(tq.order_by)}
        ;"""

    return query


def create_get_timeseries_char_query(
    tcq: TimeseriesCharQuery,
    from_joined_timeseries_clause: str
) -> str:
    """Retrieve joined timeseries using database query.

    Parameters
    ----------
    tcq : TimeseriesCharQuery
        Pydantic model containing query parameters.

    Returns
    -------
    str
        The query string.

    Notes
    -----
    Filter, Order By and Group By Fields

    * reference_time
    * primary_location_id
    * secondary_location_id
    * primary_value
    * secondary_value
    * value_time
    * configuration
    * measurement_unit
    * variable_name
    * [any user-added fields]

    Examples
    --------
    >>> order_by = ["primary_location_id"]

    >>> group_by = ["primary_location_id"]

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
    >>>     }
    >>> ]
    """
    join_max_time_on = tqu.join_time_on(
        join="mxt", join_to="chars", join_on=tcq.group_by
    )

    order_by = [f"chars.{val}" for val in tcq.order_by]

    # Create the fts_clause to remove duplicates if primary time series
    # has been specified
    if tcq.timeseries_name == "primary":

        selected_primary_fields = ["reference_time",
                                   "primary_value",
                                   "value_time",
                                   "primary_location_id",
                                   "configuration",
                                   "measurement_unit",
                                   "variable_name"]
        # Format any group by fields that are not selected
        # by default
        gb_fields = ""
        for gb_fld in tcq.group_by:
            if gb_fld not in selected_primary_fields:
                gb_fields += f"ANY_VALUE({gb_fld}) as {gb_fld}, "

        fts_clause = f"""
                     SELECT
                        ANY_VALUE(reference_time) AS reference_time,
                        ANY_VALUE(primary_value) AS primary_value,
                        value_time,
                        primary_location_id,
                        'primary' as configuration,
                        measurement_unit,
                        variable_name,
                        {gb_fields}
                     {from_joined_timeseries_clause}
                     {tqu.filters_to_sql(tcq.filters)}
                     GROUP BY
                        value_time,
                        primary_location_id,
                        --configuration,
                        measurement_unit,
                        variable_name,
                    """
    else:
        fts_clause = f"""
                      SELECT
                        *
                      {from_joined_timeseries_clause}
                      {tqu.filters_to_sql(tcq.filters)}
                      """

    query = f"""
        WITH fts AS (
            {fts_clause}
        ),
        mxt AS (
            SELECT
                {",".join(tcq.group_by)}
                , {tcq.timeseries_name}_value
                , value_time
                , ROW_NUMBER() OVER(
                    PARTITION BY {",".join(tcq.group_by)}
                    ORDER BY {tcq.timeseries_name}_value DESC, value_time
                ) as n
            FROM fts
        ),
        chars AS (
            SELECT
                {",".join(tcq.group_by)}
                ,count(fts.{tcq.timeseries_name}_value) as count
                ,min(fts.{tcq.timeseries_name}_value) as min
                ,max(fts.{tcq.timeseries_name}_value) as max
                ,avg(fts.{tcq.timeseries_name}_value) as average
                ,sum(fts.{tcq.timeseries_name}_value) as sum
                ,var_pop(fts.{tcq.timeseries_name}_value) as variance
            FROM
                fts
            GROUP BY
                {",".join(tcq.group_by)}
        )
        SELECT
            chars.{tcq.timeseries_name}_location_id as location_id,
            chars.count,
            chars.min,
            chars.max,
            chars.average,
            chars.sum,
            chars.variance,
            mxt.value_time as max_value_time
        FROM chars
            {join_max_time_on}
        ORDER BY
            {",".join(order_by)}
    ;"""

    return query


def create_unique_field_values_query(
    fn: JoinedTimeseriesFieldName,
    from_joined_timeseries_clause: str
) -> str:
    """Create a query for identifying unique values in a field.

    Parameters
    ----------
    fn : JoinedTimeseriesFieldName
        Name of the field to query for unique values.

    Returns
    -------
    str
        The query string.
    """
    query = f"""
        SELECT
        DISTINCT
            {fn.field_name}
        AS unique_{fn.field_name}_values,
        {from_joined_timeseries_clause}
        ORDER BY
            {fn.field_name}
        """

    return query
