import teehr.queries.utils as tqu
import teehr.models.queries_database as tmqd


def get_the_joined_timeseries_clause(
    mq: tmqd.MetricQuery,
    from_joined_timeseries_clause: str
) -> str:
    """Generate the metrics joined CTE for a database."""
    return f"""
        WITH joined as (
            SELECT
                *
            {from_joined_timeseries_clause}
            {tqu.filters_to_sql(mq.filters)}
        )
    """


def get_the_metrics_query(
    mq: tmqd.MetricQuery,
    from_joined_timeseries_clause: str
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
    query = tqu.joined_timeseries_clause(mq, from_joined_timeseries_clause) + \
        tqu.metrics_calculation_clause(mq)

    return query


def get_the_joined_timeseries_query(
    jtq: tmqd.JoinedTimeseriesQuery,
    from_joined_timeseries_clause: str
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
    * lead_time
    * absolute_difference
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
    query = f"""
        SELECT
            sf.*
        {tqu.geometry_select_clause(jtq)}
        {from_joined_timeseries_clause}
        {tqu.metric_geometry_join_clause_db(jtq)}
        {tqu.filters_to_sql(jtq.filters)}
        ORDER BY
            {",".join(jtq.order_by)}
    ;"""

    return query


def get_the_timeseries_query(
    tq: tmqd.TimeseriesQuery,
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
    * lead_time
    * absolute_difference
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


def get_the_timeseries_char_query(
    tcq: tmqd.TimeseriesCharQuery,
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
    * lead_time
    * [any user-added fields]

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
    join_max_time_on = tqu._join_time_on(
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


def get_the_unique_field_values_query(
    fn: tmqd.JoinedTimeseriesFieldName,
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