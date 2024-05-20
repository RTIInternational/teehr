"""A module defining queries for un-joined timeseries data."""
import duckdb

import teehr.queries.utils as tqu
import teehr.models.queries as tmq
import teehr.models.queries_database as tmqd
# from teehr.models.queries import (
#     MetricQuery,
#     JoinedTimeseriesQuery,
#     TimeseriesQuery,
#     TimeseriesCharQuery,
# )


def get_the_initial_joined_timeseries_clause(
    mq: tmq.MetricQuery
) -> str:
    """Generate the metrics joined CTE for un-joined parquet files."""
    return f"""
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
    """


def get_the_join_and_save_timeseries_query(jtq: tmqd.JoinedTimeseriesQuery) -> str:
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
    query = f"""
    WITH initial_joined as (
        SELECT
            sf.reference_time,
            sf.value_time,
            sf.location_id as secondary_location_id,
            sf.value as secondary_value,
            sf.configuration,
            sf.measurement_unit,
            pf.reference_time as primary_reference_time,
            sf.variable_name,
            pf.value as primary_value,
            pf.location_id as primary_location_id,
            sf.value_time - sf.reference_time as lead_time,
            abs(primary_value - secondary_value) as absolute_difference
        FROM read_parquet('{str(jtq.secondary_filepath)}') sf
        JOIN read_parquet('{str(jtq.crosswalk_filepath)}') cf
            on cf.secondary_location_id = sf.location_id
        JOIN read_parquet("{str(jtq.primary_filepath)}") pf
            on cf.primary_location_id = pf.location_id
            and sf.value_time = pf.value_time
            and sf.measurement_unit = pf.measurement_unit
            and sf.variable_name = pf.variable_name
    ),
    joined AS (
        SELECT
            reference_time
            , value_time
            , secondary_location_id
            , secondary_value
            , configuration
            , measurement_unit
            , variable_name
            , primary_value
            , primary_location_id
            , lead_time
            , absolute_difference
        FROM(
            SELECT *,
                row_number()
            OVER(
                PARTITION BY value_time,
                             primary_location_id,
                             configuration,
                             variable_name,
                             measurement_unit,
                             reference_time
                ORDER BY primary_reference_time desc
                ) AS rn
            FROM initial_joined
            )
        WHERE rn = 1
    )
    INSERT INTO joined_timeseries
    SELECT
        *
    FROM
        joined
    ORDER BY
        {",".join(jtq.order_by)}
    ;"""

    return query


def describe_timeseries(timeseries_filepath: str) -> Dict:
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
            num_locations_with_missing_timesteps,
    }

    return output_report


def get_the_joined_timeseries_query(jtq: tmq.JoinedTimeseriesQuery) -> str:
    r"""Retrieve joined timeseries using a parquet query.

    Returns
    -------
    Union[str, pd.DataFrame, gpd.GeoDataFrame]
        The query string or a DataFrame or GeoDataFrame of query results.

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
    """
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

    return query


def get_the_timeseries_query(tq: tmq.TimeseriesQuery) -> str:
    """Get the timeseries query.

    Parameters
    ----------
    jtq : tmq.TimeseriesQuery
        _description_

    Returns
    -------
    str
        _description_
    """
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

    return query


def get_the_timeseries_char_query(tcq: tmq.TimeseriesCharQuery) -> str:
    """Get the timeseries char query.

    Parameters
    ----------
    tcq : tmq.TimeseriesCharQuery
        _description_

    Returns
    -------
    str
        _description_
    """
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

    return query