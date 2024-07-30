import teehr_v0_3.models.queries as tmq
import teehr_v0_3.models.queries_database as tmqd
import teehr_v0_3.queries.utils as tqu
from typing import Union

from teehr_v0_3.queries.utils import filters_to_sql


def get_ind_parq_joined_timeseries_cte(
        mq: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery],
        joined_select_clause: str
) -> str:
    """Generate the joined CTE for individual parquet files.

    This is used to create the joined timeseries clause for querying
    individual parquet files as well as joining parquet files to
    insert in database.
    """
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
            FROM read_parquet({tqu._format_filepath(mq.secondary_filepath)}) sf
            JOIN read_parquet({tqu._format_filepath(mq.crosswalk_filepath)}) cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet({tqu._format_filepath(mq.primary_filepath)}) pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
            {tqu.filters_to_sql(mq.filters)}
        ),
        joined AS (
            {joined_select_clause}
        )
    """


def get_joined_timeseries_cte(
    mq: tmqd.MetricQuery,
    from_joined_timeseries_clause: str
) -> str:
    """Generate the metrics joined CTE for a database or joined parquet."""
    return f"""
        WITH joined as (
            SELECT
                *
            {from_joined_timeseries_clause}
            {filters_to_sql(mq.filters)}
        )
    """


def select_joined_clause(remove_duplicates: bool) -> str:
    """Generate the remove duplicates CTE for the MetricQuery."""
    if remove_duplicates:
        return """
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
        """
    else:
        return """
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
            FROM
                initial_joined
        """
