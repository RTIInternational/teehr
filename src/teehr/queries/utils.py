"""A module defining common utilities for queries."""
import pandas as pd
import geopandas as gpd
import warnings

from collections.abc import Iterable
from datetime import datetime
from typing import List, Union, Any
from pathlib import Path

import teehr.models.queries as tmq
import teehr.models.queries_database as tmqd

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def _get_datetime_list_string(values):
    """Get a datetime list as a list of strings."""
    return [f"'{v.strftime(SQL_DATETIME_STR_FORMAT)}'" for v in values]


def _format_iterable_value(
    values: Iterable[Union[str, int, float, datetime]]
) -> str:
    """Return an SQL formatted string from list of values.

    Parameters
    ----------
    values : Iterable
        Contains values to be formatted as a string for SQL. Only one type of
        value (str, int, float, datetime) should be used. First value in list
        is used to determine value type. Values are not checked for type
        consistency.

    Returns
    -------
    str
        An SQL formatted string from list of values.
    """
    # string
    if isinstance(values[0], str):
        return f"""({",".join([f"'{v}'" for v in values])})"""
    # int or float
    elif isinstance(values[0], int) or isinstance(values[0], float):
        return f"""({",".join([f"{v}" for v in values])})"""
    # datetime
    elif isinstance(values[0], datetime):
        return f"""({",".join(_get_datetime_list_string(values))})"""
    else:
        warnings.warn(
            "treating value as string because didn't know what else to do."
        )
        return f"""({",".join([f"'{str(v)}'" for v in values])})"""


def _format_filepath(
        filepath: Union[str, Path, List[Union[str, Path]]]
) -> str:
    if isinstance(filepath, str):
        return f"'{filepath}'"
    elif isinstance(filepath, Path):
        return f"'{str(filepath)}'"
    elif isinstance(filepath, list):
        return f"""[{",".join([f"'{str(fp)}'" for fp in filepath])}]"""


def _format_filter_item(
    filter: Union[tmq.JoinedFilter, tmq.TimeseriesFilter, tmqd.Filter]
) -> str:
    r"""Return an SQL formatted string for single filter object.

    Parameters
    ----------
    filter : models.\\*Filter
        A single \\*Filter object.

    Returns
    -------
    str
        An SQL formatted string for single filter object.
    """
    column = filter.column
    prepend_sf_list = [
        "value_time",
        "reference_time",
        "configuration",
        "measurement_unit",
        "variable"
    ]
    if column in prepend_sf_list:
        column = f"sf.{column}"

    if isinstance(filter.value, str):
        return f"""{column} {filter.operator} '{filter.value}'"""
    elif (
        isinstance(filter.value, int)
        or isinstance(filter.value, float)
    ):
        return f"""{column} {filter.operator} {filter.value}"""
    elif isinstance(filter.value, datetime):
        dt_str = filter.value.strftime(SQL_DATETIME_STR_FORMAT)
        return f"""{column} {filter.operator} '{dt_str}'"""
    elif (
        isinstance(filter.value, Iterable)
        and not isinstance(filter.value, str)
    ):
        value = _format_iterable_value(filter.value)
        return f"""{column} {filter.operator} {value}"""
    else:
        warnings.warn(
            "treating value as string because didn't know what else to do."
        )
        return f"""{column} {filter.operator} '{str(filter.value)}'"""


def filters_to_sql(
    filters: Union[List[tmq.JoinedFilter], List[tmqd.Filter]]
) -> List[str]:
    """Generate SQL where clause string from filters.

    Parameters
    ----------
    filters : Union[List[tmq.JoinedFilter], List[tmqd.Filter]]
        A list of Filter objects describing the filters.

    Returns
    -------
    str
        A where clause formatted string.
    """
    if len(filters) > 0:
        filter_strs = []
        for f in filters:
            filter_strs.append(_format_filter_item(f))
        qry = f"""WHERE {f" AND ".join(filter_strs)}"""
        return qry

    return "--no where clause"


def geometry_join_clause(
    q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for geometry."""
    if q.include_geometry:
        return f"""JOIN read_parquet({_format_filepath(q.geometry_filepath)}) gf
            on pf.location_id = gf.id
        """
    return ""


def geometry_select_clause(
    q: Union[tmq.MetricQuery,
             tmq.JoinedTimeseriesQuery,
             tmqd.MetricQuery,
             tmqd.JoinedTimeseriesQuery]
) -> str:
    """Generate the geometry select clause."""
    if q.include_geometry:
        return ",gf.geometry as geometry"
    return ""


def metrics_select_clause(
    q: Union[tmq.MetricQuery,
             tmq.JoinedTimeseriesQuery,
             tmqd.MetricQuery,
             tmqd.JoinedTimeseriesQuery]
) -> str:
    """Generate the metrics select clause."""
    if q.include_metrics == "all":
        return f""", {",".join([item.value for item in tmq.MetricEnum])}"""
    if isinstance(q.include_metrics, str):
        return f""", {tmq.MetricEnum[q.include_metrics].value}"""
    return f""", {",".join([ob for ob in q.include_metrics])}"""


def geometry_joined_select_clause(
        q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    """Generate the geometry select clause for a database."""
    if q.include_geometry:
        return ", geometry"
    return ""


def metric_geometry_join_clause_db(
    q: Union[tmqd.MetricQuery, tmqd.JoinedTimeseriesQuery]
) -> str:
    """Generate the metric geometry join clause for a database."""
    if q.include_geometry:
        return """JOIN geometry gf
            on primary_location_id = gf.id
        """
    return ""


def metric_geometry_join_clause(
    q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    """Generate the metric geometry join clause."""
    if q.include_geometry:
        return f"""JOIN read_parquet({_format_filepath(q.geometry_filepath)}) gf
            on primary_location_id = gf.id
        """
    return ""


def _remove_duplicates_jtq_cte(
    q: tmq.JoinedTimeseriesQuery
) -> str:
    """Generate the remove duplicates CTE for the JoinedTimeseriesQuery."""
    if q.remove_duplicates:
        qry = f"""
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
                {geometry_joined_select_clause(q)}
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
        qry = f"""
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
                {geometry_joined_select_clause(q)}
            FROM
                initial_joined
        """
    return qry


def _remove_duplicates_cte(
    remove_duplicates: bool
) -> str:
    """Generate the remove duplicates CTE for the various queries."""
    # TODO: This could replace _remove_duplicates_mq_cte?
    if remove_duplicates:
        qry = """
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
        """
    else:
        qry = """
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
            FROM
                initial_joined
        """
    return qry


def _remove_duplicates_mq_cte(
    q: tmq.MetricQuery
) -> str:
    """Generate the remove duplicates CTE for the MetricQuery."""
    if q.remove_duplicates:
        qry = """
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
        """
    else:
        qry = """
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
            FROM
                initial_joined
        """
    return qry


def _join_time_on(join: str, join_to: str, join_on: List[str]):
    """Generate the join time on query."""
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
        AND {join}.n = 1
    """
    return qry


def _join_on(join: str, join_to: str, join_on: List[str]) -> str:
    """Generate the join on query."""
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
    """
    return qry


def _nse_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the nash-sutcliffe-efficiency CTE."""
    if (
        "nash_sutcliffe_efficiency" in mq.include_metrics
        or "nash_sutcliffe_efficiency_normalized" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
        ,nse AS (
            SELECT
                {",".join(mq.group_by)}
                ,avg(primary_value) AS avg_primary_value
            FROM
                joined
            GROUP BY
                {",".join(mq.group_by)}
        )
        """
    return ""


# def _nse_log_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
#     """Generate the nash-sutcliffe-efficiency log CTE.

#     ToDo: Need to fix log of zero issue here.
#     """
#     if (
#         "nash_sutcliffe_efficiency_log" in mq.include_metrics
#         or mq.include_metrics == "all"
#     ):
#         return f"""
#         ,nse_log AS (
#             SELECT
#                 {",".join(mq.group_by)}
#                 ,avg(ln(primary_value)) AS avg_log_primary_value
#             FROM
#                 joined
#             GROUP BY
#                 {",".join(mq.group_by)}
#         )
#         """
#     return ""

def _spearman_ranks_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the spearman ranks CTE."""
    if (
        "spearman_correlation" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f""", spearman_ranked AS (
            SELECT
                primary_location_id
                , secondary_location_id
                , configuration
                , measurement_unit
                , variable_name
                , reference_time
                , value_time
                , AVG(primary_row_number) OVER (PARTITION BY {",".join(mq.group_by)}, primary_value) as primary_rank
                , AVG(secondary_row_number) OVER (PARTITION BY {",".join(mq.group_by)}, secondary_value) as secondary_rank
            FROM (
                SELECT
                    *
                    , ROW_NUMBER() OVER (PARTITION BY {",".join(mq.group_by)} ORDER BY primary_value) as primary_row_number
                    , ROW_NUMBER() OVER (PARTITION BY {",".join(mq.group_by)} ORDER BY secondary_value) as secondary_row_number
                FROM joined
            )
        )
        """
    return ""


def _join_spearman_ranks_cte(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the annual signature metrics CTE."""
    if (
        "spearman_correlation" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
            INNER JOIN spearman_ranked
                ON joined.primary_location_id = spearman_ranked.primary_location_id
                AND joined.secondary_location_id = spearman_ranked.secondary_location_id
                AND joined.configuration = spearman_ranked.configuration
                AND joined.measurement_unit = spearman_ranked.measurement_unit
                AND joined.variable_name = spearman_ranked.variable_name
                AND ifnull(joined.reference_time, '1') = ifnull(spearman_ranked.reference_time, '1')
                AND joined.value_time = spearman_ranked.value_time
        """
    return ""


def _select_spearman_correlation(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select spearman correlation query segment."""
    if (
        "spearman_correlation" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """
        , 1 - (6 * SUM(POWER(ABS(primary_rank - secondary_rank), 2))) / (POWER(COUNT(*), 3) - COUNT(*)) AS spearman_correlation"""
    return ""


def _annual_metrics_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the annual signature metrics CTE."""
    if (
        "annual_peak_relative_bias" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
        , annual_aggs AS (
            SELECT
                {",".join(mq.group_by)}
                , date_trunc('year', joined.value_time) as year
                , max(joined.primary_value) as primary_max_value
                , max(joined.secondary_value) as secondary_max_value
            FROM
                joined
            GROUP BY
                {",".join(mq.group_by)}
                , year
        )
        , annual_metrics AS (
            SELECT
                {",".join(mq.group_by)}
                , sum(
                    annual_aggs.secondary_max_value
                    - annual_aggs.primary_max_value
                ) / sum(annual_aggs.primary_max_value)
                AS annual_peak_relative_bias
            FROM
                annual_aggs
            GROUP BY
                {",".join(mq.group_by)}
        )
        """
    return ""


def _join_nse_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the join nash-sutcliffe-efficiency CTE."""
    if (
        "nash_sutcliffe_efficiency" in mq.include_metrics
        or "nash_sutcliffe_efficiency_normalized" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
            {_join_on(join="nse", join_to="joined", join_on=mq.group_by)}
        """
    return ""


def _join_annual_metrics_cte(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the annual signature metrics CTE."""
    if (
        "annual_peak_relative_bias" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
            {_join_on(join="annual_metrics", join_to="metrics", join_on=mq.group_by)}
        """
    return ""


# def _join_nse_log_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
#     """Generate the join nash-sutcliffe-efficiency log CTE."""
#     if (
#         "nash_sutcliffe_efficiency_log" in mq.include_metrics
#         or mq.include_metrics == "all"
#     ):
#         return f"""
#             {_join_on(join="nse_log", join_to="joined", join_on=mq.group_by)}
#         """
#     return ""


def _select_max_value_timedelta(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select max value timedelta query segment."""
    if (
        "max_value_timedelta" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, arg_max(
                joined.value_time,
                secondary_value ORDER BY joined.value_time ASC
            )
        - arg_max(
            joined.value_time,
            primary_value ORDER BY joined.value_time ASC
            ) as max_value_timedelta"""
    return ""


def _select_secondary_max_value_time(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary max value time query segment."""
    if (
        "secondary_max_value_time" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, arg_max(
            joined.value_time,
            secondary_value ORDER BY joined.value_time ASC
        ) as secondary_max_value_time"""
    return ""


def _select_primary_max_value_time(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary max value time query segment."""
    if (
        "primary_max_value_time" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, arg_max(
            joined.value_time,
            primary_value ORDER BY joined.value_time ASC
            ) as primary_max_value_time"""
    return ""


def _select_root_mean_squared_error(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select root mean squared error query segment."""
    if (
        "root_mean_squared_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sqrt(sum(power(absolute_difference, 2))/count(*))
            as root_mean_squared_error
        """
    return ""


def _select_mean_squared_error(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select mean squared error query segment."""
    if (
        "mean_squared_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(power(absolute_difference, 2))/count(*)
            as mean_squared_error
        """
    return ""


def _select_mean_absolute_error(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select mean absolute error query segment."""
    if (
        "mean_absolute_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(absolute_difference)/count(*) as mean_absolute_error"""
    return ""


def _select_mean_absolute_relative_error(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select mean absolute relative error query segment."""
    if (
        "mean_absolute_relative_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(absolute_difference)/sum(primary_value) as mean_absolute_relative_error""" # noqa E501
    return ""


def _select_kling_gupta_efficiency(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select kling gupta efficiency query segment."""
    if (
        "kling_gupta_efficiency" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, 1 - sqrt(
            pow(corr(secondary_value, primary_value) - 1, 2)
            + pow(stddev(secondary_value) / stddev(primary_value) - 1, 2)
            + pow(avg(secondary_value) / avg(primary_value) - 1, 2)
        ) as kling_gupta_efficiency
        """
    return ""


def _select_kling_gupta_efficiency_mod1(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select kling gupta efficiency mod1 query segment."""
    if (
        "kling_gupta_efficiency_mod1" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, 1 - sqrt(
            pow(corr(secondary_value, primary_value) - 1, 2)
            + pow((stddev_pop(secondary_value) / avg(secondary_value)) / (stddev_pop(primary_value) / avg(primary_value)) - 1, 2)
            + pow(avg(secondary_value) / avg(primary_value) - 1, 2)
        ) as kling_gupta_efficiency_mod1
        """
    return ""


def _select_kling_gupta_efficiency_mod2(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select kling gupta efficiency mod2 query segment."""
    if (
        "kling_gupta_efficiency_mod2" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, 1 - sqrt(
            pow(corr(secondary_value, primary_value) - 1, 2)
            + pow(stddev_pop(secondary_value)
                / stddev_pop(primary_value) - 1, 2)
            + pow(avg(secondary_value) - avg(primary_value), 2)
                / pow(stddev_pop(primary_value), 2)
        ) as kling_gupta_efficiency_mod2
        """
    return ""


def _select_nash_sutcliffe_efficiency(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select nash sutcliffe efficiency query segment."""
    if (
        "nash_sutcliffe_efficiency" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, 1 - (
            sum(pow(joined.primary_value - joined.secondary_value, 2))
            / sum(pow(joined.primary_value - nse.avg_primary_value, 2))
        ) as nash_sutcliffe_efficiency
        """
    return ""


def _select_nash_sutcliffe_efficiency_normalized(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select nash sutcliffe efficiency query segment."""
    if (
        "nash_sutcliffe_efficiency_normalized" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, 1/(2-(1 - (
            sum(pow(joined.primary_value - joined.secondary_value, 2))
            / sum(pow(joined.primary_value - nse.avg_primary_value, 2))
        ))) as nash_sutcliffe_efficiency_normalized
        """
    return ""


# def _select_nash_sutcliffe_efficiency_log(
#     mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
# ) -> str:
#     """Generate the select nash sutcliffe efficiency log query segment.

#     ToDo: Need to fix log of zero issue here.
#     """
#     if (
#         "nash_sutcliffe_efficiency_log" in mq.include_metrics
#         or mq.include_metrics == "all"
#     ):
#         return """, 1 - (
#             sum(pow(ln(joined.primary_value)
#             - ln(joined.secondary_value), 2))
#             / sum(pow(ln(joined.primary_value)
#             - nse_log.avg_log_primary_value, 2))
#         ) as nash_sutcliffe_efficiency_log
#         """
#     return ""


def _select_mean_error(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select mean_error query segment."""
    if "mean_error" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(secondary_value - primary_value)/count(*) as mean_error""" # noqa E501
    return ""


def _select_relative_bias(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select relative bias query segment."""
    if "relative_bias" in mq.include_metrics or mq.include_metrics == "all":
        return ", sum(secondary_value - primary_value) / sum(primary_value) AS relative_bias" # noqa E501
    return ""


def _select_multiplicative_bias(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select multiplicative bias query segment."""
    if (
        "multiplicative_bias" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return ", mean(secondary_value) / mean(primary_value) AS multiplicative_bias" # noqa E501
    return ""


def _select_pearson_correlation(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select pearson correlation query segment."""
    if (
        "pearson_correlation" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return ", corr(secondary_value, primary_value) AS pearson_correlation"
    return ""


def _select_r_squared(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select r squared query segment."""
    if "r_squared" in mq.include_metrics or mq.include_metrics == "all":
        return """, pow(corr(secondary_value, primary_value), 2) as r_squared""" # noqa E501
    return ""


def _select_max_value_delta(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select max value delta query segment."""
    if "max_value_delta" in mq.include_metrics or mq.include_metrics == "all":
        return """, max(secondary_value) - max(primary_value)
            as max_value_delta
        """
    return ""


def _select_primary_count(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select primary count query segment."""
    if "primary_count" in mq.include_metrics or mq.include_metrics == "all":
        return """, count(primary_value) as primary_count"""
    return ""


def _select_secondary_count(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary count query segment."""
    if "secondary_count" in mq.include_metrics or mq.include_metrics == "all":
        return """, count(secondary_value) as secondary_count"""
    return ""


def _select_primary_minimum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary minimum query segment."""
    if "primary_minimum" in mq.include_metrics or mq.include_metrics == "all":
        return """, min(primary_value) as primary_minimum"""
    return ""


def _select_secondary_minimum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary minimum query segment."""
    if (
        "secondary_minimum" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, min(secondary_value) as secondary_minimum"""
    return ""


def _select_primary_maximum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary maximum query segment."""
    if "primary_maximum" in mq.include_metrics or mq.include_metrics == "all":
        return """, max(primary_value) as primary_maximum"""
    return ""


def _select_secondary_maximum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary maximum query segment."""
    if (
        "secondary_maximum" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, max(secondary_value) as secondary_maximum"""
    return ""


def _select_primary_average(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary average query segment."""
    if "primary_average" in mq.include_metrics or mq.include_metrics == "all":
        return """, avg(primary_value) as primary_average"""
    return ""


def _select_secondary_average(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary average query segment."""
    if (
        "secondary_average" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, avg(secondary_value) as secondary_average"""
    return ""


def _select_primary_sum(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select primary sum query segment."""
    if "primary_sum" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(primary_value) as primary_sum"""
    return ""


def _select_secondary_sum(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select secondary sum query segment."""
    if "secondary_sum" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(secondary_value) as secondary_sum"""
    return ""


def _select_primary_variance(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary variance query segment."""
    if "primary_variance" in mq.include_metrics or mq.include_metrics == "all":
        return """, var_pop(primary_value) as primary_variance"""
    return ""


def _select_secondary_variance(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary variance query segment."""
    if (
        "secondary_variance" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, var_pop(secondary_value) as secondary_variance"""
    return ""


def df_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert pd.DataFrame to gpd.GeoDataFrame.

    When the `geometry` column is read from a parquet file using DuckBD
    it is a bytearray in the resulting pd.DataFrame.  The `geometry` needs
    to be convert to bytes before GeoPandas can work with it.  This function
    does that.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a `geometry` column that has geometry stored as
        a bytearray.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with a valid `geometry` column.
    """
    df["geometry"] = gpd.GeoSeries.from_wkb(
        df["geometry"].apply(lambda x: bytes(x))
    )
    return gpd.GeoDataFrame(df, crs="EPSG:4326", geometry="geometry")


def remove_empty_lines(text: str) -> str:
    """Remove empty lines from string."""
    return "".join([s for s in text.splitlines(True) if s.strip()])


# ====================== EXPERIMENTAL FUNCTIONS BELOW ========================
def get_the_metrics_calculation_clause(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery],
    join_geometry_clause: str
) -> str:
    """Generate the metrics calculation clause."""
    return f"""
        {_nse_cte(mq)}
        {_annual_metrics_cte(mq)}
        {_spearman_ranks_cte(mq)}
        , metrics AS (
            SELECT
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
                {_select_primary_count(mq)}
                {_select_secondary_count(mq)}
                {_select_primary_minimum(mq)}
                {_select_secondary_minimum(mq)}
                {_select_primary_maximum(mq)}
                {_select_secondary_maximum(mq)}
                {_select_primary_average(mq)}
                {_select_secondary_average(mq)}
                {_select_primary_sum(mq)}
                {_select_secondary_sum(mq)}
                {_select_primary_variance(mq)}
                {_select_secondary_variance(mq)}
                {_select_max_value_delta(mq)}
                {_select_mean_error(mq)}
                {_select_nash_sutcliffe_efficiency(mq)}
                {_select_nash_sutcliffe_efficiency_normalized(mq)}
                {_select_kling_gupta_efficiency(mq)}
                {_select_kling_gupta_efficiency_mod1(mq)}
                {_select_kling_gupta_efficiency_mod2(mq)}
                {_select_mean_absolute_error(mq)}
                {_select_mean_squared_error(mq)}
                {_select_root_mean_squared_error(mq)}
                {_select_primary_max_value_time(mq)}
                {_select_secondary_max_value_time(mq)}
                {_select_max_value_timedelta(mq)}
                {_select_relative_bias(mq)}
                {_select_multiplicative_bias(mq)}
                {_select_mean_absolute_relative_error(mq)}
                {_select_pearson_correlation(mq)}
                {_select_r_squared(mq)}
                {_select_spearman_correlation(mq)}
            FROM
                joined
                {_join_nse_cte(mq)}
                {_join_spearman_ranks_cte(mq)}
            GROUP BY
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
        )
        SELECT
            {",".join([f"metrics.{ob}" for ob in mq.group_by])}
            {metrics_select_clause(mq)}
            {geometry_select_clause(mq)}
        FROM metrics
            {join_geometry_clause}
            {_join_annual_metrics_cte(mq)}
        ORDER BY
            {",".join([f"metrics.{ob}" for ob in mq.order_by])}
    ;"""


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
            {filters_to_sql(mq.filters)}
        )
    """
