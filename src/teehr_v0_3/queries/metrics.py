"""Contains functions for metric queries.

The functions herein are use to support:

def get_metrics_clause(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery],
    join_geometry_clause: str
) -> str:

"""
import teehr_v0_3.models.queries as tmq
import teehr_v0_3.models.queries_database as tmqd
import teehr_v0_3.queries.metrics as tqm
import teehr_v0_3.queries.utils as tqu
from typing import Union


def nse_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
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


# def nse_log_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
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


def spearman_ranks_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
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
                , AVG(primary_row_number) OVER (
                    PARTITION BY {",".join(mq.group_by)}, primary_value
                ) as primary_rank
                , AVG(secondary_row_number) OVER (
                    PARTITION BY {",".join(mq.group_by)}, secondary_value
                ) as secondary_rank
            FROM (
                SELECT
                    *
                    , ROW_NUMBER() OVER (PARTITION BY {",".join(mq.group_by)}
                        ORDER BY primary_value) as primary_row_number
                    , ROW_NUMBER() OVER (PARTITION BY {",".join(mq.group_by)}
                        ORDER BY secondary_value) as secondary_row_number
                FROM joined
            )
        )
        """
    return ""


def join_spearman_ranks_cte(
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


def select_spearman_correlation(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select spearman correlation query segment."""
    if (
        "spearman_correlation" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """
        , 1 - (6 * SUM(POWER(ABS(primary_rank - secondary_rank), 2))) / (POWER(COUNT(*), 3) - COUNT(*)) AS spearman_correlation"""
    return ""


def annual_metrics_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
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


def join_nse_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the join nash-sutcliffe-efficiency CTE."""
    if (
        "nash_sutcliffe_efficiency" in mq.include_metrics
        or "nash_sutcliffe_efficiency_normalized" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
            {tqu.join_on(join="nse", join_to="joined", join_on=mq.group_by)}
        """
    return ""


def join_annual_metrics_cte(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the annual signature metrics CTE."""
    if (
        "annual_peak_relative_bias" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
            {tqu.join_on(join="annual_metrics", join_to="metrics", join_on=mq.group_by)}
        """
    return ""


# def join_nse_log_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
#     """Generate the join nash-sutcliffe-efficiency log CTE."""
#     if (
#         "nash_sutcliffe_efficiency_log" in mq.include_metrics
#         or mq.include_metrics == "all"
#     ):
#         return f"""
#             {tqu.join_on(join="nse_log", join_to="joined", join_on=mq.group_by)}
#         """
#     return ""


def select_max_value_timedelta(
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


def select_secondary_max_value_time(
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


def select_primary_max_value_time(
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


def select_root_mean_squared_error(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select root mean squared error query segment."""
    if (
        "root_mean_squared_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sqrt(sum(power(abs(primary_value - secondary_value), 2))/count(*))
            as root_mean_squared_error
        """
    return ""


def select_mean_squared_error(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select mean squared error query segment."""
    if (
        "mean_squared_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(power(abs(primary_value - secondary_value), 2))/count(*)
            as mean_squared_error
        """
    return ""


def select_mean_absolute_error(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select mean absolute error query segment."""
    if (
        "mean_absolute_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(abs(primary_value - secondary_value))/count(*) as mean_absolute_error"""
    return ""


def select_mean_absolute_relative_error(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select mean absolute relative error query segment."""
    if (
        "mean_absolute_relative_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(abs(primary_value - secondary_value))/sum(primary_value) as mean_absolute_relative_error""" # noqa E501
    return ""


def select_kling_gupta_efficiency(
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


def select_kling_gupta_efficiency_mod1(
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


def select_kling_gupta_efficiency_mod2(
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


def select_nash_sutcliffe_efficiency(
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


def select_nash_sutcliffe_efficiency_normalized(
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


# def select_nash_sutcliffe_efficiency_log(
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


def select_mean_error(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select mean_error query segment."""
    if "mean_error" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(secondary_value - primary_value)/count(*) as mean_error""" # noqa E501
    return ""


def select_relative_bias(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select relative bias query segment."""
    if "relative_bias" in mq.include_metrics or mq.include_metrics == "all":
        return ", sum(secondary_value - primary_value) / sum(primary_value) AS relative_bias" # noqa E501
    return ""


def select_multiplicative_bias(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select multiplicative bias query segment."""
    if (
        "multiplicative_bias" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return ", mean(secondary_value) / mean(primary_value) AS multiplicative_bias" # noqa E501
    return ""


def select_pearson_correlation(
        mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select pearson correlation query segment."""
    if (
        "pearson_correlation" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return ", corr(secondary_value, primary_value) AS pearson_correlation"
    return ""


def select_r_squared(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select r squared query segment."""
    if "r_squared" in mq.include_metrics or mq.include_metrics == "all":
        return """, pow(corr(secondary_value, primary_value), 2) as r_squared""" # noqa E501
    return ""


def select_max_value_delta(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select max value delta query segment."""
    if "max_value_delta" in mq.include_metrics or mq.include_metrics == "all":
        return """, max(secondary_value) - max(primary_value)
            as max_value_delta
        """
    return ""


def select_primary_count(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select primary count query segment."""
    if "primary_count" in mq.include_metrics or mq.include_metrics == "all":
        return """, count(primary_value) as primary_count"""
    return ""


def select_secondary_count(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary count query segment."""
    if "secondary_count" in mq.include_metrics or mq.include_metrics == "all":
        return """, count(secondary_value) as secondary_count"""
    return ""


def select_primary_minimum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary minimum query segment."""
    if "primary_minimum" in mq.include_metrics or mq.include_metrics == "all":
        return """, min(primary_value) as primary_minimum"""
    return ""


def select_secondary_minimum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary minimum query segment."""
    if (
        "secondary_minimum" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, min(secondary_value) as secondary_minimum"""
    return ""


def select_primary_maximum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary maximum query segment."""
    if "primary_maximum" in mq.include_metrics or mq.include_metrics == "all":
        return """, max(primary_value) as primary_maximum"""
    return ""


def select_secondary_maximum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary maximum query segment."""
    if (
        "secondary_maximum" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, max(secondary_value) as secondary_maximum"""
    return ""


def select_primary_average(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary average query segment."""
    if "primary_average" in mq.include_metrics or mq.include_metrics == "all":
        return """, avg(primary_value) as primary_average"""
    return ""


def select_secondary_average(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary average query segment."""
    if (
        "secondary_average" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, avg(secondary_value) as secondary_average"""
    return ""


def select_primary_sum(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select primary sum query segment."""
    if "primary_sum" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(primary_value) as primary_sum"""
    return ""


def select_secondary_sum(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    """Generate the select secondary sum query segment."""
    if "secondary_sum" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(secondary_value) as secondary_sum"""
    return ""


def select_primary_variance(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select primary variance query segment."""
    if "primary_variance" in mq.include_metrics or mq.include_metrics == "all":
        return """, var_pop(primary_value) as primary_variance"""
    return ""


def select_secondary_variance(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    """Generate the select secondary variance query segment."""
    if (
        "secondary_variance" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, var_pop(secondary_value) as secondary_variance"""
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


def get_metrics_clause(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery],
    join_geometry_clause: str
) -> str:
    """Generate the metrics calculation clause."""
    return f"""
        {tqm.nse_cte(mq)}
        {tqm.annual_metrics_cte(mq)}
        {tqm.spearman_ranks_cte(mq)}
        , metrics AS (
            SELECT
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
                {tqm.select_primary_count(mq)}
                {tqm.select_secondary_count(mq)}
                {tqm.select_primary_minimum(mq)}
                {tqm.select_secondary_minimum(mq)}
                {tqm.select_primary_maximum(mq)}
                {tqm.select_secondary_maximum(mq)}
                {tqm.select_primary_average(mq)}
                {tqm.select_secondary_average(mq)}
                {tqm.select_primary_sum(mq)}
                {tqm.select_secondary_sum(mq)}
                {tqm.select_primary_variance(mq)}
                {tqm.select_secondary_variance(mq)}
                {tqm.select_max_value_delta(mq)}
                {tqm.select_mean_error(mq)}
                {tqm.select_nash_sutcliffe_efficiency(mq)}
                {tqm.select_nash_sutcliffe_efficiency_normalized(mq)}
                {tqm.select_kling_gupta_efficiency(mq)}
                {tqm.select_kling_gupta_efficiency_mod1(mq)}
                {tqm.select_kling_gupta_efficiency_mod2(mq)}
                {tqm.select_mean_absolute_error(mq)}
                {tqm.select_mean_squared_error(mq)}
                {tqm.select_root_mean_squared_error(mq)}
                {tqm.select_primary_max_value_time(mq)}
                {tqm.select_secondary_max_value_time(mq)}
                {tqm.select_max_value_timedelta(mq)}
                {tqm.select_relative_bias(mq)}
                {tqm.select_multiplicative_bias(mq)}
                {tqm.select_mean_absolute_relative_error(mq)}
                {tqm.select_pearson_correlation(mq)}
                {tqm.select_r_squared(mq)}
                {tqm.select_spearman_correlation(mq)}
            FROM
                joined
                {tqm.join_nse_cte(mq)}
                {tqm.join_spearman_ranks_cte(mq)}
            GROUP BY
                {",".join([f"joined.{gb}" for gb in mq.group_by])}
        )
        SELECT
            {",".join([f"metrics.{ob}" for ob in mq.group_by])}
            {tqm.metrics_select_clause(mq)}
            {tqu.geometry_select_clause(mq)}
        FROM metrics
            {tqu.geometry_join_clause(mq, join_geometry_clause)}
            {tqm.join_annual_metrics_cte(mq)}
        ORDER BY
            {",".join([f"metrics.{ob}" for ob in mq.order_by])}
    ;"""