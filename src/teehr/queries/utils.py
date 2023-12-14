import pandas as pd
import geopandas as gpd
import warnings

from collections.abc import Iterable
from datetime import datetime
from typing import List, Union

import teehr.models.queries as tmq
import teehr.models.queries_database as tmqd

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def _get_datetime_list_string(values):
    return [f"'{v.strftime(SQL_DATETIME_STR_FORMAT)}'" for v in values]


def _format_iterable_value(
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


def _format_filter_item(
    filter: Union[tmq.JoinedFilter, tmq.TimeseriesFilter, tmqd.Filter]
) -> str:
    """Returns an SQL formatted string for single filter object.

    Parameters
    ----------
    filter: models.*Filter
        A single *Filter object.

    Returns
    -------
    formatted_string : str

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
            filter_strs.append(_format_filter_item(f))
        qry = f"""WHERE {f" AND ".join(filter_strs)}"""
        return qry

    return "--no where clause"


def geometry_join_clause(
    q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for"""
    if q.include_geometry:
        return f"""JOIN read_parquet('{str(q.geometry_filepath)}') gf
            on pf.location_id = gf.id
        """
    return ""


def geometry_select_clause(
    q: Union[tmq.MetricQuery,
             tmq.JoinedTimeseriesQuery,
             tmqd.MetricQuery,
             tmqd.JoinedTimeseriesQuery]
) -> str:
    if q.include_geometry:
        return ",gf.geometry as geometry"
    return ""


def geometry_joined_select_clause(
        q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    if q.include_geometry:
        return ", geometry"
    return ""


def metric_geometry_join_clause_db(
    q: Union[tmqd.MetricQuery, tmqd.JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for"""
    if q.include_geometry:
        return """JOIN geometry gf
            on primary_location_id = gf.id
        """
    return ""


def metric_geometry_join_clause(
    q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for"""
    if q.include_geometry:
        return f"""JOIN read_parquet('{str(q.geometry_filepath)}') gf
            on primary_location_id = gf.id
        """
    return ""


def _remove_duplicates_jtq_cte(
    q: tmq.JoinedTimeseriesQuery
) -> str:
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


def _remove_duplicates_mq_cte(
    q: tmq.MetricQuery
) -> str:
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
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
        AND {join}.n = 1
    """
    return qry


def _join_on(join: str, join_to: str, join_on: List[str]) -> str:
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
    """
    return qry


def _nse_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if (
        "nash_sutcliffe_efficiency" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
        ,nse AS (
            SELECT
                {",".join(mq.group_by)}
                ,AVG(primary_value) AS avg_primary_value
            FROM
                joined
            GROUP BY
                {",".join(mq.group_by)}
        )
        """
    return ""


def _join_nse_cte(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if (
        "nash_sutcliffe_efficiency" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return f"""
            {_join_on(join="nse", join_to="joined", join_on=mq.group_by)}
        """
    return ""


def _select_max_value_timedelta(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
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
    if (
        "mean_squared_error" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, sum(power(absolute_difference, 2))/count(*)
            as mean_squared_error
        """
    return ""


def _select_mean_error(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if "mean_error" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(absolute_difference)/count(*) as mean_error"""
    return ""


def _select_kling_gupta_efficiency(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if (
        "kling_gupta_efficiency" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, 1 - sqrt(
            pow(corr(secondary_value, primary_value) - 1, 2)
            + pow(stddev(secondary_value)
                / stddev(primary_value) - 1, 2)
            + pow(avg(secondary_value) / avg(primary_value) - 1, 2)
        ) as kling_gupta_efficiency
        """
    return ""


def _select_nash_sutcliffe_efficiency(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
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


def _select_bias(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if "bias" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(secondary_value - primary_value)/count(*) as bias"""
    return ""


def _select_max_value_delta(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if "max_value_delta" in mq.include_metrics or mq.include_metrics == "all":
        return """, max(secondary_value) - max(primary_value)
            as max_value_delta
        """
    return ""


def _select_primary_count(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if "primary_count" in mq.include_metrics or mq.include_metrics == "all":
        return """, count(primary_value) as primary_count"""
    return ""


def _select_secondary_count(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if "secondary_count" in mq.include_metrics or mq.include_metrics == "all":
        return """, count(secondary_value) as secondary_count"""
    return ""


def _select_primary_minimum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if "primary_minimum" in mq.include_metrics or mq.include_metrics == "all":
        return """, min(primary_value) as primary_minimum"""
    return ""


def _select_secondary_minimum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if (
        "secondary_minimum" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, min(secondary_value) as secondary_minimum"""
    return ""


def _select_primary_maximum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if "primary_maximum" in mq.include_metrics or mq.include_metrics == "all":
        return """, max(primary_value) as primary_maximum"""
    return ""


def _select_secondary_maximum(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if (
        "secondary_maximum" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, max(secondary_value) as secondary_maximum"""
    return ""


def _select_primary_average(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if "primary_average" in mq.include_metrics or mq.include_metrics == "all":
        return """, avg(primary_value) as primary_average"""
    return ""


def _select_secondary_average(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if (
        "secondary_average" in mq.include_metrics
        or mq.include_metrics == "all"
    ):
        return """, avg(secondary_value) as secondary_average"""
    return ""


def _select_primary_sum(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if "primary_sum" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(primary_value) as primary_sum"""
    return ""


def _select_secondary_sum(mq: Union[tmq.MetricQuery, tmqd.MetricQuery]) -> str:
    if "secondary_sum" in mq.include_metrics or mq.include_metrics == "all":
        return """, sum(secondary_value) as secondary_sum"""
    return ""


def _select_primary_variance(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
    if "primary_variance" in mq.include_metrics or mq.include_metrics == "all":
        return """, var_pop(primary_value) as primary_variance"""
    return ""


def _select_secondary_variance(
    mq: Union[tmq.MetricQuery, tmqd.MetricQuery]
) -> str:
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
    gdf : gpd.GeoDataFrame
        GeoDataFrame with a valid `geometry` column.

    """
    df["geometry"] = gpd.GeoSeries.from_wkb(
        df["geometry"].apply(lambda x: bytes(x))
    )
    return gpd.GeoDataFrame(df, crs="EPSG:4326", geometry="geometry")


def remove_empty_lines(text: str) -> str:
    """Remove empty lines from string."""
    return "".join([s for s in text.splitlines(True) if s.strip()])
