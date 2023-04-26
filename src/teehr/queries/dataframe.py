import warnings
import duckdb
import numpy as np

import pandas as pd
import geopandas as gpd
import dask.dataframe as dd

from hydrotools.metrics import metrics as hm

from collections.abc import Iterable
from datetime import datetime
from typing import List, Union

import teehr.models.queries as tmq
import teehr.queries.duckdb as tqd

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_metrics(
    primary_filepath: str,
    secondary_filepath: str,
    crosswalk_filepath: str,
    group_by: List[str],
    order_by: List[str],
    filters: Union[List[dict], None] = None,
    return_query: bool = True,
    geometry_filepath: Union[str, None] = None,
    include_geometry: bool = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    """Calculate performance metrics using a Pandas or Dask DataFrame.

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

    mq = tmq.MetricQuery.parse_obj(
        {
            "primary_filepath": primary_filepath,
            "secondary_filepath": secondary_filepath,
            "crosswalk_filepath": crosswalk_filepath,
            "group_by": group_by,
            "order_by": order_by,
            "filters": filters,
            "return_query": return_query,
            "include_geometry": include_geometry,
            "geometry_filepath": geometry_filepath
        }
    )

    if mq.return_query:
        raise ValueError(
            "`return query` is not a valid option for `dask.get_metrics()`."
        )

    if mq.include_geometry:
        if "geometry" not in mq.group_by:
            mq.group_by.append("geometry")

    df = tqd.get_joined_timeseries(
        primary_filepath=mq.primary_filepath,
        secondary_filepath=mq.secondary_filepath,
        crosswalk_filepath=mq.crosswalk_filepath,
        order_by=mq.order_by,
        filters=mq.filters,
        return_query=False,
        include_geometry=mq.include_geometry,
        geometry_filepath=mq.geometry_filepath
    )

    grouped = df.groupby(mq.group_by, as_index=False)

    calculated_metrics = grouped.apply(
        calculate_metrics_on_groups,
        metrics="all"
    )

    # ddf = dd.from_pandas(df, npartitions=4)

    # calculated_metrics = ddf.groupby(mq.group_by).apply(
    #     calculate_metrics_on_groups,
    #     metrics=["primary_count"],
    #     meta={"primary_count": "int"}
    # ).compute()

    return calculated_metrics


def calculate_metrics_on_groups(
        group: pd.DataFrame,
        metrics: Union[List[str], str] = "all"
):
    """Calculate metrics on a pd.DataFrame

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
    max(secondary_value) - max(primary_value) as max_period_delta,
    sum(primary_value - secondary_value)/count(*) as bias
    var_pop(secondary_value) as secondary_variance,
    var_pop(primary_value) as primary_variance,

    regr_intercept(secondary_value, primary_value) as intercept,
    covar_pop(secondary_value, primary_value) as covariance,
    corr(secondary_value, primary_value) as corr,
    regr_r2(secondary_value, primary_value) as r_squared,
    """
    data = {}

    # Simple Metrics
    if metrics == "all" or "primary_count" in metrics:
        data["primary_count"] = len(group["primary_value"])

    if metrics == "all" or "secondary_count" in metrics:
        data["secondary_count"] = len(group["secondary_value"])

    if metrics == "all" or "primary_min" in metrics:
        data["primary_min"] = np.min(group["primary_value"])

    if metrics == "all" or "secondary_min" in metrics:
        data["secondary_min"] = np.min(group["secondary_value"])

    if metrics == "all" or "primary_max" in metrics:
        data["primary_max"] = np.max(group["primary_value"])

    if metrics == "all" or "secondary_max" in metrics:
        data["secondary_max"] = np.max(group["secondary_value"])

    if metrics == "all" or "primary_ave" in metrics:
        data["primary_ave"] = np.mean(group["primary_value"])

    if metrics == "all" or "secondary_ave" in metrics:
        data["secondary_ave"] = np.mean(group["secondary_value"])

    if metrics == "all" or "primary_sum" in metrics:
        data["primary_sum"] = np.sum(group["primary_value"])

    if metrics == "all" or "secondary_sum" in metrics:
        data["secondary_sum"] = np.sum(group["secondary_value"])

    if metrics == "all" or "primary_variance" in metrics:
        data["primary_variance"] = np.var(group["primary_value"])

    if metrics == "all" or "secondary_variance" in metrics:
        data["secondary_variance"] = np.var(group["secondary_value"])

    if metrics == "all" or "bias" in metrics:
        group["difference"] = group["primary_value"] - group["secondary_value"]
        data["bias"] = np.sum(group["difference"])/len(group)

    if metrics == "all" or "delta_max" in metrics:
        data["delta_max"] = (
            np.max(group["secondary_value"])
            - np.max(group["primary_value"])
        )

    # HydroTools Forecast Metrics
    if metrics == "all" or "nash_sutcliffe_efficiency" in metrics:
        nse = hm.nash_sutcliffe_efficiency(
            group["primary_value"],
            group["secondary_value"]
        )
        data["nash_sutcliffe_efficiency"] = nse

    if metrics == "all" or "kling_gupta_efficiency(" in metrics:
        kge = hm.kling_gupta_efficiency(
            group["primary_value"],
            group["secondary_value"]
        )
        data["kling_gupta_efficiency"] = kge



    return pd.Series(data)
