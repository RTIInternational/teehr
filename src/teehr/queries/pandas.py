"""A module for defining SQL queries against Pandas DataFrames."""
import numpy as np
import pandas as pd
import geopandas as gpd
# import dask.dataframe as dd

from hydrotools.metrics import metrics as hm

from typing import List, Union

import teehr.models.queries as tmq
import teehr.queries.duckdb as tqu


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
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
    r"""Calculate performance metrics using a Pandas or Dask DataFrame.

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
        List of metrics (see below) for allowable list, or "all" to return all.
    filters : Union[List[dict], None] = None
        List of dictionaries describing the "where" clause to limit data that
        is included in metrics.
    return_query : bool = False
        True returns the query string instead of the data.
    include_geometry : bool = True
        True joins the geometry to the query results.
        Only works if `primary_location_id`
        is included as a group_by field.

    Returns
    -------
    Union[str, pd.DataFrame, gpd.GeoDataFrame]
        The query string or a DataFrame or GeoDataFrame of query results.

    Notes
    -----
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

    """
    mq = tmq.MetricQuery.model_validate(
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

    if mq.return_query:
        raise ValueError(
            "`return query` is not a valid option "
            "for `dataframe.get_metrics()`."
        )

    # This loads all the timeseries in memory
    df = tqu.get_joined_timeseries(
        primary_filepath=mq.primary_filepath,
        secondary_filepath=mq.secondary_filepath,
        crosswalk_filepath=mq.crosswalk_filepath,
        order_by=mq.order_by,
        filters=mq.filters,
        return_query=False,
    )

    # Pandas DataFrame GroupBy approach (works).
    grouped = df.groupby(mq.group_by, as_index=False, observed=False)

    calculated_metrics = grouped.apply(
        calculate_group_metrics,
        include_metrics=include_metrics
    )

    # Dask DataFrame GroupBy approach (does not work).
    # ddf = dd.from_pandas(df, npartitions=4)
    # calculated_metrics = ddf.groupby(mq.group_by).apply(
    #     calculate_metrics_on_groups,
    #     metrics=["primary_count"],
    #     meta={"primary_count": "int"}
    # ).compute()

    if mq.include_geometry:
        gdf = gpd.read_parquet(mq.geometry_filepath)
        merged_gdf = gdf.merge(
            calculated_metrics,
            left_on="id",
            right_on="primary_location_id"
        )
        return merged_gdf

    return calculated_metrics


def calculate_group_metrics(
        group: pd.DataFrame,
        include_metrics: Union[List[str], str]
):
    """Calculate metrics on a pd.DataFrame.

    Parameters
    ----------
    group : pd.DataFrame
        Represents a population group to calculate the metrics on.
    include_metrics : List[str]
        List of metrics (see below) for allowable list, or "all" to
        return all.

    Returns
    -------
    pd.DataFrame
        A DataFrame of calculated metrics.

    Notes
    -----
    This approach to calculating metrics is not as fast as
    `teehr.queries.duckdb.get_metrics()` but is easier to update
    and contains more metrics.  It also serves as the reference
    implementation for the duckdb queries.

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
    """
    data = {}

    # Simple Metrics
    if include_metrics == "all" or "primary_count" in include_metrics:
        data["primary_count"] = len(group["primary_value"])

    if include_metrics == "all" or "secondary_count" in include_metrics:
        data["secondary_count"] = len(group["secondary_value"])

    if include_metrics == "all" or "primary_minimum" in include_metrics:
        data["primary_minimum"] = np.min(group["primary_value"])

    if include_metrics == "all" or "secondary_minimum" in include_metrics:
        data["secondary_minimum"] = np.min(group["secondary_value"])

    if include_metrics == "all" or "primary_maximum" in include_metrics:
        data["primary_maximum"] = np.max(group["primary_value"])

    if include_metrics == "all" or "secondary_maximum" in include_metrics:
        data["secondary_maximum"] = np.max(group["secondary_value"])

    if include_metrics == "all" or "primary_average" in include_metrics:
        data["primary_average"] = np.mean(group["primary_value"])

    if include_metrics == "all" or "secondary_average" in include_metrics:
        data["secondary_average"] = np.mean(group["secondary_value"])

    if include_metrics == "all" or "primary_sum" in include_metrics:
        data["primary_sum"] = np.sum(group["primary_value"])

    if include_metrics == "all" or "secondary_sum" in include_metrics:
        data["secondary_sum"] = np.sum(group["secondary_value"])

    if include_metrics == "all" or "primary_variance" in include_metrics:
        data["primary_variance"] = np.var(group["primary_value"])

    if include_metrics == "all" or "secondary_variance" in include_metrics:
        data["secondary_variance"] = np.var(group["secondary_value"])

    if include_metrics == "all" or "mean_error" in include_metrics:
        group["difference"] = group["secondary_value"] - group["primary_value"]
        data["mean_error"] = np.sum(group["difference"])/len(group)

    if include_metrics == "all" or "relative_bias" in include_metrics:
        group["difference"] = group["secondary_value"] - group["primary_value"]
        data["relative_bias"] = (
            np.sum(group["difference"])/np.sum(group["primary_value"])
        )

    if (
        include_metrics == "all"
        or "mean_absolute_relative_error" in include_metrics
    ):
        group["absolute_difference"] = (
            np.abs(group["secondary_value"] - group["primary_value"])
        )
        data["mean_absolute_relative_error"] = (
            (
                np.sum(group["absolute_difference"])
                / np.sum(group["primary_value"])
            )
        )

    if include_metrics == "all" or "multiplicative_bias" in include_metrics:
        data["multiplicative_bias"] = (
            np.mean(group["secondary_value"])
            / np.mean(group["primary_value"])
        )

    if include_metrics == "all" or "pearson_correlation" in include_metrics:
        pearson_correlation = (
            np.corrcoef(group["secondary_value"], group["primary_value"])
        )[0][1]
        data["pearson_correlation"] = pearson_correlation

    if include_metrics == "all" or "r_squared" in include_metrics:
        pearson_correlation = (
            np.corrcoef(group["secondary_value"], group["primary_value"])
        )[0][1]
        r_squared = (np.power(pearson_correlation, 2))
        data["r_squared"] = r_squared

    if include_metrics == "all" or "max_value_delta" in include_metrics:
        data["max_value_delta"] = (
            np.max(group["secondary_value"])
            - np.max(group["primary_value"])
        )

    if (
        include_metrics == "all"
        or "annual_peak_relative_bias" in include_metrics
    ):
        primary_yearly_max_values = (
            group
            .groupby(group.value_time.dt.year)
            .primary_value.max()
        )
        secondary_yearly_max_values = (
            group
            .groupby(group.value_time.dt.year)
            .secondary_value.max()
        )
        data["annual_peak_relative_bias"] = (
            np.sum(secondary_yearly_max_values - primary_yearly_max_values)
            / np.sum(primary_yearly_max_values)
        )

    if (
        include_metrics == "all"
        or "spearman_correlation" in include_metrics
    ):
        group["primary_rank"] = group["primary_value"].rank()
        group["secondary_rank"] = group["secondary_value"].rank()
        count = len(group)

        data["spearman_correlation"] = (
            1 - (
                6 * np.sum(
                    np.abs(
                        group["primary_rank"]
                        - group["secondary_rank"]
                    )**2)
                / (count * (count**2 - 1))
            )
        )

    # HydroTools Forecast Metrics
    if (
        include_metrics == "all"
        or "nash_sutcliffe_efficiency" in include_metrics
    ):
        nse = hm.nash_sutcliffe_efficiency(
            group["primary_value"],
            group["secondary_value"]
        )
        data["nash_sutcliffe_efficiency"] = nse

    if (
        include_metrics == "all"
        or "nash_sutcliffe_efficiency_normalized" in include_metrics
    ):
        nse = hm.nash_sutcliffe_efficiency(
            group["primary_value"],
            group["secondary_value"],
            normalized=True
        )
        data["nash_sutcliffe_efficiency_normalized"] = nse

    # if (
    #     include_metrics == "all"
    #     or "nash_sutcliffe_efficiency_log" in include_metrics
    # ):
    #     nse = hm.nash_sutcliffe_efficiency(
    #         group["primary_value"],
    #         group["secondary_value"],
    #         log=True
    #     )
    #     data["nash_sutcliffe_efficiency_log"] = nse

    if (
            include_metrics == "all"
            or "kling_gupta_efficiency" in include_metrics
    ):
        kge = hm.kling_gupta_efficiency(
            group["primary_value"],
            group["secondary_value"]
        )
        data["kling_gupta_efficiency"] = kge

    if (
        include_metrics == "all"
        or "kling_gupta_efficiency_mod1" in include_metrics
    ):

        # Pearson correlation coefficient (same as kge)
        linear_correlation = np.corrcoef(
            group["secondary_value"], group["primary_value"]
        )[0, 1]

        # Variability_ratio
        variability_ratio = (
            (
                np.std(group["secondary_value"])
                / np.mean(group["secondary_value"])
            )
            / (
                np.std(group["primary_value"])
                / np.mean(group["primary_value"])
            )
        )

        # Relative mean (same as kge)
        relative_mean = (
            np.mean(group["secondary_value"])
            / np.mean(group["primary_value"])
        )

        # Scaled Euclidean distance
        euclidean_distance = np.sqrt(
            ((linear_correlation - 1.0)) ** 2.0 +
            ((variability_ratio - 1.0)) ** 2.0 +
            ((relative_mean - 1.0)) ** 2.0
            )

        data["kling_gupta_efficiency_mod1"] = 1.0 - euclidean_distance

    if (
        include_metrics == "all"
        or "kling_gupta_efficiency_mod2" in include_metrics
    ):
        # Pearson correlation coefficient (same as kge)
        linear_correlation = np.corrcoef(
            group["secondary_value"], group["primary_value"]
        )[0, 1]

        # Relative variability (same as kge)
        relative_variability = (
            np.std(group["secondary_value"])
            / np.std(group["primary_value"])
        )

        # bias component
        bias_component = (
            (
                (
                    np.mean(group["secondary_value"])
                    - np.mean(group["primary_value"])
                ) ** 2
            )
            /
            (
                np.std(group["primary_value"]) ** 2
            )
        )

        # Scaled Euclidean distance
        euclidean_distance = np.sqrt(
            ((linear_correlation - 1.0)) ** 2.0 +
            ((relative_variability - 1.0)) ** 2.0 +
            bias_component
            )

        data["kling_gupta_efficiency_mod2"] = 1.0 - euclidean_distance

    if (
        include_metrics == "all"
        or "coefficient_of_extrapolation" in include_metrics
    ):
        coe = hm.coefficient_of_extrapolation(
            group["primary_value"],
            group["secondary_value"]
        )
        data["coefficient_of_extrapolation"] = coe

    if (
        include_metrics == "all"
        or "coefficient_of_persistence" in include_metrics
    ):
        cop = hm.coefficient_of_persistence(
            group["primary_value"],
            group["secondary_value"]
        )
        data["coefficient_of_persistence"] = cop

    if include_metrics == "all" or "mean_absolute_error" in include_metrics:
        me = hm.mean_error(
            group["primary_value"],
            group["secondary_value"]
        )
        data["mean_absolute_error"] = me

    if include_metrics == "all" or "mean_squared_error" in include_metrics:
        mse = hm.mean_squared_error(
            group["primary_value"],
            group["secondary_value"]
        )
        data["mean_squared_error"] = mse

    if (
        include_metrics == "all"
        or "root_mean_squared_error" in include_metrics
    ):
        rmse = hm.root_mean_squared_error(
            group["primary_value"],
            group["secondary_value"]
        )
        data["root_mean_squared_error"] = rmse

    # Ensure the first occurrence of a repeated value gets selected
    group = group.sort_values(by=["reference_time", "value_time"])

    # Time-based Metrics
    time_indexed_df = group.set_index("value_time")
    if (
        include_metrics == "all"
        or "primary_max_value_time" in include_metrics
    ):
        pmvt = time_indexed_df["primary_value"].idxmax()
        data["primary_max_value_time"] = pmvt

    if (
        include_metrics == "all"
        or "secondary_max_value_time" in include_metrics
    ):
        smvt = time_indexed_df["secondary_value"].idxmax()
        data["secondary_max_value_time"] = smvt

    if (
        include_metrics == "all"
        or "max_value_timedelta" in include_metrics
    ):
        pmvt = time_indexed_df["primary_value"].idxmax()
        smvt = time_indexed_df["secondary_value"].idxmax()
        data["max_value_timedelta"] = smvt - pmvt

    return pd.Series(data)
