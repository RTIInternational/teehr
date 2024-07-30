"""A module defining common utilities for queries."""
import pandas as pd
import geopandas as gpd
import warnings

from collections.abc import Iterable
from datetime import datetime
from fastapi.encoders import jsonable_encoder
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
    """Format str, Path of list of the same for SQL."""
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
    operator = jsonable_encoder(filter.operator)
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
        return f"""{column} {operator} '{filter.value}'"""
    elif (
        isinstance(filter.value, int)
        or isinstance(filter.value, float)
    ):
        return f"""{column} {operator} {filter.value}"""
    elif isinstance(filter.value, datetime):
        dt_str = filter.value.strftime(SQL_DATETIME_STR_FORMAT)
        return f"""{column} {operator} '{dt_str}'"""
    elif (
        isinstance(filter.value, Iterable)
        and not isinstance(filter.value, str)
    ):
        value = _format_iterable_value(filter.value)
        return f"""{column} {operator} {value}"""
    else:
        warnings.warn(
            "treating value as string because didn't know what else to do."
        )
        return f"""{column} {operator} '{str(filter.value)}'"""


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


def geometry_joined_join_clause(
    q: Union[tmq.MetricQuery, tmq.JoinedTimeseriesQuery]
) -> str:
    """Generate the join clause for joined timeseries or metrics."""
    if q.include_geometry:
        return f"""
        JOIN read_parquet({_format_filepath(q.geometry_filepath)}) gf
            on primary_location_id = gf.id
        """
    return ""


def geometry_db_join_clause(
    q: Union[tmqd.MetricQuery, tmqd.JoinedTimeseriesQuery]
) -> str:
    """Generate the metric geometry join clause for a database."""
    if q.include_geometry:
        return """JOIN geometry gf
            on primary_location_id = gf.id
        """
    return ""


def geometry_join_clause(
    q: Union[
        tmq.MetricQuery,
        tmq.JoinedTimeseriesQuery,
        tmqd.MetricQuery,
        tmqd.JoinedTimeseriesQuery
    ],
    geometry_join_clause: str
) -> str:
    """Generate the geometry join clause."""
    if q.include_geometry:
        return geometry_join_clause
    return ""


def geometry_select_clause(
    q: Union[tmq.MetricQuery,
             tmq.JoinedTimeseriesQuery,
             tmqd.MetricQuery,
             tmqd.JoinedTimeseriesQuery]
) -> str:
    """Generate the geometry select clause."""
    if q.include_geometry:
        return ", gf.geometry as geometry"
    return ""


def join_time_on(join: str, join_to: str, join_on: List[str]):
    """Generate the join time on query."""
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
        AND {join}.n = 1
    """
    return qry


def join_on(join: str, join_to: str, join_on: List[str]) -> str:
    """Generate the join on query."""
    qry = f"""
        INNER JOIN {join}
        ON {f" AND ".join([f"{join}.{jo} = {join_to}.{jo}" for jo in join_on])}
    """
    return qry


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
