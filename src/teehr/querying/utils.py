"""Utility functions for querying data."""
import geopandas as gpd
import pandas as pd
from typing import List, Union
from teehr.models.str_enum import StrEnum
import pyspark.sql as ps

import logging

logger = logging.getLogger(__name__)


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


def order_df(df, sort_by: Union[str, StrEnum, List[Union[str, StrEnum]]]):
    """Sort a DataFrame by a list of columns."""
    if not isinstance(sort_by, List):
        sort_by = [sort_by]
    sort_by_strings = []
    for field in sort_by:
        if isinstance(field, str):
            sort_by_strings.append(field)
        else:
            sort_by_strings.append(field.value)

    return df.orderBy(*sort_by_strings)


def group_df(df, group_by: Union[str, StrEnum, List[Union[str, StrEnum]]]):
    """Group a DataFrame by a list of columns."""
    if not isinstance(group_by, List):
        group_by = [group_by]
    group_by_strings = []
    for field in group_by:
        if isinstance(field, str):
            group_by_strings.append(field)
        else:
            group_by_strings.append(field.value)

    return df.groupBy(*group_by_strings)


def join_geometry(
    target_df: ps.DataFrame,
    location_df: ps.DataFrame,
    target_location_id: str = "location_id",
):
    """Join geometry."""
    logger.debug("Joining locations geometry.")

    joined_df = target_df.join(
        location_df.withColumnRenamed(
            "id",
            target_location_id
        ).select(
            target_location_id,
            "geometry"
        ), on=target_location_id
    )
    return df_to_gdf(joined_df.toPandas())