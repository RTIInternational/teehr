"""Utility functions for querying data."""
import geopandas as gpd
import pandas as pd
from typing import List

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


def order_df(df, sort_by):
    """Sort a DataFrame by a list of columns."""
    if not isinstance(sort_by, List):
        sort_by = [sort_by]
    return df.orderBy([field.value for field in sort_by])


def group_df(df, group_by):
    """Group a DataFrame by a list of columns."""
    if not isinstance(group_by, List):
        group_by = [group_by]
    return df.groupBy([field.value for field in group_by])
