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


def validate_fields_exist(
        valid_fields: List[str],
        requested_fields: List[str]
):
    """Validate that the requested_fields are in the valid_fields list."""
    logger.debug("Validating requested fields.")
    if not all(e in valid_fields for e in requested_fields):
        error_msg = f"One of the requested fields: {requested_fields} is not" \
                    f" a valid DataFrame field: {valid_fields}."
        logger.error(error_msg)
        raise ValueError(error_msg)


def parse_fields_to_list(
        requested_fields: Union[str, StrEnum, List[Union[str, StrEnum]]]
) -> List[str]:
    """Convert the requested fields to a list of strings."""
    logger.debug("Parsing requested fields to a list of strings.")
    if not isinstance(requested_fields, List):
        requested_fields = [requested_fields]
    requested_fields_strings = []
    for field in requested_fields:
        if isinstance(field, str):
            requested_fields_strings.append(field)
        else:
            requested_fields_strings.append(field.value)
    return requested_fields_strings


def order_df(df, sort_by: Union[str, StrEnum, List[Union[str, StrEnum]]]):
    """Sort a DataFrame by a list of columns."""
    logger.debug("Ordering DataFrame.")
    sort_by_strings = parse_fields_to_list(sort_by)
    validate_fields_exist(df.columns, sort_by_strings)
    return df.orderBy(*sort_by_strings)


def group_df(df, group_by: Union[str, StrEnum, List[Union[str, StrEnum]]]):
    """Group a DataFrame by a list of columns."""
    logger.debug("Grouping DataFrame.")
    group_by_strings = parse_fields_to_list(group_by)
    validate_fields_exist(df.columns, group_by_strings)
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
