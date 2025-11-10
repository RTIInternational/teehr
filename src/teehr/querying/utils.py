"""Utility functions for querying data."""
import geopandas as gpd
import pandas as pd
from typing import List, Union
import pyspark.sql as ps
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pydantic import BaseModel as PydanticBaseModel

from teehr.models.str_enum import StrEnum
# from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.table_enums import (
    JoinedTimeseriesFields
)

import logging

logger = logging.getLogger(__name__)


def post_process_metric_results(
    sdf: ps.DataFrame,
    include_metrics: List[PydanticBaseModel],
    group_by: Union[
        str, JoinedTimeseriesFields,
        List[Union[str, JoinedTimeseriesFields]]
    ]
) -> ps.DataFrame:
    """Post-process the results of the metrics query.

    Notes
    -----
    This method includes functionality to update the dataframe returned
    by the query method depending on metric model attributes.

    If the metric model specifies a reference configuration, it will
    calculate the skill score of metric values for each configuration
    relative to the reference configuration. The skill score is calculated
    as `1 - (metric_value / reference_metric_value)`.

    Additionally, if the metric model specifies unpacking of results,
    metric results returned as a dictionary will be unpacked into separate
    columns in the DataFrame.
    """
    for model in include_metrics:
        if model.reference_configuration is not None:
            sdf = calculate_metric_skill_score(
                sdf,
                model.output_field_name,
                model.reference_configuration,
                group_by
            )

        if model.unpack_results:
            sdf = model.unpack_function(
                sdf,
                model.output_field_name
            )

    return sdf


def calculate_metric_skill_score(
    sdf: ps.DataFrame,
    metric_field: str,
    reference_configuration: str,
    group_by: Union[
        str, JoinedTimeseriesFields,
        List[Union[str, JoinedTimeseriesFields]]
    ]
) -> ps.DataFrame:
    """Calculate skill score based on a reference configuration.

    Calculate the skill score of metric values for each configuration
    relative to the reference configuration. The skill score is calculated
    as `1 - (metric_value / reference_metric_value)`.
    """
    logger.debug("Calculating skill score.")
    group_by_strings = parse_fields_to_list(group_by)
    # TODO: Raise error if configuration_name is not in group_by?
    group_by_strings.remove("configuration_name")

    pivot_sdf = (
        sdf
        .groupBy(group_by_strings).
        pivot("configuration_name").
        agg(F.first(metric_field))
    )
    # Get all configuration names except the reference configuration
    configurations = sdf.select("configuration_name").distinct().collect()
    configurations = [row.configuration_name for row in configurations]
    configurations.remove(reference_configuration)

    skill_score_col = f"{metric_field}_skill_score"
    sdf = sdf.withColumn(skill_score_col, F.lit(None))

    for config in configurations:
        # Pivot and calculate the skill score.
        temp_col = f"{config}_{metric_field}_skill"
        pivot_sdf = pivot_sdf.withColumn(
            temp_col,
            1 - F.col(config) / F.col(reference_configuration)
        ).withColumn(
            "configuration_name",
            F.lit(config)
        )
        # Join skill score values from the pivot table.
        join_cols = group_by_strings + ["configuration_name"]
        sdf = sdf.join(
            pivot_sdf,
            on=join_cols,
            how="left"
        ).select(
            *join_cols,
            F.col(f"{metric_field}"),
            F.col(temp_col),
            F.col(skill_score_col)
        )
        # Now update the column based on the configuration name.
        sdf = sdf.withColumn(
            skill_score_col,
            F.when(
                sdf["configuration_name"] == f"{config}",
                sdf[temp_col]
            ).otherwise(sdf[skill_score_col])
        ).select(
            *join_cols,
            F.col(f"{metric_field}"),
            F.col(skill_score_col)
        )

    return sdf


def unpack_sdf_dict_columns(sdf: DataFrame, column_name: str) -> DataFrame:
    """Explode a column of dictionaries into new columns named key_item."""
    first_row = sdf.select(column_name).first()
    keys = sdf.sparkSession.createDataFrame([first_row]).select(
        F.explode(F.map_keys(F.col(column_name)))
        ).distinct()
    key_list = list(map(lambda row: row[0], keys.collect()))
    key_cols = list(
        map(lambda f: F.col(column_name).getItem(f).alias(str(f)), key_list)
    )
    df_cols = list(sdf.columns)
    df_cols.remove(column_name)
    return sdf.select(*df_cols, *key_cols)


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
