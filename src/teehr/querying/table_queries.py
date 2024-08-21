"""Functions to query the joined timeseries data."""
import logging
from typing import Union, List
from pathlib import Path
from pyspark.sql import SparkSession
import geopandas as gpd
import pandas as pd

from teehr.querying.filter_format import apply_filters, validate_filter_values
from teehr.querying.metrics_format import apply_aggregation_metrics
from teehr.querying.utils import (
    order_df,
    df_to_gdf,
    group_df,
    join_locations_geometry
)
from teehr.models.metrics.metrics import MetricsBasemodel
from teehr.models.dataset.table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
    Location,
    LocationAttribute,
    LocationCrosswalk,
    Timeseries,
)
from teehr.models.dataset.filters import (
    ConfigurationFilter,
    UnitFilter,
    VariableFilter,
    AttributeFilter,
    LocationFilter,
    LocationAttributeFilter,
    LocationCrosswalkFilter,
    TimeseriesFilter,
    JoinedTimeseriesFilter
)
from teehr.models.dataset.table_enums import (
    ConfigurationFields,
    UnitFields,
    VariableFields,
    AttributeFields,
    LocationFields,
    LocationAttributeFields,
    LocationCrosswalkFields,
    TimeseriesFields,
    JoinedTimeseriesFields
)

logger = logging.getLogger(__name__)


def get_units(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[UnitFilter, List[UnitFilter]] = None,
    order_by: Union[UnitFields, List[UnitFields]] = None
) -> pd.DataFrame:
    """Get the units data."""
    # Read all the files in the given directory
    units_df = (
        spark.read.format("csv")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .option("header", True)
        # .option("delimiter", ",")
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, Unit)
        units_df = apply_filters(units_df, validated_filters)

    if order_by is not None:
        units_df = order_df(units_df, order_by)

    return units_df.toPandas()


def get_variables(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[VariableFilter, List[VariableFilter]] = None,
    order_by: Union[VariableFields, List[VariableFields]] = None
) -> pd.DataFrame:
    """Get the variables data."""
    # Read all the files in the given directory
    variables_df = (
        spark.read.format("csv")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .option("header", True)
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, Variable)
        variables_df = apply_filters(variables_df, validated_filters)

    if order_by is not None:
        variables_df = order_df(variables_df, order_by)

    return variables_df.toPandas()


def get_attributes(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[AttributeFilter, List[AttributeFilter]] = None,
    order_by: Union[AttributeFields, List[AttributeFields]] = None
) -> pd.DataFrame:
    """Get the attributes data."""
    # Read all the files in the given directory
    attributes_df = (
        spark.read.format("csv")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .option("header", True)
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, Attribute)
        attributes_df = apply_filters(attributes_df, validated_filters)

    if order_by is not None:
        attributes_df = order_df(attributes_df, order_by)

    return attributes_df.toPandas()


def get_configurations(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[
        ConfigurationFilter,
        List[ConfigurationFilter]
    ] = None,
    order_by: Union[
        ConfigurationFields,
        List[ConfigurationFields]
    ] = None
) -> pd.DataFrame:
    """Get the configurations data."""
    # Read all the files in the given directory
    configurations_df = (
        spark.read.format("csv")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .option("header", True)
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, Configuration)
        configurations_df = apply_filters(configurations_df, validated_filters)

    if order_by is not None:
        configurations_df = order_df(configurations_df, order_by)

    return configurations_df.toPandas()


def get_locations(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[LocationFilter, List[LocationFilter]] = None,
    order_by: Union[LocationFields, List[LocationFields]] = None,
) -> gpd.GeoDataFrame:
    """Get the locations data."""
    # Read all the files in the given directory
    locations_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, Location)
        locations_df = apply_filters(locations_df, validated_filters)

    if order_by is not None:
        locations_df = order_df(locations_df, order_by)

    return df_to_gdf(locations_df.toPandas())


def get_location_attributes(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[
        LocationAttributeFilter,
        List[LocationAttributeFilter]
    ] = None,
    order_by: Union[
        LocationAttributeFields,
        List[LocationAttributeFields]
    ] = None
) -> pd.DataFrame:
    """Get the location attributes data."""
    # Read all the files in the given directory
    location_attributes_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, LocationAttribute)
        location_attributes_df = apply_filters(
            location_attributes_df, validated_filters
        )

    if order_by is not None:
        location_attributes_df = order_df(location_attributes_df, order_by)

    return location_attributes_df.toPandas()


def get_location_crosswalks(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[
        LocationCrosswalkFilter,
        List[LocationCrosswalkFilter]
    ] = None,
    order_by: Union[
        LocationCrosswalkFields,
        List[LocationCrosswalkFields]
    ] = None
) -> pd.DataFrame:
    """Get the location crosswalks data."""
    # Read all the files in the given directory
    location_crosswalks_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, LocationCrosswalk)
        location_crosswalks_df = apply_filters(
            location_crosswalks_df, validated_filters
        )

    if order_by is not None:
        location_crosswalks_df = order_df(location_crosswalks_df, order_by)

    return location_crosswalks_df.toPandas()


def get_timeseries(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[TimeseriesFilter, List[TimeseriesFilter]] = None,
    order_by: Union[TimeseriesFields, List[TimeseriesFields]] = None
) -> pd.DataFrame:
    """Get the timeseries data."""
    # Read all the files in the given directory
    timeseries_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(dirpath))
    )
    if filters is not None:
        validated_filters = validate_filter_values(filters, Timeseries)
        timeseries_df = apply_filters(timeseries_df, validated_filters)

    if order_by is not None:
        timeseries_df = order_df(timeseries_df, order_by)

    return timeseries_df.toPandas()


def get_joined_timeseries(
    spark: SparkSession,
    dirpath: Union[str, Path],
    filters: Union[
        JoinedTimeseriesFilter,
        List[JoinedTimeseriesFilter]
    ] = None,
    order_by: Union[
        JoinedTimeseriesFields,
        List[JoinedTimeseriesFields]
    ] = None
) -> pd.DataFrame:
    """Get the joined timeseries data."""
    # Read all the files in the given directory
    joined_timeseries_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(dirpath))
    )
    # validated_filters = validate_filter_values(filters, Timeseries)
    if filters is not None:
        joined_timeseries_df = apply_filters(joined_timeseries_df, filters)

    if order_by is not None:
        joined_timeseries_df = order_df(joined_timeseries_df, order_by)

    return joined_timeseries_df.toPandas()


def get_metrics(
    spark: SparkSession,
    dirpath: Union[str, Path],
    locations_dirpath: Union[str, Path],
    include_metrics: Union[
        List[MetricsBasemodel],
        str
    ],
    group_by: Union[
        JoinedTimeseriesFields,
        List[JoinedTimeseriesFields]
    ],
    filters: Union[
        JoinedTimeseriesFilter,
        List[JoinedTimeseriesFilter]
    ] = None,
    order_by: Union[
        JoinedTimeseriesFields,
        List[JoinedTimeseriesFields]
    ] = None,
    include_geometry: bool = False
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Get the metrics data."""
    joined_timeseries_df = (
        spark.read.format("parquet")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .load(str(dirpath))
    )
    if filters is not None:
        joined_timeseries_df = apply_filters(joined_timeseries_df, filters)

    if order_by is not None:
        joined_timeseries_df = order_df(joined_timeseries_df, order_by)

    grouped_df = group_df(joined_timeseries_df, group_by)

    metrics_df = apply_aggregation_metrics(
        grouped_df,
        include_metrics
    )

    if include_geometry:
        return join_locations_geometry(
            spark,
            metrics_df,
            group_by,
            locations_dirpath
        )

    return metrics_df.toPandas()
