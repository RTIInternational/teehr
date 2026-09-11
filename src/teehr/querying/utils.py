"""Utility functions for querying data."""
import inspect
import geopandas as gpd
import pandas as pd
from typing import Callable, List, Union
import pyspark.sql as ps
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pydantic import BaseModel as PydanticBaseModel

import teehr
from teehr.models.str_enum import StrEnum

import logging

logger = logging.getLogger(__name__)


def post_process_metric_results(
    metrics_sdf: ps.DataFrame,
    include_metrics: List[PydanticBaseModel],
    group_by: Union[
        str,
        List[str]
    ]
) -> ps.DataFrame:
    """Post-process the results of the metrics query.

    Parameters
    ----------
    metrics_sdf : ps.DataFrame
        DataFrame containing calculated metrics.
    include_metrics : List[PydanticBaseModel]
        List of metric models used in the query.
    group_by : Union[str, JoinedTimeseriesFields, List[...]]
        Fields used for grouping in the metrics calculation.

    Returns
    -------
    ps.DataFrame
        Processed DataFrame with skill scores and unpacked results as specified
        by the metric models.

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
    columns in the DataFrame. For a bootstrap metric with quantiles the
    unpacked column names are derived from the metric configuration as
    ``{output_field_name}_{quantile}`` with dots replaced by underscores, so
    unpacking adds no Spark action and aggregation remains lazy. Metrics whose
    map keys cannot be derived from configuration fall back to reading the keys
    from the first row, which does trigger an action.
    """
    for model in include_metrics:
        if model.reference_configuration is not None:
            """
            self.df = self._calculate_metric_skill_score(
                model.output_field_name,
                model.reference_configuration,
                group_by
            )
            """
            # 1) get the original cols ahead of skill score join
            original_cols = metrics_sdf.columns
            # 2) calculate skill score sdf
            sdf = calculate_metric_skill_score(
                metrics_sdf,
                model.output_field_name,
                model.reference_configuration,
                group_by
            )
            # 3) remove original metric column from skill score sdf
            sdf = sdf.drop(model.output_field_name)
            # 3) get join columns
            join_cols = parse_fields_to_list(group_by)
            # 4) join returned table back to self.df, trim
            metrics_sdf = metrics_sdf.join(
                sdf,
                on=join_cols,
                how="left"
            ).select(
                *original_cols,
                F.col(f"{model.output_field_name}_skill_score")
            )

        if model.unpack_results:
            # Shared-bootstrap expansion may have already unpacked this metric's
            # MapType column into individual quantile columns. Skip if gone.
            if model.output_field_name not in metrics_sdf.columns:
                continue

            key_list = derive_map_key_list(model)
            if key_list is not None and _unpacker_accepts_key_list(
                model.unpack_function
            ):
                # Statically derived keys keep the whole aggregate() plan lazy:
                # no eager Spark action, and therefore no re-execution of the
                # upstream bootstrap DAG, per metric.
                metrics_sdf = model.unpack_function(
                    metrics_sdf,
                    model.output_field_name,
                    key_list=key_list
                )
            else:
                metrics_sdf = model.unpack_function(
                    metrics_sdf,
                    model.output_field_name
                )

    return metrics_sdf


def sanitize_map_key_name(key, dot_replacement: str = "_") -> str:
    """Sanitize a map key into a Spark-safe output column name."""
    return str(key).replace(".", dot_replacement)


def bootstrap_quantile_key(output_field_name: str, quantile) -> str:
    """Return the map key used for one bootstrap quantile value.

    This is the single source of truth for the key format shared by the
    bootstrap UDFs (``metrics.bootstrap_funcs``,
    ``metrics.vectorized_bootstrap_funcs``), the shared-bootstrap map
    reconstruction (``metrics.format``), and the static key derivation used
    when unpacking. Keys intentionally keep their dots; dots are replaced only
    when building output column aliases (see ``sanitize_map_key_name``).
    """
    return f"{output_field_name}_{quantile}"


def derive_map_key_list(model: PydanticBaseModel) -> Union[List[str], None]:
    """Statically derive the MapType keys a metric's output column will have.

    Parameters
    ----------
    model : PydanticBaseModel
        A metric model.

    Returns
    -------
    Union[List[str], None]
        The ordered map keys, or None when the key set cannot be known from
        configuration alone and must be discovered from the data.
    """
    bootstrap = getattr(model, "bootstrap", None)
    quantiles = getattr(bootstrap, "quantiles", None)
    if quantiles is not None:
        keys = [
            bootstrap_quantile_key(model.output_field_name, q)
            for q in quantiles
        ]
        # A repeated quantile (e.g. [0.5, 0.50]) collapses to a single key in
        # the dict the UDF returns, so dedupe to mirror the producer and avoid
        # a spurious "sanitized map keys are not unique" error while unpacking.
        return list(dict.fromkeys(keys))

    # Metrics that always return the same map keys (e.g. ConfusionMatrix).
    static_keys = getattr(type(model), "static_map_keys", None)
    if static_keys:
        return list(dict.fromkeys(static_keys))

    return None


def _unpacker_accepts_key_list(func: Callable) -> bool:
    """Whether an unpack callable supports the ``key_list`` keyword."""
    if func is unpack_sdf_dict_columns:
        return True
    try:
        params = inspect.signature(func).parameters
    except (TypeError, ValueError):
        # Builtins and some C callables have no introspectable signature.
        return False
    return "key_list" in params or any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
    )


def calculate_metric_skill_score(
    metrics_sdf: ps.DataFrame,
    metric_field: str,
    reference_configuration: str,
    group_by: Union[
        str,
        List[str]
    ]
) -> ps.DataFrame:
    """Calculate skill score based on a reference configuration.

    Parameters
    ----------
    metrics_sdf : ps.DataFrame
        DataFrame containing calculated metrics.
    metric_field : str
        The name of the metric field to calculate skill scores for.
    reference_configuration : str
        The name of the reference configuration.
    group_by : Union[str, List[str]]
        Fields used for grouping in the metrics calculation.

    Calculate the skill score of metric values for each configuration
    relative to the reference configuration. The skill score is calculated
    as `1 - (metric_value / reference_metric_value)`.
    """
    logger.debug("Calculating skill score.")
    group_by_strings = parse_fields_to_list(group_by)
    # TODO: Raise error if configuration_name is not in group_by?
    group_by_strings.remove("configuration_name")

    pivot_sdf = (
        metrics_sdf
        .groupBy(group_by_strings).
        pivot("configuration_name").
        agg(F.first(metric_field))
    )
    # Get all configuration names except the reference configuration
    configurations = metrics_sdf.select("configuration_name").distinct().collect()
    configurations = [row.configuration_name for row in configurations]
    configurations.remove(reference_configuration)

    skill_score_col = f"{metric_field}_skill_score"
    sdf = metrics_sdf.withColumn(skill_score_col, F.lit(None))

    for config in configurations:
        # Pivot and calculate the skill score.
        temp_col = f"{config}_{metric_field}_skill"
        pivot_sdf = pivot_sdf.withColumn(
            temp_col,
            1 - F.try_divide(F.col(config), F.col(reference_configuration))
        ).withColumn(
            "configuration_name",
            F.lit(config)
        )
        # warn user if try_divide results in nulls (division by zero)
        null_count = pivot_sdf.filter(F.col(temp_col).isNull()).count()
        if null_count > 0:
            logger.warning(
                f"Division by zero encountered when calculating skill "
                f"score for configuration '{config}' relative to "
                f"reference configuration '{reference_configuration}'. "
                f"{null_count} null values were produced."
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


def unpack_sdf_dict_columns(
    sdf: ps.DataFrame,
    column_name: str,
    dot_replacement: str = "_",
    key_list: Union[List[str], None] = None
) -> ps.DataFrame:
    """Expand a MapType column into one column per key.

    Parameters
    ----------
    sdf : ps.DataFrame
        DataFrame containing the MapType column to expand.
    column_name : str
        Name of the MapType column.
    dot_replacement : str
        Replacement for dots in the output column names, by default "_".
    key_list : Union[List[str], None]
        Map keys to expand, in output order. When None the keys are discovered
        from the first row, which requires an eager Spark action and therefore
        re-executes the upstream plan. Callers that can derive the keys from
        configuration should pass them so the plan stays lazy (see
        `derive_map_key_list`).

    Returns
    -------
    ps.DataFrame
        DataFrame with the MapType column replaced by one column per key,
        with dots in the key names replaced by `dot_replacement`.
    """
    field_type = sdf.schema[column_name].dataType
    if not isinstance(field_type, T.MapType):
        raise ValueError(
            f"Cannot unpack column {column_name!r}: expected a MapType column "
            f"but found {field_type.simpleString()}. Unpacking is only "
            "supported for metrics that return a map, such as a bootstrap "
            "metric with 'quantiles' set. A bootstrap configured with "
            "quantiles=None returns an array of replicates instead; either "
            "set quantiles or leave unpack_results=False."
        )

    if key_list is None:
        logger.debug(
            "Discovering map keys for column '%s' from the first row. This "
            "triggers an eager Spark action and re-executes the upstream plan.",
            column_name
        )
        first = sdf.select(column_name).first()
        m = first[column_name] if first is not None else None
        key_list = list(m.keys()) if m else []
    else:
        key_list = list(key_list)

    def safe_name(k) -> str:
        # k might be non-string; preserve uniqueness but remove '.' which breaks resolution
        return sanitize_map_key_name(k, dot_replacement=dot_replacement)

    base_cols = [c for c in sdf.columns if c != column_name]

    alias_to_keys = {}
    for key in key_list:
        alias = safe_name(key)
        alias_to_keys.setdefault(alias, []).append(key)

    duplicate_aliases = {
        alias: keys for alias, keys in alias_to_keys.items() if len(keys) > 1
    }
    base_col_collisions = {
        alias: keys for alias, keys in alias_to_keys.items() if alias in base_cols
    }

    if duplicate_aliases or base_col_collisions:
        problems = []
        if duplicate_aliases:
            problems.append(
                "sanitized map keys are not unique: "
                + ", ".join(
                    f"{alias!r} <- {[str(k) for k in keys]!r}"
                    for alias, keys in sorted(duplicate_aliases.items())
                )
            )
        if base_col_collisions:
            problems.append(
                "sanitized map keys collide with existing columns: "
                + ", ".join(
                    f"{alias!r} from {[str(k) for k in keys]!r}"
                    for alias, keys in sorted(base_col_collisions.items())
                )
            )
        raise ValueError(
            f"Cannot unpack column {column_name!r} because "
            + "; ".join(problems)
        )

    value_cols = [
        F.col(column_name).getItem(k).alias(alias)
        for k, alias in ((key, safe_name(key)) for key in key_list)
    ]
    return sdf.select(*base_cols, *value_cols)


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


def join_attributes(
    target_df: ps.DataFrame,
    attrs_df: ps.DataFrame,
    target_location_id: str = None,
) -> ps.DataFrame:
    """Join pivoted location attributes to a target DataFrame.

    Parameters
    ----------
    target_df : ps.DataFrame
        The target DataFrame to join attributes to.
    attrs_df : ps.DataFrame
        The pivoted attributes DataFrame (from location_attributes_view).
    target_location_id : str, optional
        The column name in target_df to join on. If None, checks for
        'location_id' then 'primary_location_id' in target_df columns.

    Returns
    -------
    ps.DataFrame
        The target DataFrame with attributes added.
    """
    logger.debug("Joining location attributes.")

    target_df_columns = target_df.columns
    if target_location_id is None:
        if "location_id" in target_df_columns:
            target_location_id = "location_id"
        elif "primary_location_id" in target_df_columns:
            target_location_id = "primary_location_id"
        elif "id" in target_df_columns:
            target_location_id = "id"
        else:
            error_msg = (
                "No 'location_id', 'primary_location_id', or 'id' column "
                "found in target DataFrame."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

    # Get attr columns excluding location_id (the join key from attrs_df)
    attr_cols = [c for c in attrs_df.columns if c != "location_id"]

    # Rename attrs' location_id to match target join column if needed,
    # and select only relevant columns to prevent column ambiguity
    if target_location_id != "location_id":
        attrs_df = attrs_df.withColumnRenamed("location_id", target_location_id)

    attrs_df = attrs_df.select([target_location_id] + attr_cols)

    joined_df = target_df.join(attrs_df, on=target_location_id)
    return joined_df


def join_geometry(
    target_df: ps.DataFrame,
    location_df: ps.DataFrame,
    target_location_id: str = None,
) -> ps.DataFrame:
    """Join geometry."""
    logger.debug("Joining locations geometry.")

    target_df_columns = target_df.columns
    if target_location_id is None:
        if "location_id" in target_df_columns:
            target_location_id = "location_id"
        elif "primary_location_id" in target_df_columns:
            target_location_id = "primary_location_id"
        else:
            error_msg = """
                No 'location_id' or 'primary_location_id' column
                found in target DataFrame.
            """
            logger.error(error_msg)
            raise ValueError(error_msg)

    location_df = location_df.withColumnRenamed(
        "id",
        target_location_id
    ).select(
        target_location_id,
        "name",
        "geometry"
    )

    joined_df = target_df.join(
        location_df, on=target_location_id
    )
    return joined_df
