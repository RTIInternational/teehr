"""Functions for formatting metrics for querying."""
from typing import List
import logging

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import GroupedData
from pyspark.sql.functions import pandas_udf

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.metrics.bootstrap_funcs import (
    partition_metrics_by_bootstrap,
    create_shared_bootstrap_func,
)
from teehr.querying.utils import (
    sanitize_map_key_name,
    validate_fields_exist,
    parse_fields_to_list
)

logger = logging.getLogger(__name__)


def _build_non_bootstrap_udf(model: MetricsBasemodel, gp: GroupedData):
    """Return (udf_col_expr, alias) for a non-bootstrap or raw-array metric."""
    if hasattr(model, "get_input_field_names"):
        input_field_names = parse_fields_to_list(model.get_input_field_names())
    else:
        input_field_names = parse_fields_to_list(model.input_field_names)

    if model.attrs["requires_threshold_field"]:
        if model.threshold_field_name is None:
            raise ValueError(
                f"{model} requires a valid threshold_field_name argument."
            )
        if model.threshold_field_name not in input_field_names:
            input_field_names.append(model.threshold_field_name)

    validate_fields_exist(gp._df.columns, input_field_names)

    alias = model.output_field_name

    if "bootstrap" in model.model_dump() and model.bootstrap is not None:
        logger.debug(
            f"Applying metric: {alias} with {model.bootstrap.name}"
            " bootstrapping (raw array path)"
        )
        func_pd = pandas_udf(
            model.bootstrap.func(model),
            model.bootstrap.return_type
        )
        if model.bootstrap.include_value_time and "value_time" not in input_field_names:
            input_field_names.append("value_time")
    else:
        logger.debug(f"Applying metric: {alias}")
        func_pd = pandas_udf(model.func(model), model.return_type)

    return func_pd(*input_field_names).alias(alias)


def _build_shared_bootstrap_udfs(
    boot_groups,
    gp: GroupedData,
):
    """Return (func_list_entries, expansion_steps) for shared-bootstrap groups.

    Parameters
    ----------
    boot_groups : dict
        Mapping of key → list of metrics sharing the same bootstrap config.
    gp : GroupedData
        The grouped Spark DataFrame (used for field validation).

    Returns
    -------
    func_list : list
        Aggregation column expressions for each group.
    expansions : list of (temp_col, metrics_in_group)
        Post-agg instructions for expanding each shared MapType column into
        its individual quantile columns.
    """
    func_list = []
    expansions = []

    for idx, (key, group_metrics) in enumerate(boot_groups.items()):
        ref = group_metrics[0]
        boot = ref.bootstrap

        if hasattr(ref, "get_input_field_names"):
            input_field_names = parse_fields_to_list(ref.get_input_field_names())
        else:
            input_field_names = parse_fields_to_list(ref.input_field_names)

        if boot.include_value_time and "value_time" not in input_field_names:
            input_field_names.append("value_time")

        validate_fields_exist(gp._df.columns, input_field_names)

        if len(group_metrics) == 1:
            # Singleton — use the standard path but still via shared helper
            # to keep the code uniform; it's the same cost as the old path.
            logger.debug(
                f"Applying metric: {ref.output_field_name} with "
                f"{boot.name} bootstrapping"
            )
            func_pd = pandas_udf(
                boot.func(ref),
                boot.return_type,
            )
            func_list.append(func_pd(*input_field_names).alias(ref.output_field_name))
            # No expansion needed — MapType column already has the right name.
        else:
            names = [m.output_field_name for m in group_metrics]
            logger.debug(
                f"Applying {len(group_metrics)} metrics sharing {boot.name} "
                f"bootstrap samples: {names}"
            )
            temp_col = f"_bsgrp_{idx}"
            shared_func = create_shared_bootstrap_func(group_metrics)
            return_type = T.MapType(T.StringType(), T.FloatType())
            func_pd = pandas_udf(shared_func, return_type)
            func_list.append(func_pd(*input_field_names).alias(temp_col))
            expansions.append((temp_col, group_metrics))

    return func_list, expansions


def _expand_shared_bootstrap_columns(sdf, expansions):
    """Unpack shared MapType columns into per-quantile columns and drop temps."""
    for temp_col, group_metrics in expansions:
        for metric in group_metrics:
            name = metric.output_field_name
            quantiles = metric.bootstrap.quantiles
            for q in quantiles:
                key = f"{name}_{q}"
                col_name = sanitize_map_key_name(key)
                sdf = sdf.withColumn(col_name, F.col(temp_col).getItem(key))
        sdf = sdf.drop(temp_col)
    return sdf


def apply_aggregation_metrics(
    gp: GroupedData,
    include_metrics: List[MetricsBasemodel] = None
) -> pd.DataFrame:
    """Apply metrics to a PySpark DataFrame."""
    if not isinstance(include_metrics, List):
        include_metrics = [include_metrics]

    # Separate metrics that can share bootstrap samples from everything else.
    no_boot_metrics, boot_groups = partition_metrics_by_bootstrap(include_metrics)

    func_list = []

    # Non-bootstrap and raw-array-bootstrap metrics (unchanged path).
    for model in no_boot_metrics:
        func_list.append(_build_non_bootstrap_udf(model, gp))

    # Bootstrap groups (shared-sample path for quantile metrics).
    boot_func_list, expansions = _build_shared_bootstrap_udfs(boot_groups, gp)
    func_list.extend(boot_func_list)

    sdf = gp.agg(*func_list)

    # Expand shared MapType columns into individual quantile columns.
    if expansions:
        sdf = _expand_shared_bootstrap_columns(sdf, expansions)

    return sdf
