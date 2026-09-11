"""Functions for formatting metrics for querying."""
from typing import List
import logging

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.functions import pandas_udf

from teehr.metrics.models.base import MetricsBasemodel
from teehr.metrics.bootstrap_funcs import (
    partition_metrics_by_bootstrap,
    create_shared_bootstrap_func,
)
from teehr.querying.utils import (
    bootstrap_quantile_key,
    validate_fields_exist,
    parse_fields_to_list
)

logger = logging.getLogger(__name__)


def _build_non_bootstrap_udf(model: MetricsBasemodel, gp: GroupedData):
    """Return the aggregation column expression for a non-bootstrap metric.

    Only ever called with ``partition_metrics_by_bootstrap``'s ``no_boot``
    list, and ``bootstrap_group_key`` returns None only when ``bootstrap`` is
    falsy -- so every model reaching here has no bootstrap config. This used to
    carry a ``model.bootstrap is not None`` branch for a "raw array path"; it
    was unreachable, and it was the only other consumer of ``boot.func`` /
    ``boot.return_type``.
    """
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
        individual metric output columns.
    """
    func_list = []
    expansions = []
    existing_columns = set(gp._df.columns)

    for idx, (key, group_metrics) in enumerate(boot_groups.items()):
        ref = group_metrics[0]
        boot = ref.bootstrap

        if hasattr(ref, "get_input_field_names"):
            input_field_names = parse_fields_to_list(ref.get_input_field_names())
        else:
            input_field_names = parse_fields_to_list(ref.input_field_names)

        # Same check _build_non_bootstrap_udf runs. Without it a bootstrapped
        # threshold metric with threshold_field_name=None fails inside the UDF
        # with an opaque TypeError instead of this explicit error.
        if ref.attrs["requires_threshold_field"]:
            if ref.threshold_field_name is None:
                raise ValueError(
                    f"{ref} requires a valid threshold_field_name argument."
                )
            if ref.threshold_field_name not in input_field_names:
                input_field_names.append(ref.threshold_field_name)

        if boot.include_value_time and "value_time" not in input_field_names:
            input_field_names.append("value_time")

        validate_fields_exist(gp._df.columns, input_field_names)

        names = [m.output_field_name for m in group_metrics]
        logger.debug(
            f"Applying {len(group_metrics)} metric(s) sharing {boot.name} "
            f"bootstrap samples: {names}"
        )

        # Every group takes this path, including groups of one. Singletons used
        # to be special-cased through boot.func(ref) -- i.e. the legacy
        # per-replicate loop -- which never consulted the vectorized engine, so
        # a lone bootstrapped metric got no benefit from it at all. Routing
        # them here also gives them the sample-size/mean/variance quality
        # guards in create_shared_bootstrap_func, which previously applied only
        # once a group had two or more metrics. Those guards are configured on
        # the Bootstrappers model, so nothing needs threading through here.
        temp_col = f"_bsgrp_{idx}"
        while temp_col in existing_columns:
            temp_col = f"_{temp_col}"
        existing_columns.add(temp_col)

        shared_func = create_shared_bootstrap_func(group_metrics)
        if boot.quantiles is None:
            return_type = T.MapType(
                T.StringType(),
                T.ArrayType(T.FloatType()),
            )
        else:
            return_type = T.MapType(T.StringType(), T.FloatType())
        func_pd = pandas_udf(shared_func, return_type)
        func_list.append(func_pd(*input_field_names).alias(temp_col))
        expansions.append((temp_col, group_metrics))

    return func_list, expansions


def _materialize_shared_bootstrap_columns(sdf, expansions):
    """Build per-metric output columns from shared bootstrap MapType temps.

    For shared quantile bootstrap groups, the temporary map contains flattened
    keys (e.g., ``metric_name_0.5``) across all metrics in the group. This
    function reconstructs one output column per metric:

    - quantiles is None: output is an ArrayType column with raw samples.
    - quantiles set: output is a MapType column for that metric only.

    Downstream post-processing can then honor ``unpack_results`` consistently.
    """
    for temp_col, group_metrics in expansions:
        for metric in group_metrics:
            name = metric.output_field_name
            quantiles = metric.bootstrap.quantiles
            if quantiles is None:
                sdf = sdf.withColumn(name, F.col(temp_col).getItem(name))
            else:
                # Dedupe exactly as derive_map_key_list does. A repeated
                # quantile (e.g. [0.5, 0.50]) collapses to one entry in the
                # dict the UDF returns, but F.create_map would emit the key
                # twice and Spark's default mapKeyDedupPolicy=EXCEPTION then
                # raises DUPLICATED_MAP_KEY at collect time.
                keys = list(dict.fromkeys(
                    bootstrap_quantile_key(name, q) for q in quantiles
                ))
                key_value_pairs = []
                for key in keys:
                    key_value_pairs.extend([
                        F.lit(key),
                        F.col(temp_col).getItem(key),
                    ])
                sdf = sdf.withColumn(name, F.create_map(*key_value_pairs))
        sdf = sdf.drop(temp_col)
    return sdf


def apply_aggregation_metrics(
    gp: GroupedData,
    include_metrics: List[MetricsBasemodel] = None
) -> DataFrame:
    """Apply metrics to grouped Spark data and return an aggregated DataFrame."""
    if not isinstance(include_metrics, List):
        include_metrics = [include_metrics]

    # Separate metrics that can share bootstrap samples from everything else.
    no_boot_metrics, boot_groups = partition_metrics_by_bootstrap(include_metrics)

    func_list = []

    # Non-bootstrap metrics.
    for model in no_boot_metrics:
        func_list.append(_build_non_bootstrap_udf(model, gp))

    # Bootstrap groups (shared-sample path for quantile or raw-array metrics).
    boot_func_list, expansions = _build_shared_bootstrap_udfs(boot_groups, gp)
    func_list.extend(boot_func_list)

    sdf = gp.agg(*func_list)

    # Materialize one output column per metric from shared temp maps.
    if expansions:
        sdf = _materialize_shared_bootstrap_columns(sdf, expansions)

    return sdf
