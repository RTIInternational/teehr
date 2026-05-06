"""Execution adapter primitives for metric planning.

Adapters let a planner orchestrate ordering and engine fallback while
delegating family-specific metric execution details.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame

from teehr.metrics.base_models import MetricsBasemodel


MetricBatch = list[MetricsBasemodel]
"""Contiguous metric models consumed by one adapter invocation."""

SupportsMetric = Callable[[MetricsBasemodel], bool]
"""Predicate indicating whether an adapter can execute a metric model."""

ConsumeMetricBatch = Callable[[list[MetricsBasemodel], int], tuple[MetricBatch, int]]
"""Batch selector returning (batch, next_index) from an ordered metric list."""

ApplyMetricBatch = Callable[[DataFrame, list[str], MetricBatch], DataFrame | None]
"""Batch executor producing grouped metric outputs for selected metrics."""


@dataclass(frozen=True)
class MetricExecutionAdapter:
    """Routing unit used by metric planners to execute compatible metric runs.

    A planner iterates through the user-provided metric list in order and, for
    each position, chooses the first adapter whose ``supports`` predicate
    matches the current metric model. The adapter then:

    1. Uses ``consume_batch`` to select a contiguous run of compatible metrics
       starting at the current index.
    2. Uses ``apply_batch`` to compute those metrics against the input
       DataFrame and group-by columns.

    Attributes
    ----------
    name : str
        Human-readable identifier used for debugging and tracing.
    supports : SupportsMetric
        Predicate that determines whether this adapter can execute a metric.
    consume_batch : ConsumeMetricBatch
        Function that selects which contiguous metrics this adapter will handle
        in a single execution pass.
    apply_batch : ApplyMetricBatch
        Function that executes the selected batch and returns a DataFrame with
        grouped outputs for that batch, or ``None`` when no frame is produced.
    """

    name: str
    supports: SupportsMetric
    consume_batch: ConsumeMetricBatch
    apply_batch: ApplyMetricBatch


def _consume_single_metric(
    metrics: list[MetricsBasemodel],
    start: int,
) -> tuple[MetricBatch, int]:
    return [metrics[start]], start + 1


def _consume_contiguous_supported(
    metrics: list[MetricsBasemodel],
    start: int,
    supports: SupportsMetric,
) -> tuple[MetricBatch, int]:
    batch = [metrics[start]]
    idx = start + 1
    while idx < len(metrics) and supports(metrics[idx]):
        batch.append(metrics[idx])
        idx += 1
    return batch, idx


def single_metric_adapter(
    *,
    name: str,
    supports: SupportsMetric,
    apply_metric: Callable[[DataFrame, list[str], MetricsBasemodel], DataFrame | None],
) -> MetricExecutionAdapter:
    """Create an adapter that executes one metric model at a time."""

    def _apply_batch(
        sdf: DataFrame,
        group_by_cols: list[str],
        batch: MetricBatch,
    ) -> DataFrame | None:
        return apply_metric(sdf, group_by_cols, batch[0])

    return MetricExecutionAdapter(
        name=name,
        supports=supports,
        consume_batch=_consume_single_metric,
        apply_batch=_apply_batch,
    )


def contiguous_batch_adapter(
    *,
    name: str,
    supports: SupportsMetric,
    apply_batch: ApplyMetricBatch,
) -> MetricExecutionAdapter:
    """Create an adapter that executes contiguous runs of compatible metrics."""

    def _consume(metrics: list[MetricsBasemodel], start: int) -> tuple[MetricBatch, int]:
        return _consume_contiguous_supported(metrics, start, supports)

    return MetricExecutionAdapter(
        name=name,
        supports=supports,
        consume_batch=_consume,
        apply_batch=apply_batch,
    )
