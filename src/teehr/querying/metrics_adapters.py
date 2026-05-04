"""Execution adapter primitives for metric planning.

Adapters let a planner orchestrate ordering and engine fallback while
delegating family-specific metric execution details.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame

from teehr.models.metrics.basemodels import MetricsBasemodel


MetricBatch = list[MetricsBasemodel]
SupportsMetric = Callable[[MetricsBasemodel], bool]
ConsumeMetricBatch = Callable[[list[MetricsBasemodel], int], tuple[MetricBatch, int]]
ApplyMetricBatch = Callable[[DataFrame, list[str], MetricBatch], DataFrame | None]


@dataclass(frozen=True)
class MetricExecutionAdapter:
    """Engine adapter that consumes and executes one contiguous metric batch."""

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