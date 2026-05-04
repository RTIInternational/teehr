"""Execution adapter primitives for calculated-field planning.

Adapters let planners orchestrate ordering and batching while delegating
execution details to engine-specific handlers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame

from teehr.models.calculated_fields.base import CalculatedFieldBaseModel


Batch = list[CalculatedFieldBaseModel]
ConsumeBatch = Callable[[list[CalculatedFieldBaseModel], int], tuple[Batch, int]]
ApplyBatch = Callable[[DataFrame, Batch], DataFrame]
SupportsField = Callable[[CalculatedFieldBaseModel], bool]


@dataclass(frozen=True)
class CalculatedFieldAdapter:
    """Engine adapter that can consume and execute one contiguous batch."""

    name: str
    supports: SupportsField
    consume_batch: ConsumeBatch
    apply_batch: ApplyBatch


def _consume_single(
    cfs: list[CalculatedFieldBaseModel],
    start: int,
) -> tuple[Batch, int]:
    return [cfs[start]], start + 1


def single_field_adapter(
    *,
    name: str,
    supports: SupportsField,
    apply_field: Callable[[DataFrame, CalculatedFieldBaseModel], DataFrame],
) -> CalculatedFieldAdapter:
    """Create an adapter that executes one calculated field at a time."""

    def _apply_batch(sdf: DataFrame, batch: Batch) -> DataFrame:
        return apply_field(sdf, batch[0])

    return CalculatedFieldAdapter(
        name=name,
        supports=supports,
        consume_batch=_consume_single,
        apply_batch=_apply_batch,
    )


def batched_adapter(
    *,
    name: str,
    supports: SupportsField,
    consume_batch: ConsumeBatch,
    apply_batch: ApplyBatch,
) -> CalculatedFieldAdapter:
    """Create an adapter that can consume/execute contiguous compatible batches."""
    return CalculatedFieldAdapter(
        name=name,
        supports=supports,
        consume_batch=consume_batch,
        apply_batch=apply_batch,
    )