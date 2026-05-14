"""Execution adapter primitives for calculated-field planning.

Adapters let planners orchestrate ordering and batching while delegating
execution details to engine-specific handlers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame

from teehr.calculated_fields.models.base import CalculatedFieldBaseModel


Batch = list[CalculatedFieldBaseModel]
"""Contiguous calculated-field models consumed by one adapter invocation."""

ConsumeBatch = Callable[[list[CalculatedFieldBaseModel], int], tuple[Batch, int]]
"""Batch selector returning (batch, next_index) from ordered field models."""

ApplyBatch = Callable[[DataFrame, Batch], DataFrame]
"""Batch executor that applies selected calculated fields to a DataFrame."""

SupportsField = Callable[[CalculatedFieldBaseModel], bool]
"""Predicate indicating whether an adapter can execute a field model."""


@dataclass(frozen=True)
class CalculatedFieldAdapter:
    """Routing unit used by planners for calculated-field execution.

    A planner walks calculated fields in user-provided order and selects the
    first adapter whose ``supports`` predicate matches the current field. The
    adapter then:

    1. Uses ``consume_batch`` to select a contiguous compatible run beginning
       at the current index.
    2. Uses ``apply_batch`` to apply that run to the DataFrame.

    Attributes
    ----------
    name : str
        Human-readable identifier used for debugging and traceability.
    supports : SupportsField
        Predicate that determines whether this adapter can execute a field.
    consume_batch : ConsumeBatch
        Function that selects which contiguous fields this adapter will
        execute in one pass.
    apply_batch : ApplyBatch
        Function that applies the selected batch and returns the resulting
        DataFrame.
    """

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
