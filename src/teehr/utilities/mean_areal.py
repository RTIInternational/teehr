"""Mean areal values of a lazily opened grid, read one block at a time.

Built for long records over many polygons: the caller splits the grid into
chunk-aligned blocks of steps and runs :func:`mean_areal_block` on each, in
one process or on Spark executors. Memory is bounded by reading the block's
window in bands of grid rows and averaging in slices of steps.
"""
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, Optional, Sequence

import numpy as np
import pyarrow as pa
import xarray as xr
from scipy import sparse

from teehr.fetching.const import LOCATION_ID, VALUE
from teehr.fetching.nwm.grid_utils import (
    weighted_means_from_sums,
    weighted_sums_from_matrix,
)


@dataclass(frozen=True)
class GridWeights:
    """Zone-by-pixel weights, as returned by ``build_weights_matrix``.

    ``rows``/``cols`` give each matrix column's pixel in the full grid, and
    ``location_ids`` each matrix row's zone.
    """

    matrix: sparse.csr_matrix
    rows: np.ndarray
    cols: np.ndarray
    location_ids: np.ndarray


def mean_areal_block(
    da: xr.DataArray,
    weights: GridWeights,
    y_dim: str,
    x_dim: str,
    step_columns: Dict[str, np.ndarray],
    band_edges: Optional[Sequence[int]] = None,
    max_slice_steps: int = 256,
    min_coverage: float = 1.0,
    scale: float = 1.0,
    offset: float = 0.0,
    constants: Optional[Dict[str, object]] = None,
    schema: Optional[pa.Schema] = None,
) -> pa.Table:
    """Mean areal values of one block of grid steps, as long rows.

    Parameters
    ----------
    da : xr.DataArray
        The block, lazily opened: ``y_dim``, ``x_dim`` and any step dims
        (e.g. ``time``, or ``reference_time``, ``member``, ``lead_time``).
        Only the window holding weighted pixels is read.
    weights : GridWeights
        Weights indexing the full grid that ``da`` is a block of.
    y_dim, x_dim : str
        The spatial dims.
    step_columns : Dict[str, np.ndarray]
        Output columns describing each step, e.g. ``value_time``. Steps are
        ``da``'s non-spatial dims, in ``da.dims`` order, flattened in C order.
    band_edges : Sequence[int], optional
        Grid row edges of the bands read one at a time, ideally on chunk
        boundaries. Defaults to one band spanning the weighted pixels.
    max_slice_steps : int
        Steps averaged at once, which bounds the float64 copies made while
        averaging.
    min_coverage : float
        See ``weighted_average_from_matrix``.
    scale, offset : float
        Applied to the means (``value * scale + offset``), e.g. a unit
        conversion. Exact, since a coverage-weighted mean is linear.
    constants : Dict[str, object], optional
        Columns with one value for every row, e.g. ``variable_name``.
    schema : pa.Schema, optional
        Output schema, e.g. the target table's: columns are ordered and cast
        to it, and missing nullable columns are filled with nulls.

    Returns
    -------
    pa.Table
        One row per step and zone with a value; NaN means are dropped.
    """
    step_dims = [d for d in da.dims if d not in (y_dim, x_dim)]
    da = da.transpose(*step_dims, y_dim, x_dim)
    n_steps = int(np.prod([da.sizes[d] for d in step_dims], dtype="int64"))
    for name, column in step_columns.items():
        if len(column) != n_steps:
            raise ValueError(
                f"step_columns['{name}'] has {len(column)} values for "
                f"{n_steps} steps."
            )

    matrix = weights.matrix.tocsc()
    n_zones = matrix.shape[0]
    total_weight = np.asarray(matrix.sum(axis=1)).ravel()
    sums = np.zeros((n_zones, n_steps))
    missing_weight = np.zeros((n_zones, n_steps))

    rows, cols = weights.rows, weights.cols
    if band_edges is None:
        band_edges = [int(rows.min()), int(rows.max()) + 1]
    for r0, r1 in zip(band_edges[:-1], band_edges[1:]):
        in_band = np.flatnonzero((rows >= r0) & (rows < r1))
        if in_band.size == 0:
            continue
        band_rows, band_cols = rows[in_band], cols[in_band]
        c0, c1 = int(band_cols.min()), int(band_cols.max()) + 1
        window = da.isel({y_dim: slice(r0, r1), x_dim: slice(c0, c1)}).values
        pixels = window.reshape(n_steps, r1 - r0, c1 - c0)[
            :, band_rows - r0, band_cols - c0
        ]
        del window
        band_matrix = matrix[:, in_band]
        for s in range(0, n_steps, max_slice_steps):
            part_sums, part_missing = weighted_sums_from_matrix(
                band_matrix, pixels[s:s + max_slice_steps]
            )
            sums[:, s:s + max_slice_steps] += part_sums
            missing_weight[:, s:s + max_slice_steps] += part_missing

    values = weighted_means_from_sums(
        sums, missing_weight, total_weight, min_coverage
    ).ravel() * scale + offset
    keep = ~np.isnan(values)
    columns = {
        name: np.repeat(np.asarray(column), n_zones)[keep]
        for name, column in step_columns.items()
    }
    columns[LOCATION_ID] = np.tile(weights.location_ids, n_steps)[keep]
    columns[VALUE] = values[keep]
    table = pa.table(columns)
    for name, value in (constants or {}).items():
        table = table.append_column(name, pa.repeat(value, table.num_rows))
    return table if schema is None else _conform(table, schema)


@dataclass(frozen=True)
class BlockTask:
    """One block of a grid: ``isel`` positions of its steps, and their columns.

    See ``mean_areal_block`` for ``step_columns``.
    """

    selection: Dict[str, object]
    step_columns: Dict[str, np.ndarray]


def run_block_task(
    task: BlockTask, da: xr.DataArray, weights: GridWeights, **kwargs
) -> pa.Table:
    """``mean_areal_block`` of one task's block of ``da``."""
    return mean_areal_block(
        da.isel(task.selection), weights, step_columns=task.step_columns,
        **kwargs
    )


def arrow_task_runner(
    tasks: Sequence[BlockTask],
    da: xr.DataArray,
    weights: GridWeights,
    **kwargs,
) -> Callable[[Iterator[pa.RecordBatch]], Iterator[pa.RecordBatch]]:
    """A Spark ``mapInArrow`` function running the tasks named by a ``task`` column.

    ``da`` is a lazily opened grid, which pickles with its store (zarr over
    obstore, or an Icechunk read-only session), so executors read the source
    directly and need nothing beyond teehr's dependencies.
    """
    def run(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            for i in batch.column("task").to_pylist():
                yield from run_block_task(tasks[i], da, weights, **kwargs).to_batches()
    return run


def _conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Order and cast ``table``'s columns to ``schema``."""
    arrays = []
    for field in schema:
        if field.name in table.column_names:
            arrays.append(table[field.name].cast(field.type))
        elif field.nullable:
            arrays.append(pa.nulls(table.num_rows, field.type))
        else:
            raise ValueError(f"No values for non-nullable column '{field.name}'.")
    return pa.Table.from_arrays(arrays, schema=schema)
