"""Vectorized (numpy-batched) shared-bootstrap metric evaluation.

``bootstrap_funcs.create_shared_bootstrap_func`` builds one pandas_udf that
evaluates every metric sharing a bootstrap config once per replicate, via a
plain Python ``for rep in range(reps): ...`` loop (through
``arch.bootstrap.IIDBootstrap.apply``). At scale (many groups x many
replicates x several metrics), that loop's Python/numpy dispatch overhead
dominates wall-clock time -- not the actual arithmetic.

This module replaces the *metric-evaluation* half of that loop with vectorized
numpy operations across all replicates at once. It deliberately does **not**
change how resample indices are drawn: instead of re-deriving the RNG
internals of each bootstrap method, it calls the real
``arch.bootstrap.IIDBootstrap`` subclass's own ``update_indices()`` in a
lightweight per-replicate loop (see ``build_index_matrix``). That loop is
bit-identical to the legacy path by construction (same object, same method,
same RNG state) and is cheap on its own -- the expense in the legacy code
was never index construction, it was calling N metric functions per
replicate. Vectorizing just the metric evaluation removes that cost while
keeping index generation provably correct.

Coverage is intentionally narrow and explicit:

- ``VECTORIZED_BOOTSTRAP_METHODS``: which bootstrap resampler classes this
  supports. ``Gumboot`` is a from-scratch implementation with a different
  indices/resample contract and is NOT covered here -- it always falls back
  to the legacy loop in ``create_shared_bootstrap_func``.
- ``VECTORIZED_METRIC_FUNCS``: which metric classes have a vectorized kernel.
  Any metric not in this registry (e.g. a future custom metric) falls back
  to the legacy per-rep scalar closure for that metric only, so adding new
  metrics never silently produces wrong numbers for unvectorized ones.

  This fallback really is per metric. It previously was not: the gate in
  ``bootstrap_funcs`` used ``all(...)``, so a single uncovered metric pushed
  its whole group onto the legacy loop -- measured at 78 ms/group for nine
  covered metrics versus 638 ms/group for the same nine plus one uncovered
  one. Since ``bootstrap_group_key`` deliberately excludes metric class,
  heterogeneous groups are the norm, so that cliff was easy to hit. The
  fallback shares one index matrix with the kernels, so a mixed group still
  evaluates every metric on identical draws.

Every kernel here mirrors the corresponding scalar closure in
``deterministic_funcs.py`` formula-for-formula. Behavioral equivalence with the
scalar path is the contract, so a kernel is never "improved" relative to its
closure; where a formula genuinely needed correcting, both sides were changed
together (see ``_vec_pearson_r`` on the ddof mismatch).
"""
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from teehr.metrics.models.base import MetricsBasemodel, TransformEnum
from teehr.querying.utils import bootstrap_quantile_key

EPSILON = 1e-6

# Bootstrap resampler classes covered by the vectorized path. Both are
# arch.bootstrap.IIDBootstrap subclasses whose update_indices() only depends
# on the object's own RNG state (no extra per-call arguments), which is what
# build_index_matrix relies on.
VECTORIZED_BOOTSTRAP_METHODS = {"Stationary", "CircularBlock"}


def build_index_matrix(bs: Any, reps: int) -> np.ndarray:
    """Build a ``(reps, n)`` resample index matrix.

    Calls ``bs.update_indices()`` (the real arch bootstrap object's own
    method) once per replicate. This is bit-identical to the indices arch's
    own ``bootstrap()``/``apply()`` would produce for the same object and
    seed, since it's the literal same method call in the same order -- but
    decoupled from metric evaluation, so it stays cheap even at reps=1000+.
    """
    return np.stack([np.asarray(bs.update_indices()) for _ in range(reps)])


def _apply_transform_1d(
    x: np.ndarray,
    transform: TransformEnum,
    add_epsilon: bool,
) -> np.ndarray:
    """Apply one transform to one matrix, mirroring the scalar ``match`` block.

    Shared by the two-field and single-field transforms so the branch list
    cannot drift between them. Never mutates ``x``.
    """
    if transform == TransformEnum.log:
        if add_epsilon:
            x = x + EPSILON
        return np.log(x)
    elif transform == TransformEnum.sqrt:
        return np.sqrt(x)
    elif transform == TransformEnum.square:
        return np.square(x)
    elif transform == TransformEnum.cube:
        return np.power(x, 3)
    elif transform == TransformEnum.exp:
        return np.exp(x)
    elif transform == TransformEnum.inv:
        if add_epsilon:
            x = x + EPSILON
        return 1.0 / x
    elif transform == TransformEnum.abs:
        return np.abs(x)
    raise ValueError(f"Unsupported transform: {transform}")


def _vectorized_transform(
    p: np.ndarray,
    s: np.ndarray,
    model: MetricsBasemodel,
) -> tuple:
    """Row-wise equivalent of ``deterministic_funcs._transform`` for (reps, n) matrices.

    Uses NaN-masking instead of element-wise dropping (dropping would make
    rows ragged across replicates). Every vectorized metric kernel below
    uses NaN-aware reductions (``nanmean``/``nanstd``/``nanmedian``/
    ``nansum``), so masking is mathematically equivalent to the legacy
    per-replicate drop-then-compute behavior for all metrics covered here.

    The mask is **pairwise and unconditional**, mirroring
    ``deterministic_funcs._transform``. Both properties matter:

    - *Pairwise*, because the scalar path drops a whole row when either series
      is non-finite there. Masking only each series' own NaNs would let
      ``nanmean(p)`` and ``nanmean(s)`` reduce over different subsets, so e.g.
      ``relative_mean`` would divide two means computed from different rows.
    - *Unconditional*, because the scalar drop is no longer gated on a
      transform being set (it used to be, which is what made the two paths
      disagree on gappy data).
    """
    transform = getattr(model, "transform", None)
    add_epsilon = getattr(model, "add_epsilon", False)

    if transform is not None:
        p = _apply_transform_1d(p, transform, add_epsilon)
        s = _apply_transform_1d(s, transform, add_epsilon)

    invalid = ~(np.isfinite(p) & np.isfinite(s))
    if np.any(invalid):
        p = np.where(invalid, np.nan, p)
        s = np.where(invalid, np.nan, s)

    return p, s


def _vectorized_signature_transform(
    p: np.ndarray,
    model: MetricsBasemodel,
) -> np.ndarray:
    """Row-wise equivalent of ``signature_funcs._transform``.

    Signature metrics are single-field, so the mask depends on ``p`` alone --
    that is the only difference from ``_vectorized_transform``, which masks
    pairwise. Masking non-finite values is required rather than optional: the
    scalar path drops them (``signature_funcs._transform``), so leaving an inf
    in place would let ``nansum``/``nanmax`` see a value the scalar path never
    does.
    """
    transform = getattr(model, "transform", None)
    if transform is not None:
        p = _apply_transform_1d(
            p, transform, getattr(model, "add_epsilon", False)
        )

    invalid = ~np.isfinite(p)
    if np.any(invalid):
        p = np.where(invalid, np.nan, p)

    return p


def _finite_pair_count(p: np.ndarray, s: np.ndarray) -> np.ndarray:
    """Per-row count of positions finite in both series (or in ``p`` alone).

    This is the row-wise equivalent of ``len(p)`` *after* ``_transform``, which
    since the unconditional-drop fix is always the count of surviving pairs --
    so metrics dividing by ``len(...)`` (the ``_mean_error`` family) need this
    rather than the raw row width.
    """
    if s is None:
        return np.sum(np.isfinite(p), axis=1)
    return np.sum(np.isfinite(p) & np.isfinite(s), axis=1)


def _vec_pearson_r(p: np.ndarray, s: np.ndarray, add_epsilon: bool) -> np.ndarray:
    """Row-wise Pearson correlation, matching ``pearson_correlation_inner``.

    ``add_epsilon=False`` mirrors ``np.corrcoef(s, p)[0][1]``.
    ``add_epsilon=True`` mirrors
    ``np.cov(p, s, ddof=0)[0, 1] / (nanstd(p) * nanstd(s) + EPSILON)``.

    Both branches use a consistent ddof=0, so the two differ only by the
    ``+EPSILON`` divide-by-zero guard. An earlier version paired ``np.cov``'s
    default ddof=1 with ddof=0 standard deviations; those do not cancel and the
    result was ``r * n/(n-1)``, which exceeds 1.0 on small well-correlated
    samples. The mismatch was documented as intentional but produced a quantity
    that is not a correlation coefficient.
    """
    n = np.sum(np.isfinite(p) & np.isfinite(s), axis=1)
    p_mean = np.nanmean(p, axis=1, keepdims=True)
    s_mean = np.nanmean(s, axis=1, keepdims=True)
    dp = p - p_mean
    ds = s - s_mean
    cov_sum = np.nansum(dp * ds, axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        if add_epsilon:
            # ddof=0 (population covariance), matching np.cov(..., ddof=0) and
            # the ddof=0 nanstd denominator below.
            cov = cov_sum / np.maximum(n, 1)
            denom = np.nanstd(p, axis=1) * np.nanstd(s, axis=1) + EPSILON
            return cov / denom
        else:
            # np.corrcoef is ddof-invariant (any consistent ddof cancels).
            denom = np.sqrt(np.nansum(dp**2, axis=1) * np.nansum(ds**2, axis=1))
            return cov_sum / denom


def _vec_relative_mean(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    p_mean = np.nanmean(p, axis=1)
    s_mean = np.nanmean(s, axis=1)
    if model.add_epsilon:
        return s_mean / (p_mean + EPSILON)
    return s_mean / p_mean


def _vec_relative_median(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    p_med = np.nanmedian(p, axis=1)
    s_med = np.nanmedian(s, axis=1)
    if model.add_epsilon:
        return s_med / (p_med + EPSILON)
    return s_med / p_med


def _vec_relative_minimum(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    p_min = np.nanmin(p, axis=1)
    s_min = np.nanmin(s, axis=1)
    if model.add_epsilon:
        return s_min / (p_min + EPSILON)
    return s_min / p_min


def _vec_relative_maximum(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    p_max = np.nanmax(p, axis=1)
    s_max = np.nanmax(s, axis=1)
    if model.add_epsilon:
        return s_max / (p_max + EPSILON)
    return s_max / p_max


def _vec_relative_standard_deviation(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    p_std = np.nanstd(p, axis=1)
    s_std = np.nanstd(s, axis=1)
    if model.add_epsilon:
        return s_std / (p_std + EPSILON)
    return s_std / p_std


def _vec_relative_bias(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    diff_sum = np.nansum(s - p, axis=1)
    p_sum = np.nansum(p, axis=1)
    if model.add_epsilon:
        return diff_sum / (p_sum + EPSILON)
    return diff_sum / p_sum


def _vec_nse_parts(p, s, model) -> tuple:
    """Numerator, denominator and NaN guard shared by NSE and normalized NSE.

    The two scalar closures are identical up to their final expression, so
    sharing the parts keeps the guards from drifting apart.
    """
    # Legacy guards (per-row, before transform): empty or all-zero-sum rows -> NaN.
    n_valid = _finite_pair_count(p, s)
    p_sum_raw = np.nansum(p, axis=1)
    s_sum_raw = np.nansum(s, axis=1)
    guard_nan = (n_valid == 0) | (p_sum_raw == 0) | (s_sum_raw == 0)

    p, s = _vectorized_transform(p, s, model)
    numerator = np.nansum((p - s) ** 2, axis=1)
    p_mean = np.nanmean(p, axis=1, keepdims=True)
    denominator = np.nansum((p - p_mean) ** 2, axis=1)
    if model.add_epsilon:
        denominator = denominator + EPSILON

    return numerator, denominator, guard_nan | (denominator == 0)


def _vec_nash_sutcliffe_efficiency(p, s, model) -> np.ndarray:
    numerator, denominator, guard_nan = _vec_nse_parts(p, s, model)
    with np.errstate(invalid="ignore", divide="ignore"):
        result = 1.0 - numerator / denominator
    return np.where(guard_nan, np.nan, result)


def _vec_nash_sutcliffe_efficiency_normalized(p, s, model) -> np.ndarray:
    numerator, denominator, guard_nan = _vec_nse_parts(p, s, model)
    with np.errstate(invalid="ignore", divide="ignore"):
        # Written as the scalar closure writes it (1/(1 + num/den)) rather than
        # the algebraically equal 1/(2 - NSE): same value, but bit-identical to
        # deterministic_funcs and no second division.
        result = 1.0 / (1.0 + numerator / denominator)
    return np.where(guard_nan, np.nan, result)


def _vec_kling_gupta_efficiency(p, s, model) -> np.ndarray:
    # Legacy guard (pre-transform): zero std on either side -> NaN.
    guard_nan = (np.nanstd(s, axis=1) == 0) | (np.nanstd(p, axis=1) == 0)

    p, s = _vectorized_transform(p, s, model)
    r = _vec_pearson_r(p, s, add_epsilon=False)  # kge always uses plain corrcoef

    p_std = np.nanstd(p, axis=1)
    s_std = np.nanstd(s, axis=1)
    p_mean = np.nanmean(p, axis=1)
    s_mean = np.nanmean(s, axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        if model.add_epsilon:
            rel_var = s_std / (p_std + EPSILON)
            rel_mean = s_mean / (p_mean + EPSILON)
        else:
            rel_var = s_std / p_std
            rel_mean = s_mean / p_mean

    euclidean = np.sqrt(
        model.sr * (r - 1.0) ** 2
        + model.sa * (rel_var - 1.0) ** 2
        + model.sb * (rel_mean - 1.0) ** 2
    )
    result = 1.0 - euclidean
    return np.where(guard_nan, np.nan, result)


def _vec_pearson_correlation(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    return _vec_pearson_r(p, s, add_epsilon=model.add_epsilon)


def _vec_r_squared(p, s, model) -> np.ndarray:
    # r_squared_inner is pearson_correlation_inner with the result squared.
    p, s = _vectorized_transform(p, s, model)
    return _vec_pearson_r(p, s, add_epsilon=model.add_epsilon) ** 2


def _vec_mean_error_core(p, s, model, power=1.0, root=False) -> np.ndarray:
    """Row-wise ``deterministic_funcs._mean_error`` on transformed matrices.

    Takes ALREADY-transformed inputs, mirroring the scalar helper, which its
    callers likewise invoke after ``_transform``.

    The denominator is the finite-pair count, not the row width: the scalar
    helper divides by ``len(y_true)`` *after* ``_transform`` has dropped
    non-finite pairs.
    """
    with np.errstate(invalid="ignore", divide="ignore"):
        me = (
            np.nansum(np.abs(p - s) ** power, axis=1)
            / _finite_pair_count(p, s)
        )
    return np.sqrt(me) if root else me


def _vec_mean_error(p, s, model) -> np.ndarray:
    # mean_error_inner uses np.sum(s - p)/len(p) directly, NOT _mean_error --
    # note it is signed, and s - p rather than |p - s|.
    p, s = _vectorized_transform(p, s, model)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.nansum(s - p, axis=1) / _finite_pair_count(p, s)


def _vec_mean_absolute_error(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    return _vec_mean_error_core(p, s, model)


def _vec_mean_squared_error(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    return _vec_mean_error_core(p, s, model, power=2.0)


def _vec_root_mean_squared_error(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    return _vec_mean_error_core(p, s, model, power=2.0, root=True)


def _vec_root_mean_standard_deviation_ratio(p, s, model) -> np.ndarray:
    # Transforms ONCE then calls the core helper, mirroring
    # root_mean_standard_deviation_ratio_inner calling _root_mean_squared_error
    # (the helper that does no transform of its own). Delegating to
    # _vec_root_mean_squared_error instead would transform twice.
    p, s = _vectorized_transform(p, s, model)
    rmse = _vec_mean_error_core(p, s, model, power=2.0, root=True)
    p_std = np.nanstd(p, axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        if model.add_epsilon:
            return rmse / (p_std + EPSILON)
        return rmse / p_std


def _vec_mean_absolute_relative_error(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    numerator = np.nansum(np.abs(s - p), axis=1)
    p_sum = np.nansum(p, axis=1)          # np.sum(p): p only, not pairwise
    with np.errstate(invalid="ignore", divide="ignore"):
        if model.add_epsilon:
            return numerator / (p_sum + EPSILON)
        return numerator / p_sum


def _vec_max_value_delta(p, s, model) -> np.ndarray:
    p, s = _vectorized_transform(p, s, model)
    return np.nanmax(s, axis=1) - np.nanmax(p, axis=1)


def _vec_kling_gupta_efficiency_mod1(p, s, model) -> np.ndarray:
    # Legacy guard (pre-transform): zero std on either side -> NaN.
    guard_nan = (np.nanstd(s, axis=1) == 0) | (np.nanstd(p, axis=1) == 0)

    p, s = _vectorized_transform(p, s, model)
    r = _vec_pearson_r(p, s, add_epsilon=False)  # always plain corrcoef

    p_std = np.nanstd(p, axis=1)
    s_std = np.nanstd(s, axis=1)
    p_mean = np.nanmean(p, axis=1)
    s_mean = np.nanmean(s, axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        if model.add_epsilon:
            # Mod1's variability ratio is a ratio of coefficients of
            # variation, unlike kge's ratio of raw standard deviations.
            var_ratio = (
                (s_std / (s_mean + EPSILON)) / (p_std / (p_mean + EPSILON))
            )
            rel_mean = s_mean / (p_mean + EPSILON)
        else:
            var_ratio = (s_std / s_mean) / (p_std / p_mean)
            rel_mean = s_mean / p_mean

    euclidean = np.sqrt(
        model.sr * (r - 1.0) ** 2
        + model.sa * (var_ratio - 1.0) ** 2
        + model.sb * (rel_mean - 1.0) ** 2
    )
    return np.where(guard_nan, np.nan, 1.0 - euclidean)


def _vec_kling_gupta_efficiency_mod2(p, s, model) -> np.ndarray:
    # Legacy guard (pre-transform): zero std on either side -> NaN.
    guard_nan = (np.nanstd(s, axis=1) == 0) | (np.nanstd(p, axis=1) == 0)

    p, s = _vectorized_transform(p, s, model)
    r = _vec_pearson_r(p, s, add_epsilon=False)  # always plain corrcoef

    p_std = np.nanstd(p, axis=1)
    s_std = np.nanstd(s, axis=1)
    p_mean = np.nanmean(p, axis=1)
    s_mean = np.nanmean(s, axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        if model.add_epsilon:
            rel_var = s_std / (p_std + EPSILON)
            bias = ((s_mean - p_mean) ** 2) / ((p_std ** 2) + EPSILON)
        else:
            rel_var = s_std / p_std
            bias = ((s_mean - p_mean) ** 2) / (p_std ** 2)

    euclidean = np.sqrt(
        model.sr * (r - 1.0) ** 2
        + model.sa * (rel_var - 1.0) ** 2
        + model.sb * bias          # NOT squared, unlike the other two terms
    )
    return np.where(guard_nan, np.nan, 1.0 - euclidean)


# --- Signature kernels -----------------------------------------------------
#
# Single-field: `s` is None (see compute_vectorized_shared_bootstrap) and is
# accepted only to keep one uniform kernel signature across the registry.
# bootstrap_group_key includes the input-field tuple, so signature metrics form
# their own groups and can never be mixed with two-field ones.

def _vec_count(p, s, model) -> np.ndarray:
    """Row-wise ``len(p)`` after the drop.

    Near-degenerate under fixed-size resampling: every draw has the same
    length, so this varies only with how many non-finite positions a draw
    happens to hit. Registered anyway because the engine gate is per group --
    leaving it out would drag a group like {Count, Average, Maximum} entirely
    onto the per-replicate loop.
    """
    return _finite_pair_count(
        _vectorized_signature_transform(p, model), None
    ).astype(float)


def _vec_minimum(p, s, model) -> np.ndarray:
    return np.nanmin(_vectorized_signature_transform(p, model), axis=1)


def _vec_maximum(p, s, model) -> np.ndarray:
    return np.nanmax(_vectorized_signature_transform(p, model), axis=1)


def _vec_average(p, s, model) -> np.ndarray:
    return np.nanmean(_vectorized_signature_transform(p, model), axis=1)


def _vec_sum(p, s, model) -> np.ndarray:
    return np.nansum(_vectorized_signature_transform(p, model), axis=1)


def _vec_variance(p, s, model) -> np.ndarray:
    # np.var(Series) dispatches to pandas with ddof=0, so nanvar matches.
    return np.nanvar(_vectorized_signature_transform(p, model), axis=1)


# Registry: metric class name -> vectorized kernel(p_mat, s_mat, model) -> (reps,) array.
VECTORIZED_METRIC_FUNCS = {
    "RelativeMean": _vec_relative_mean,
    "RelativeMedian": _vec_relative_median,
    "RelativeMinimum": _vec_relative_minimum,
    "RelativeMaximum": _vec_relative_maximum,
    "RelativeStandardDeviation": _vec_relative_standard_deviation,
    "RelativeBias": _vec_relative_bias,
    "NashSutcliffeEfficiency": _vec_nash_sutcliffe_efficiency,
    "KlingGuptaEfficiency": _vec_kling_gupta_efficiency,
    "PearsonCorrelation": _vec_pearson_correlation,
    # --- Deterministic, row-wise reductions ---
    "MeanError": _vec_mean_error,
    "MeanAbsoluteError": _vec_mean_absolute_error,
    "MeanSquareError": _vec_mean_squared_error,
    "RootMeanSquareError": _vec_root_mean_squared_error,
    "RootMeanStandardDeviationRatio": _vec_root_mean_standard_deviation_ratio,
    "MeanAbsoluteRelativeError": _vec_mean_absolute_relative_error,
    # multiplicative_bias_inner and relative_mean_inner are the same formula.
    "MultiplicativeBias": _vec_relative_mean,
    "Rsquared": _vec_r_squared,
    "NormalizedNashSutcliffeEfficiency": (
        _vec_nash_sutcliffe_efficiency_normalized
    ),
    "KlingGuptaEfficiencyMod1": _vec_kling_gupta_efficiency_mod1,
    "KlingGuptaEfficiencyMod2": _vec_kling_gupta_efficiency_mod2,
    "MaxValueDelta": _vec_max_value_delta,
    # --- Signature (single-field; the kernel's `s` argument is None) ---
    "Count": _vec_count,
    "Minimum": _vec_minimum,
    "Maximum": _vec_maximum,
    "Average": _vec_average,
    "Sum": _vec_sum,
    "Variance": _vec_variance,
}


def is_vectorized_metric(metric: MetricsBasemodel) -> bool:
    """Whether this metric has a vectorized kernel in the registry."""
    return type(metric).__name__ in VECTORIZED_METRIC_FUNCS


def resample_args(args: tuple, indices: np.ndarray) -> tuple:
    """One replicate's args, as ``arch.IIDBootstrap._resample`` builds them.

    pandas inputs are resampled with ``.iloc``, which **preserves the original
    (now duplicated) index labels** -- verified against ``bs.apply`` to match
    on values, index and type. Several scalar closures depend on pandas
    semantics rather than plain arrays (``Series.idxmax`` in
    ``deterministic_funcs.max_value_timedelta``, ``Series.sort_values`` in
    ``signature_funcs.flow_duration_curve_slope``, and the boolean-mask
    indexing in ``_transform``), so both the type and the index have to match
    or the fallback would not be equivalent to the legacy path.
    """
    return tuple(
        a.iloc[indices] if isinstance(a, (pd.Series, pd.DataFrame))
        else np.asarray(a)[indices]
        for a in args
    )


def assemble_bootstrap_output(
    output_names: List[str],
    values: np.ndarray,
    quantiles,
) -> Dict[str, Any]:
    """Format a ``(reps, n_metrics)`` result matrix as the UDF's return dict.

    Shared by the vectorized and legacy paths so the two cannot drift in the
    keys or ordering they emit.

    Parameters
    ----------
    output_names : list of str
        One name per column of *values*.
    values : np.ndarray
        ``(reps, n_metrics)`` replicate results.
    quantiles : list or None
        Quantile-keyed floats when set; raw per-replicate lists when None.
    """
    combined: Dict[str, Any] = {}
    for i, name in enumerate(output_names):
        column = np.asarray(values[:, i], dtype=float)
        if quantiles is None:
            combined[name] = column.tolist()
        else:
            q_values = np.quantile(column, quantiles)
            for q, v in zip(quantiles, q_values):
                combined[bootstrap_quantile_key(name, q)] = v
    return combined


def compute_vectorized_shared_bootstrap(
    metrics: List[MetricsBasemodel],
    metric_funcs: List[Any],
    args: tuple,
    bs: Any,
    reps: int,
    quantiles,
    max_matrix_cells: int = 20_000_000,
) -> Dict[str, Any]:
    """Vectorized equivalent of the per-rep loop inside ``create_shared_bootstrap_func``.

    Metrics **with** a kernel are evaluated by it on the whole ``(reps, n)``
    batch; metrics **without** one fall back to their own scalar closure,
    called once per replicate. Both consume the *same* index matrix, so every
    metric in the group still sees identical draws -- which is the entire point
    of a shared bootstrap. ``bs.apply`` is deliberately not called here, so
    there is no second RNG consumer that could let the two subsets diverge.

    Parameters
    ----------
    metrics : list
        Metrics sharing the same bootstrap config. At least one must have a
        kernel (see ``bootstrap_funcs._can_use_vectorized_engine``); the rest
        are handled by the fallback loop.
    metric_funcs : list
        The scalar closures for *metrics*, positionally aligned, built once at
        UDF-creation time. Passed in rather than derived here so the closure
        factories are not re-run for every Spark group.
    args : tuple
        The series passed to the UDF, in the same order used to build ``bs``.
        ``args[0]`` is primary; ``args[1]``, when present, is secondary.
    bs : arch.bootstrap.IIDBootstrap subclass instance
        Already-constructed bootstrap object (the same one the legacy path
        would use) -- reused here purely for ``update_indices()`` and its RNG
        state.
    reps : int
        Number of bootstrap replicates.
    quantiles : list or None
        If set, return per-metric quantile dict entries; if None, return
        raw per-replicate arrays (matching the legacy raw-array contract).
    max_matrix_cells : int, optional
        Cap on ``reps * n`` per batch. ``idx``, ``p_mat`` and ``s_mat`` are
        each this size, so an uncapped call at large n would allocate three
        big matrices inside a Spark executor, per concurrent task. Chunking is
        bit-identical because ``build_index_matrix`` draws sequentially, so
        chunk boundaries do not perturb the RNG stream.

    Returns
    -------
    dict
        Same shape/contract as ``create_shared_bootstrap_func``'s returned
        UDF: quantile-keyed floats when ``quantiles`` is set, else a raw
        list of floats per metric name.
    """
    p_arr = np.asarray(args[0], dtype=float)
    s_arr = np.asarray(args[1], dtype=float) if len(args) > 1 else None
    n = p_arr.shape[0]

    covered = [
        (i, m, VECTORIZED_METRIC_FUNCS[type(m).__name__])
        for i, m in enumerate(metrics)
        if is_vectorized_metric(m)
    ]
    fallback = [
        (i, metric_funcs[i])
        for i, m in enumerate(metrics)
        if not is_vectorized_metric(m)
    ]

    # Mirrors arch's own np.zeros((reps, num_params)) result buffer.
    values = np.empty((reps, len(metrics)), dtype=float)
    chunk = max(1, min(reps, max_matrix_cells // max(n, 1)))

    done = 0
    while done < reps:
        k = min(chunk, reps - done)
        idx = build_index_matrix(bs, k)  # (k, n), same draw order as bs.apply
        if covered:
            p_mat = p_arr[idx]
            s_mat = s_arr[idx] if s_arr is not None else None
            for i, metric, kernel in covered:
                values[done:done + k, i] = kernel(p_mat, s_mat, metric)
        for r in range(k):
            if not fallback:
                break
            draw = resample_args(args, idx[r])
            for i, fn in fallback:
                values[done + r, i] = fn(*draw)
        done += k

    return assemble_bootstrap_output(
        [m.output_field_name for m in metrics], values, quantiles
    )
