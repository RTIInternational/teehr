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

Every kernel here mirrors the corresponding scalar closure in
``deterministic_funcs.py`` formula-for-formula, including quirks (e.g.
``pearson_correlation``'s ``add_epsilon`` branch mixes ``np.cov`` (ddof=1)
with ``np.std``/``nanstd`` (ddof=0) -- that mismatch is preserved exactly,
not "fixed", since the goal is behavioral equivalence with the existing
implementation, not a cleaner formula).
"""
from typing import Any, Dict, List

import numpy as np

from teehr.metrics.models.base import MetricsBasemodel, TransformEnum

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
    """
    transform = getattr(model, "transform", None)
    if transform is None:
        return p, s

    add_epsilon = getattr(model, "add_epsilon", False)

    if transform == TransformEnum.log:
        if add_epsilon:
            p = p + EPSILON
            s = s + EPSILON
        p = np.log(p)
        s = np.log(s)
    elif transform == TransformEnum.sqrt:
        p = np.sqrt(p)
        s = np.sqrt(s)
    elif transform == TransformEnum.square:
        p = np.square(p)
        s = np.square(s)
    elif transform == TransformEnum.cube:
        p = np.power(p, 3)
        s = np.power(s, 3)
    elif transform == TransformEnum.exp:
        p = np.exp(p)
        s = np.exp(s)
    elif transform == TransformEnum.inv:
        if add_epsilon:
            p = p + EPSILON
            s = s + EPSILON
        p = 1.0 / p
        s = 1.0 / s
    elif transform == TransformEnum.abs:
        p = np.abs(p)
        s = np.abs(s)
    else:
        raise ValueError(f"Unsupported transform: {transform}")

    invalid = ~(np.isfinite(p) & np.isfinite(s))
    if np.any(invalid):
        p = np.where(invalid, np.nan, p)
        s = np.where(invalid, np.nan, s)

    return p, s


def _vec_pearson_r(p: np.ndarray, s: np.ndarray, add_epsilon: bool) -> np.ndarray:
    """Row-wise Pearson correlation, matching ``pearson_correlation_inner``.

    ``add_epsilon=False`` mirrors ``np.corrcoef(s, p)[0][1]`` (a single
    consistent-ddof formula). ``add_epsilon=True`` mirrors
    ``np.cov(p, s)[0, 1] / (nanstd(p) * nanstd(s) + EPSILON)`` -- note the
    ddof=1 (cov) vs ddof=0 (std) mismatch is intentional, preserved as-is.
    """
    n = np.sum(np.isfinite(p) & np.isfinite(s), axis=1)
    p_mean = np.nanmean(p, axis=1, keepdims=True)
    s_mean = np.nanmean(s, axis=1, keepdims=True)
    dp = p - p_mean
    ds = s - s_mean
    cov_sum = np.nansum(dp * ds, axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        if add_epsilon:
            # np.cov default ddof=1 (sample covariance).
            cov = cov_sum / np.maximum(n - 1, 1)
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


def _vec_nash_sutcliffe_efficiency(p, s, model) -> np.ndarray:
    # Legacy guards (per-row, before transform): empty or all-zero-sum rows -> NaN.
    n_valid = np.sum(np.isfinite(p) & np.isfinite(s), axis=1)
    p_sum_raw = np.nansum(p, axis=1)
    s_sum_raw = np.nansum(s, axis=1)
    guard_nan = (n_valid == 0) | (p_sum_raw == 0) | (s_sum_raw == 0)

    p, s = _vectorized_transform(p, s, model)
    numerator = np.nansum((p - s) ** 2, axis=1)
    p_mean = np.nanmean(p, axis=1, keepdims=True)
    denominator = np.nansum((p - p_mean) ** 2, axis=1)
    if model.add_epsilon:
        denominator = denominator + EPSILON

    with np.errstate(invalid="ignore", divide="ignore"):
        result = 1.0 - numerator / denominator
    result = np.where(guard_nan | (denominator == 0), np.nan, result)
    return result


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
}


def compute_vectorized_shared_bootstrap(
    metrics: List[MetricsBasemodel],
    args: tuple,
    bs: Any,
    reps: int,
    quantiles,
) -> Dict[str, Any]:
    """Vectorized equivalent of the per-rep loop inside ``create_shared_bootstrap_func``.

    Parameters
    ----------
    metrics : list
        Metrics sharing the same bootstrap config, all present in
        ``VECTORIZED_METRIC_FUNCS`` (callers must check this beforehand).
    args : tuple
        The (primary_value, secondary_value) series/arrays passed to the UDF,
        in the same order used to build ``bs``.
    bs : arch.bootstrap.IIDBootstrap subclass instance
        Already-constructed bootstrap object (same as the legacy path's
        ``_make_bs_object`` result) -- reused here purely for its
        ``update_indices()`` method and RNG state.
    reps : int
        Number of bootstrap replicates.
    quantiles : list or None
        If set, return per-metric quantile dict entries; if None, return
        raw per-replicate arrays (matching the legacy raw-array contract).

    Returns
    -------
    dict
        Same shape/contract as ``create_shared_bootstrap_func``'s returned
        UDF: quantile-keyed floats when ``quantiles`` is set, else a raw
        list of floats per metric name.
    """
    p_arr = np.asarray(args[0], dtype=float)
    s_arr = np.asarray(args[1], dtype=float)

    idx = build_index_matrix(bs, reps)  # (reps, n)
    p_mat = p_arr[idx]
    s_mat = s_arr[idx]

    combined: Dict[str, Any] = {}
    for metric in metrics:
        kernel = VECTORIZED_METRIC_FUNCS[type(metric).__name__]
        values = kernel(p_mat, s_mat, metric)
        name = metric.output_field_name
        if quantiles is None:
            combined[name] = np.asarray(values, dtype=float).tolist()
        else:
            q_values = np.quantile(values, quantiles)
            for q, v in zip(quantiles, q_values):
                combined[f"{name}_{q}"] = v

    return combined
