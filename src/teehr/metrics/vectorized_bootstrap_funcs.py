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
    p, s, _ = _vectorized_transform_flagged(p, s, model)
    return p, s


def _vectorized_transform_flagged(p, s, model) -> tuple:
    """``_vectorized_transform``, also reporting whether the mask fired.

    The flag lets _Moments take plain ``np.sum``/``np.mean``/``np.min`` on a
    chunk with no NaN in it. numpy's ``nan*`` reductions run ``_replace_nan``
    first, which tests every element and copies the array when any is NaN --
    real cost on a 12.8 MB matrix, repeated per reduction, to handle a case
    that mostly does not arise: the mask only fires on gappy input.
    """
    transform = getattr(model, "transform", None)
    add_epsilon = getattr(model, "add_epsilon", False)

    if transform is not None:
        p = _apply_transform_1d(p, transform, add_epsilon)
        s = _apply_transform_1d(s, transform, add_epsilon)

    invalid = ~(np.isfinite(p) & np.isfinite(s))
    has_invalid = bool(np.any(invalid))
    if has_invalid:
        p = np.where(invalid, np.nan, p)
        s = np.where(invalid, np.nan, s)

    return p, s, has_invalid


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


def _vec_divide(numerator, denominator):
    """Row-wise ``deterministic_funcs._divide``: NaN where the denominator is 0.

    The kernels are required to be bit-identical to the scalar closures (see
    the parity tests), so they need the same zero-denominator rule. Without it
    a bootstrapped ``relative_minimum`` over a group whose observed minimum is
    zero draws ``inf`` replicates, and a single ``inf`` takes the quantiles
    with it.
    """
    with np.errstate(invalid="ignore", divide="ignore"):
        result = numerator / denominator
    return np.where(np.asarray(denominator) == 0, np.nan, result)


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


# --- Shared per-chunk accumulators -----------------------------------------
#
# Every two-field metric here is a function of the same handful of sums over
# the same (reps, n) matrices. Computed per kernel, as they were, a group of
# nine metrics walked those matrices ~30 times and applied the finite mask
# nine times: 51 ms of the 87 ms spent in kernels for a production-shaped
# group (n=1600, reps=1000, measured on an M-series laptop). ``_Moments``
# computes each accumulator at most once and hands it to every metric that
# needs it.
#
# There is one implementation of each formula, not two: the per-metric
# entries in VECTORIZED_METRIC_FUNCS are thin wrappers over the same
# ``_derive_*`` functions the batch path calls, so the single-metric and
# shared paths cannot drift, and the existing kernel-vs-scalar parity tests
# cover both.
#
# Accumulators are lazy -- a group asking only for RelativeMinimum never pays
# for a covariance pass -- and every one of them reproduces exactly the
# expression the kernel used before, including which matrices (raw or
# transformed) each legacy guard reads.


def _transform_key(model) -> tuple:
    """Key identifying metrics whose transformed matrices are identical.

    Metrics in one bootstrap group may differ in ``transform`` and
    ``add_epsilon`` -- ``bootstrap_group_key`` deliberately excludes both, so
    that unrelated metrics can still share draws. Only ``log`` and ``inv``
    consult ``add_epsilon`` while transforming (the rest ignore it and apply
    it at the final division), so it belongs in the key only for those two.
    Keeping it out otherwise is what lets a mixed add_epsilon group -- the
    common case -- share one set of accumulators.
    """
    transform = getattr(model, "transform", None)
    if transform in (TransformEnum.log, TransformEnum.inv):
        return (transform, getattr(model, "add_epsilon", False))
    return (transform, None)


class _Moments:
    """Lazily computed accumulators over one ``(reps, n)`` resample chunk.

    ``raw_*`` properties read the matrices as passed in; everything else
    reads them after ``_vectorized_transform``. The distinction is not
    cosmetic: the NSE and KGE guards are evaluated pre-transform (and, for
    NSE, pre-mask), so reproducing them exactly means keeping both.
    """

    __slots__ = (
        "_p_raw", "_s_raw", "_model", "_cache", "p", "s", "_clean", "_plain"
    )

    def __init__(self, p_raw, s_raw, model):
        self._p_raw = p_raw
        self._s_raw = s_raw
        self._model = model
        self._cache = {}
        self.p, self.s, has_invalid = _vectorized_transform_flagged(
            p_raw, s_raw, model
        )
        # No NaN anywhere: the nan-aware reductions have nothing to skip, and
        # the raw matrices are the transformed ones when no transform is set,
        # so the pre-transform guards can reuse the same accumulators.
        self._clean = not has_invalid
        self._plain = self._clean and getattr(model, "transform", None) is None

    def _get(self, name, fn):
        if name not in self._cache:
            self._cache[name] = fn()
        return self._cache[name]

    # Reduction pickers: identical results, one skips numpy's NaN machinery.
    def _sum(self, x):
        return np.sum(x, axis=1) if self._clean else np.nansum(x, axis=1)

    def _mean(self, x):
        return (
            np.mean(x, axis=1, keepdims=True)
            if self._clean
            else np.nanmean(x, axis=1, keepdims=True)
        )

    # -- guards, read pre-transform to match the legacy kernels -------------
    @property
    def raw_sum_p(self):
        if self._plain:
            return self.sum_p
        return self._get("raw_sum_p", lambda: np.nansum(self._p_raw, axis=1))

    @property
    def raw_sum_s(self):
        if self._plain:
            return self._get(
                "sum_s", lambda: self._sum(self.s)
            )
        return self._get("raw_sum_s", lambda: np.nansum(self._s_raw, axis=1))

    @property
    def raw_std_p(self):
        if self._plain:
            return self.std_p
        return self._get("raw_std_p", lambda: np.nanstd(self._p_raw, axis=1))

    @property
    def raw_std_s(self):
        if self._plain:
            return self.std_s
        return self._get("raw_std_s", lambda: np.nanstd(self._s_raw, axis=1))

    # -- counts and first moments ------------------------------------------
    @property
    def n(self):
        if self._clean:
            return self._get(
                "n", lambda: np.full(self.p.shape[0], self.p.shape[1])
            )
        return self._get("n", lambda: _finite_pair_count(self.p, self.s))

    @property
    def mean_p(self):
        return self._get("mean_p", lambda: self._mean(self.p))

    @property
    def mean_s(self):
        return self._get("mean_s", lambda: self._mean(self.s))

    @property
    def sum_p(self):
        return self._get("sum_p", lambda: self._sum(self.p))

    # -- centered second moments -------------------------------------------
    #
    # dp/ds are materialized because six metrics want them; that is two more
    # (reps, n) allocations, which the chunking in
    # compute_vectorized_shared_bootstrap already bounds.
    @property
    def dp(self):
        return self._get("dp", lambda: self.p - self.mean_p)

    @property
    def ds(self):
        return self._get("ds", lambda: self.s - self.mean_s)

    @property
    def sum_dp2(self):
        return self._get("sum_dp2", lambda: self._sum(self.dp**2))

    @property
    def sum_ds2(self):
        return self._get("sum_ds2", lambda: self._sum(self.ds**2))

    @property
    def sum_dpds(self):
        return self._get("sum_dpds", lambda: self._sum(self.dp * self.ds))

    # np.nanstd is exactly sqrt(mean of squared deviations) over the non-NaN
    # entries, which is what these two are -- computed from sum_dp2 rather
    # than by a second np.nanstd pass over the matrix.
    @property
    def std_p(self):
        return self._get(
            "std_p", lambda: np.sqrt(self.sum_dp2 / np.maximum(self.n, 1))
        )

    @property
    def std_s(self):
        return self._get(
            "std_s", lambda: np.sqrt(self.sum_ds2 / np.maximum(self.n, 1))
        )

    # -- difference accumulators -------------------------------------------
    #
    # sum_sdiff is its own reduction rather than sum_s - sum_p: the two agree
    # to a rounding error that is negligible against either sum but not
    # against their difference, which is the quantity relative_bias divides.
    @property
    def sum_diff2(self):
        return self._get(
            "sum_diff2", lambda: self._sum((self.p - self.s) ** 2)
        )

    @property
    def sum_absdiff(self):
        return self._get(
            "sum_absdiff", lambda: self._sum(np.abs(self.p - self.s))
        )

    @property
    def sum_sdiff(self):
        return self._get("sum_sdiff", lambda: self._sum(self.s - self.p))

    # -- extrema -----------------------------------------------------------
    @property
    def min_p(self):
        return self._get(
            "min_p",
            lambda: (np.min if self._clean else np.nanmin)(self.p, axis=1),
        )

    @property
    def min_s(self):
        return self._get(
            "min_s",
            lambda: (np.min if self._clean else np.nanmin)(self.s, axis=1),
        )

    @property
    def max_p(self):
        return self._get(
            "max_p",
            lambda: (np.max if self._clean else np.nanmax)(self.p, axis=1),
        )

    @property
    def max_s(self):
        return self._get(
            "max_s",
            lambda: (np.max if self._clean else np.nanmax)(self.s, axis=1),
        )


def _pearson_r_from(m: _Moments, add_epsilon: bool) -> np.ndarray:
    """Row-wise Pearson correlation, matching ``pearson_correlation_inner``.

    ``add_epsilon=False`` mirrors ``np.corrcoef(s, p)[0][1]``.
    ``add_epsilon=True`` mirrors
    ``np.cov(p, s, ddof=0)[0, 1] / (nanstd(p) * nanstd(s) + EPSILON)``.

    Both branches use a consistent ddof=0, so the two differ only by the
    ``+EPSILON`` divide-by-zero guard. An earlier version paired ``np.cov``'s
    default ddof=1 with ddof=0 standard deviations; those do not cancel and
    the result was ``r * n/(n-1)``, which exceeds 1.0 on small
    well-correlated samples. The mismatch was documented as intentional but
    produced a quantity that is not a correlation coefficient.
    """
    with np.errstate(invalid="ignore", divide="ignore"):
        if add_epsilon:
            cov = m.sum_dpds / np.maximum(m.n, 1)
            return _vec_divide(cov, m.std_p * m.std_s + EPSILON)
        denom = np.sqrt(m.sum_dp2 * m.sum_ds2)
        return _vec_divide(m.sum_dpds, denom)


def _vec_pearson_r(p: np.ndarray, s: np.ndarray, add_epsilon: bool) -> np.ndarray:
    """Pre-transformed-matrix entry point, kept for callers outside this file."""
    m = _Moments.__new__(_Moments)
    m._cache = {}
    m._p_raw, m._s_raw, m._model = p, s, None
    m.p, m.s = p, s
    m._clean = not bool(np.isnan(p).any() or np.isnan(s).any())
    m._plain = m._clean
    return _pearson_r_from(m, add_epsilon)


# --- Derivations ------------------------------------------------------------
#
# One function per metric, all reading accumulators rather than matrices.


def _derive_relative_mean(m: _Moments, model) -> np.ndarray:
    mean_p, mean_s = m.mean_p[:, 0], m.mean_s[:, 0]
    if model.add_epsilon:
        return _vec_divide(mean_s, mean_p + EPSILON)
    return _vec_divide(mean_s, mean_p)


def _derive_relative_minimum(m: _Moments, model) -> np.ndarray:
    if model.add_epsilon:
        return _vec_divide(m.min_s, m.min_p + EPSILON)
    return _vec_divide(m.min_s, m.min_p)


def _derive_relative_maximum(m: _Moments, model) -> np.ndarray:
    if model.add_epsilon:
        return _vec_divide(m.max_s, m.max_p + EPSILON)
    return _vec_divide(m.max_s, m.max_p)


def _derive_relative_standard_deviation(m: _Moments, model) -> np.ndarray:
    if model.add_epsilon:
        return _vec_divide(m.std_s, m.std_p + EPSILON)
    return _vec_divide(m.std_s, m.std_p)


def _derive_relative_bias(m: _Moments, model) -> np.ndarray:
    if model.add_epsilon:
        return _vec_divide(m.sum_sdiff, m.sum_p + EPSILON)
    return _vec_divide(m.sum_sdiff, m.sum_p)


def _derive_max_value_delta(m: _Moments, model) -> np.ndarray:
    return m.max_s - m.max_p


def _nse_parts_from(m: _Moments, model) -> tuple:
    """Numerator, denominator and NaN guard shared by NSE and normalized NSE.

    The two scalar closures are identical up to their final expression, so
    sharing the parts keeps the guards from drifting apart. The guard reads
    the RAW matrices, pre-transform and pre-mask, exactly as the scalar
    closures do.
    """
    guard_nan = (m.n == 0) | (m.raw_sum_p == 0) | (m.raw_sum_s == 0)
    denominator = m.sum_dp2
    if model.add_epsilon:
        denominator = denominator + EPSILON
    return m.sum_diff2, denominator, guard_nan | (denominator == 0)


def _derive_nash_sutcliffe_efficiency(m: _Moments, model) -> np.ndarray:
    numerator, denominator, guard_nan = _nse_parts_from(m, model)
    with np.errstate(invalid="ignore", divide="ignore"):
        result = 1.0 - numerator / denominator
    return np.where(guard_nan, np.nan, result)


def _derive_nash_sutcliffe_efficiency_normalized(m: _Moments, model) -> np.ndarray:
    numerator, denominator, guard_nan = _nse_parts_from(m, model)
    with np.errstate(invalid="ignore", divide="ignore"):
        # Written as the scalar closure writes it (1/(1 + num/den)) rather
        # than the algebraically equal 1/(2 - NSE): same value, but
        # bit-identical to deterministic_funcs and no second division.
        result = 1.0 / (1.0 + numerator / denominator)
    return np.where(guard_nan, np.nan, result)


def _derive_kling_gupta_efficiency(m: _Moments, model) -> np.ndarray:
    # Legacy guard (pre-transform): zero std on either side -> NaN.
    guard_nan = (m.raw_std_s == 0) | (m.raw_std_p == 0)
    r = _pearson_r_from(m, add_epsilon=False)  # kge always uses plain corrcoef
    mean_p, mean_s = m.mean_p[:, 0], m.mean_s[:, 0]

    if model.add_epsilon:
        rel_var = _vec_divide(m.std_s, m.std_p + EPSILON)
        rel_mean = _vec_divide(mean_s, mean_p + EPSILON)
    else:
        rel_var = _vec_divide(m.std_s, m.std_p)
        rel_mean = _vec_divide(mean_s, mean_p)

    euclidean = np.sqrt(
        model.sr * (r - 1.0) ** 2
        + model.sa * (rel_var - 1.0) ** 2
        + model.sb * (rel_mean - 1.0) ** 2
    )
    return np.where(guard_nan, np.nan, 1.0 - euclidean)


def _derive_kling_gupta_efficiency_mod1(m: _Moments, model) -> np.ndarray:
    guard_nan = (m.raw_std_s == 0) | (m.raw_std_p == 0)
    r = _pearson_r_from(m, add_epsilon=False)
    mean_p, mean_s = m.mean_p[:, 0], m.mean_s[:, 0]

    if model.add_epsilon:
        # Mod1's variability ratio is a ratio of coefficients of variation,
        # unlike kge's ratio of raw standard deviations.
        var_ratio = _vec_divide(
            _vec_divide(m.std_s, mean_s + EPSILON),
            _vec_divide(m.std_p, mean_p + EPSILON),
        )
        rel_mean = _vec_divide(mean_s, mean_p + EPSILON)
    else:
        var_ratio = _vec_divide(
            _vec_divide(m.std_s, mean_s), _vec_divide(m.std_p, mean_p)
        )
        rel_mean = _vec_divide(mean_s, mean_p)

    euclidean = np.sqrt(
        model.sr * (r - 1.0) ** 2
        + model.sa * (var_ratio - 1.0) ** 2
        + model.sb * (rel_mean - 1.0) ** 2
    )
    return np.where(guard_nan, np.nan, 1.0 - euclidean)


def _derive_kling_gupta_efficiency_mod2(m: _Moments, model) -> np.ndarray:
    guard_nan = (m.raw_std_s == 0) | (m.raw_std_p == 0)
    r = _pearson_r_from(m, add_epsilon=False)
    mean_p, mean_s = m.mean_p[:, 0], m.mean_s[:, 0]

    if model.add_epsilon:
        rel_var = _vec_divide(m.std_s, m.std_p + EPSILON)
        bias = _vec_divide((mean_s - mean_p) ** 2, (m.std_p**2) + EPSILON)
    else:
        rel_var = _vec_divide(m.std_s, m.std_p)
        bias = _vec_divide((mean_s - mean_p) ** 2, m.std_p**2)

    euclidean = np.sqrt(
        model.sr * (r - 1.0) ** 2
        + model.sa * (rel_var - 1.0) ** 2
        + model.sb * bias  # NOT squared, unlike the other two terms
    )
    return np.where(guard_nan, np.nan, 1.0 - euclidean)


def _derive_pearson_correlation(m: _Moments, model) -> np.ndarray:
    return _pearson_r_from(m, add_epsilon=model.add_epsilon)


def _derive_r_squared(m: _Moments, model) -> np.ndarray:
    # r_squared_inner is pearson_correlation_inner with the result squared.
    return _pearson_r_from(m, add_epsilon=model.add_epsilon) ** 2


def _derive_mean_error(m: _Moments, model) -> np.ndarray:
    # mean_error_inner uses np.sum(s - p)/len(p) directly, NOT _mean_error --
    # note it is signed, and s - p rather than |p - s|.
    return _vec_divide(m.sum_sdiff, m.n)


def _derive_mean_absolute_error(m: _Moments, model) -> np.ndarray:
    return _vec_divide(m.sum_absdiff, m.n)


def _derive_mean_squared_error(m: _Moments, model) -> np.ndarray:
    return _vec_divide(m.sum_diff2, m.n)


def _derive_root_mean_squared_error(m: _Moments, model) -> np.ndarray:
    return np.sqrt(_vec_divide(m.sum_diff2, m.n))


def _derive_root_mean_standard_deviation_ratio(m: _Moments, model) -> np.ndarray:
    rmse = np.sqrt(_vec_divide(m.sum_diff2, m.n))
    if model.add_epsilon:
        return _vec_divide(rmse, m.std_p + EPSILON)
    return _vec_divide(rmse, m.std_p)


def _derive_mean_absolute_relative_error(m: _Moments, model) -> np.ndarray:
    if model.add_epsilon:
        return _vec_divide(m.sum_absdiff, m.sum_p + EPSILON)
    return _vec_divide(m.sum_absdiff, m.sum_p)


#: Metrics computable from shared accumulators alone. The batch path builds
#: one _Moments per (chunk, transform key) and calls these; the registry
#: wrappers below call the same functions one metric at a time.
MOMENT_DERIVED_FUNCS = {
    "RelativeMean": _derive_relative_mean,
    "MultiplicativeBias": _derive_relative_mean,
    "RelativeMinimum": _derive_relative_minimum,
    "RelativeMaximum": _derive_relative_maximum,
    "RelativeStandardDeviation": _derive_relative_standard_deviation,
    "VariabilityRatio": _derive_relative_standard_deviation,
    "RelativeBias": _derive_relative_bias,
    "MaxValueDelta": _derive_max_value_delta,
    "NashSutcliffeEfficiency": _derive_nash_sutcliffe_efficiency,
    "NormalizedNashSutcliffeEfficiency": (
        _derive_nash_sutcliffe_efficiency_normalized
    ),
    "KlingGuptaEfficiency": _derive_kling_gupta_efficiency,
    "KlingGuptaEfficiencyMod1": _derive_kling_gupta_efficiency_mod1,
    "KlingGuptaEfficiencyMod2": _derive_kling_gupta_efficiency_mod2,
    "PearsonCorrelation": _derive_pearson_correlation,
    "Rsquared": _derive_r_squared,
    "MeanError": _derive_mean_error,
    "MeanAbsoluteError": _derive_mean_absolute_error,
    "MeanSquareError": _derive_mean_squared_error,
    "RootMeanSquareError": _derive_root_mean_squared_error,
    "RootMeanStandardDeviationRatio": _derive_root_mean_standard_deviation_ratio,
    "MeanAbsoluteRelativeError": _derive_mean_absolute_relative_error,
}


def _single(derive):
    """Wrap a derivation as a one-metric ``(p, s, model)`` kernel."""

    def kernel(p, s, model):
        return derive(_Moments(p, s, model), model)

    return kernel


_vec_relative_mean = _single(_derive_relative_mean)
_vec_relative_minimum = _single(_derive_relative_minimum)
_vec_relative_maximum = _single(_derive_relative_maximum)
_vec_relative_standard_deviation = _single(_derive_relative_standard_deviation)
_vec_relative_bias = _single(_derive_relative_bias)
_vec_max_value_delta = _single(_derive_max_value_delta)
_vec_nash_sutcliffe_efficiency = _single(_derive_nash_sutcliffe_efficiency)
_vec_nash_sutcliffe_efficiency_normalized = _single(
    _derive_nash_sutcliffe_efficiency_normalized
)
_vec_kling_gupta_efficiency = _single(_derive_kling_gupta_efficiency)
_vec_kling_gupta_efficiency_mod1 = _single(_derive_kling_gupta_efficiency_mod1)
_vec_kling_gupta_efficiency_mod2 = _single(_derive_kling_gupta_efficiency_mod2)
_vec_pearson_correlation = _single(_derive_pearson_correlation)
_vec_r_squared = _single(_derive_r_squared)
_vec_mean_error = _single(_derive_mean_error)
_vec_mean_absolute_error = _single(_derive_mean_absolute_error)
_vec_mean_squared_error = _single(_derive_mean_squared_error)
_vec_root_mean_squared_error = _single(_derive_root_mean_squared_error)
_vec_root_mean_standard_deviation_ratio = _single(
    _derive_root_mean_standard_deviation_ratio
)
_vec_mean_absolute_relative_error = _single(_derive_mean_absolute_relative_error)


def _vec_relative_median(p, s, model) -> np.ndarray:
    """The one two-field metric no accumulator can serve.

    An order statistic needs the values themselves, so this keeps its own
    pass over the matrices.
    """
    p, s = _vectorized_transform(p, s, model)
    p_med = _row_nanmedian(p)
    s_med = _row_nanmedian(s)
    if model.add_epsilon:
        return _vec_divide(s_med, p_med + EPSILON)
    return _vec_divide(s_med, p_med)


def _row_nanmedian(x: np.ndarray) -> np.ndarray:
    """Row-wise median, skipping NaN.

    ``np.nanmedian`` copies the input and full-sorts each row. When the chunk
    has no NaN at all -- the common case, since the mask only fires on gappy
    input -- ``np.median`` reaches ``np.partition`` instead, which is O(n)
    per row rather than O(n log n) and does not need the copy.
    """
    if not np.isnan(x).any():
        return np.median(x, axis=1)
    return np.nanmedian(x, axis=1)


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
    # Likewise variability_ratio_inner and relative_standard_deviation_inner.
    # Tracked separately as a duplicate-metric question; registering the
    # kernel keeps the two from diverging in the meantime.
    "VariabilityRatio": _vec_relative_standard_deviation,
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
            # Metrics derivable from shared accumulators get one _Moments per
            # distinct transform for the whole chunk, instead of each kernel
            # re-walking the same matrices. Anything else (RelativeMedian, the
            # single-field signatures) keeps its own kernel. Both routes end
            # in the same _derive_* code, so they cannot disagree.
            moments = {}
            for i, metric, kernel in covered:
                derive = MOMENT_DERIVED_FUNCS.get(type(metric).__name__)
                if derive is not None and s_mat is not None:
                    key = _transform_key(metric)
                    shared = moments.get(key)
                    if shared is None:
                        shared = moments[key] = _Moments(p_mat, s_mat, metric)
                    values[done:done + k, i] = derive(shared, metric)
                else:
                    values[done:done + k, i] = kernel(p_mat, s_mat, metric)
            del moments
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
