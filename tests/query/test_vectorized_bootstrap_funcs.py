"""Tests for the vectorized shared-bootstrap engine against the legacy path.

These tests validate that vectorized_bootstrap_funcs.py produces the same
results as the existing per-replicate loop in bootstrap_funcs.py, at three
levels: (1) resample index construction, (2) individual metric kernels, and
(3) the full shared-bootstrap UDF body end-to-end. The vectorized engine is
gated behind the TEEHR_BOOTSTRAP_ENGINE=vectorized env var (see
bootstrap_funcs._can_use_vectorized_engine) and is off by default, so these
tests explicitly enable it via monkeypatch where needed.
"""
import numpy as np
import pandas as pd
import pytest

from teehr import DeterministicMetrics, Signatures
from teehr.metrics.bootstrap_funcs import (
    _make_bs_object,
    create_shared_bootstrap_func,
)
from teehr.metrics.models.bootstrap import Bootstrappers
from teehr.querying.utils import derive_map_key_list
from teehr.metrics.vectorized_bootstrap_funcs import (
    VECTORIZED_METRIC_FUNCS,
    build_index_matrix,
    compute_vectorized_shared_bootstrap,
)

RNG = np.random.default_rng(42)


def _random_series(n, seed, loc=10.0, scale=3.0):
    rng = np.random.default_rng(seed)
    return pd.Series(np.abs(rng.normal(loc=loc, scale=scale, size=n)) + 0.1)


# ---------------------------------------------------------------------------
# 1. Index construction: build_index_matrix vs. the legacy bs.apply() loop.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("boot_cls,kwargs", [
    (Bootstrappers.Stationary, {"block_size": 5}),
    (Bootstrappers.CircularBlock, {"block_size": 5}),
])
def test_index_matrix_matches_legacy_apply_loop(boot_cls, kwargs):
    """build_index_matrix's indices must exactly match arch's own per-rep loop."""
    n = 41
    reps = 30
    p = _random_series(n, seed=1)
    s = _random_series(n, seed=2)
    boot = boot_cls(seed=1234, reps=reps, quantiles=None, **kwargs)

    # Legacy: capture the exact index sequence arch produces via .bootstrap().
    bs_legacy = _make_bs_object(boot, (p, s))
    legacy_indices = []
    for pos_data, _ in bs_legacy.bootstrap(reps):
        # Recover the index by locating it isn't directly exposed; instead
        # re-derive from bs_legacy._index, set just before _resample() yields.
        legacy_indices.append(bs_legacy._index.copy())
    legacy_indices = np.array(legacy_indices)

    # Vectorized: fresh object, same seed.
    bs_vec = _make_bs_object(boot, (p, s))
    vec_indices = build_index_matrix(bs_vec, reps)

    assert vec_indices.shape == (reps, n)
    np.testing.assert_array_equal(vec_indices, legacy_indices)


# ---------------------------------------------------------------------------
# 2. Individual vectorized metric kernels vs. the scalar closures they replace.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("metric_cls,extra_kwargs", [
    (DeterministicMetrics.RelativeMean, {}),
    (DeterministicMetrics.RelativeMedian, {}),
    (DeterministicMetrics.RelativeMinimum, {}),
    (DeterministicMetrics.RelativeMaximum, {}),
    (DeterministicMetrics.RelativeStandardDeviation, {}),
    (DeterministicMetrics.RelativeBias, {}),
    (DeterministicMetrics.NashSutcliffeEfficiency, {}),
    (DeterministicMetrics.PearsonCorrelation, {}),
    (DeterministicMetrics.KlingGuptaEfficiency, {}),
])
@pytest.mark.parametrize("add_epsilon", [False, True])
def test_vectorized_kernel_matches_scalar_closure(metric_cls, extra_kwargs, add_epsilon):
    """Each vectorized kernel must match the scalar closure row-by-row."""
    n = 25
    reps = 40
    metric = metric_cls(add_epsilon=add_epsilon, **extra_kwargs)
    scalar_func = metric.func(metric)
    kernel = VECTORIZED_METRIC_FUNCS[type(metric).__name__]

    p_mat = np.abs(RNG.normal(loc=10, scale=3, size=(reps, n))) + 0.1
    s_mat = np.abs(RNG.normal(loc=9, scale=4, size=(reps, n))) + 0.1

    expected = np.array([scalar_func(p_mat[r], s_mat[r]) for r in range(reps)])
    actual = kernel(p_mat.copy(), s_mat.copy(), metric)

    np.testing.assert_allclose(actual, expected, rtol=1e-9, atol=1e-12)


def _metric_class(name):
    """Resolve a VECTORIZED_METRIC_FUNCS key to its metric class."""
    cls = getattr(DeterministicMetrics, name, None)
    if cls is None:
        cls = getattr(Signatures, name, None)
    assert cls is not None, f"{name!r} in the registry is not a metric class"
    return cls


def _gappy_matrices(reps, n, seed=17):
    """Matrices with non-finite values in p and s at DIFFERENT indices.

    The offset placement is the point: it makes the p-only valid count, the
    s-only valid count, and the pairwise valid count all differ, so a kernel
    that masks per-series instead of pairwise gives a different answer.
    """
    rng = np.random.default_rng(seed)
    p = np.abs(rng.normal(10, 3, size=(reps, n))) + 0.1
    s = np.abs(rng.normal(9, 4, size=(reps, n))) + 0.1
    p[:, 2] = np.nan
    s[:, 5] = np.nan
    p[0, :] = np.nan          # all-NaN row -> empty after the drop
    p[1, 7] = np.inf          # inf is dropped by the scalar path, not skipped
    s[2, 4] = -np.inf
    return p, s


@pytest.mark.parametrize("metric_name", sorted(VECTORIZED_METRIC_FUNCS))
@pytest.mark.parametrize("transform", [None, "sqrt", "log"])
def test_kernel_matches_scalar_closure_on_gappy_data(metric_name, transform):
    """Kernels must match the scalar closures when the data has gaps.

    Regression test for two bugs that shipped together and were invisible to
    the clean-data tests above:

    1. ``deterministic_funcs._transform`` gated its non-finite drop on a
       transform being set, so with ``transform=None`` NaNs reached the
       reduction -- where ``np.sum``/``np.mean``/etc. on a pd.Series skip them
       (pandas dispatch) but ``np.median``/``np.cov``/``np.corrcoef`` propagate
       them (numpy dispatch). RelativeMedian, PearsonCorrelation and
       KlingGuptaEfficiency returned NaN where their kernels returned a number.
    2. ``_vectorized_transform`` returned early when no transform was set,
       skipping the pairwise mask, so kernels reduced p and s over different
       valid subsets.

    The scalar closure is called with **pd.Series**, not numpy rows: numpy rows
    would propagate NaN through every reduction and so validate a code path
    that arch never exercises (it passes Series).
    """
    reps, n = 6, 20
    metric = _metric_class(metric_name)(transform=transform)
    scalar_func = metric.func(metric)
    kernel = VECTORIZED_METRIC_FUNCS[metric_name]

    p_mat, s_mat = _gappy_matrices(reps, n)

    expected = np.array([
        scalar_func(pd.Series(p_mat[r]), pd.Series(s_mat[r]))
        for r in range(reps)
    ])
    actual = kernel(p_mat.copy(), s_mat.copy(), metric)

    np.testing.assert_allclose(
        actual, expected, rtol=1e-9, atol=1e-12, equal_nan=True
    )


def test_pairwise_mask_applied_without_transform():
    """The mask must be pairwise, not per-series, even with no transform.

    p and s are non-finite at different indices, so a per-series mask would
    make relative_mean a ratio of means taken over different rows.
    """
    p = np.array([[1.0, 2.0, np.nan, 4.0]])
    s = np.array([[2.0, np.nan, 6.0, 8.0]])
    metric = DeterministicMetrics.RelativeMean()      # transform=None

    # Only indices 0 and 3 are finite in BOTH series.
    expected = np.mean([2.0, 8.0]) / np.mean([1.0, 4.0])
    kernel = VECTORIZED_METRIC_FUNCS["RelativeMean"]
    actual = kernel(p.copy(), s.copy(), metric)

    assert actual[0] == pytest.approx(expected)


@pytest.mark.parametrize("n", [10, 30, 182, 1000])
@pytest.mark.parametrize("metric_name", ["PearsonCorrelation", "Rsquared"])
def test_correlation_stays_within_unit_interval(metric_name, n):
    """A correlation coefficient cannot exceed 1, on either path.

    The ``add_epsilon`` branch used to divide np.cov's default ddof=1
    covariance by ddof=0 standard deviations. Those do not cancel, so the
    result was ``r * n/(n-1)`` -- 1.110 at n=10 on well-correlated data, which
    is not a correlation at all. Both paths now use a consistent ddof=0, so
    the branch differs from the np.corrcoef branch only by the +EPSILON guard.
    """
    rng = np.random.default_rng(11)
    p = np.abs(rng.lognormal(2, 1, n)) + 0.1
    s = p * rng.uniform(0.9, 1.1, n)          # near-perfect correlation
    r_true = np.corrcoef(s, p)[0][1]
    if metric_name == "Rsquared":
        r_true = r_true ** 2

    metric = _metric_class(metric_name)(add_epsilon=True)
    scalar = metric.func(metric)(pd.Series(p), pd.Series(s))

    assert abs(scalar) <= 1.0, f"scalar {metric_name} = {scalar} exceeds 1.0"
    assert scalar == pytest.approx(r_true, rel=1e-5)

    # Rsquared has no kernel yet; this half starts asserting when one is added.
    if metric_name in VECTORIZED_METRIC_FUNCS:
        kernel = VECTORIZED_METRIC_FUNCS[metric_name](
            p[None, :].copy(), s[None, :].copy(), metric
        )[0]
        assert abs(kernel) <= 1.0, (
            f"kernel {metric_name} = {kernel} exceeds 1.0"
        )
        assert kernel == pytest.approx(r_true, rel=1e-5)


def test_vectorized_kernel_matches_scalar_closure_with_degenerate_rows():
    """NSE/KGE guard behavior (zero std, zero sum) must match on a mixed batch."""
    n = 10
    p_mat = np.ones((4, n))  # zero variance -> KGE guard; NSE denominator == 0
    s_mat = RNG.normal(size=(4, n)) + 5

    nse = DeterministicMetrics.NashSutcliffeEfficiency()
    kge = DeterministicMetrics.KlingGuptaEfficiency()

    nse_scalar = np.array([
        nse.func(nse)(p_mat[r], s_mat[r]) for r in range(4)
    ])
    kge_scalar = np.array([
        kge.func(kge)(p_mat[r], s_mat[r]) for r in range(4)
    ])

    nse_vec = VECTORIZED_METRIC_FUNCS["NashSutcliffeEfficiency"](p_mat.copy(), s_mat.copy(), nse)
    kge_vec = VECTORIZED_METRIC_FUNCS["KlingGuptaEfficiency"](p_mat.copy(), s_mat.copy(), kge)

    np.testing.assert_array_equal(np.isnan(nse_scalar), np.isnan(nse_vec))
    np.testing.assert_allclose(nse_scalar[~np.isnan(nse_scalar)], nse_vec[~np.isnan(nse_vec)])
    np.testing.assert_array_equal(np.isnan(kge_scalar), np.isnan(kge_vec))
    np.testing.assert_allclose(kge_scalar[~np.isnan(kge_scalar)], kge_vec[~np.isnan(kge_vec)])


# ---------------------------------------------------------------------------
# 3. End-to-end: compute_vectorized_shared_bootstrap vs. the legacy UDF body.
# ---------------------------------------------------------------------------

def test_shared_bootstrap_vectorized_matches_legacy_end_to_end(monkeypatch):
    """Full shared-bootstrap UDF output must match between engines, same seed."""
    n = 60
    reps = 200
    p = _random_series(n, seed=10)
    s = _random_series(n, seed=11, loc=9.5, scale=4)

    boot = Bootstrappers.Stationary(seed=777, reps=reps, block_size=6, quantiles=[0.05, 0.95])
    metrics = [
        DeterministicMetrics.RelativeMean(output_field_name="rm", bootstrap=boot),
        DeterministicMetrics.NashSutcliffeEfficiency(output_field_name="nse", bootstrap=boot),
        DeterministicMetrics.KlingGuptaEfficiency(output_field_name="kge", bootstrap=boot),
        DeterministicMetrics.PearsonCorrelation(output_field_name="pearson", bootstrap=boot),
    ]

    monkeypatch.delenv("TEEHR_BOOTSTRAP_ENGINE", raising=False)
    legacy_func = create_shared_bootstrap_func(metrics)
    legacy_result = legacy_func(p, s)

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    vectorized_func = create_shared_bootstrap_func(metrics)
    vectorized_result = vectorized_func(p, s)

    assert set(legacy_result.keys()) == set(vectorized_result.keys())
    for key in legacy_result:
        assert vectorized_result[key] == pytest.approx(legacy_result[key], rel=1e-9, abs=1e-12)


def test_shared_bootstrap_vectorized_matches_legacy_circularblock(monkeypatch):
    """Same end-to-end check for CircularBlock (also routed to the vectorized path)."""
    n = 50
    reps = 150
    p = _random_series(n, seed=20)
    s = _random_series(n, seed=21, loc=11, scale=2.5)

    boot = Bootstrappers.CircularBlock(seed=555, reps=reps, block_size=7, quantiles=[0.1, 0.9])
    metrics = [
        DeterministicMetrics.RelativeMedian(output_field_name="rmed", bootstrap=boot),
        DeterministicMetrics.RelativeStandardDeviation(output_field_name="rstd", bootstrap=boot),
        DeterministicMetrics.NashSutcliffeEfficiency(output_field_name="nse", bootstrap=boot),
        DeterministicMetrics.PearsonCorrelation(output_field_name="pearson", bootstrap=boot),
    ]

    monkeypatch.delenv("TEEHR_BOOTSTRAP_ENGINE", raising=False)
    legacy_result = create_shared_bootstrap_func(metrics)(p, s)

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    vectorized_result = create_shared_bootstrap_func(metrics)(p, s)

    assert set(legacy_result.keys()) == set(vectorized_result.keys())
    for key in legacy_result:
        assert vectorized_result[key] == pytest.approx(legacy_result[key], rel=1e-9, abs=1e-12)


def test_engine_flag_defaults_to_legacy(monkeypatch):
    """Without the env var set, the vectorized path must not be used."""
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine

    monkeypatch.delenv("TEEHR_BOOTSTRAP_ENGINE", raising=False)
    boot = Bootstrappers.Stationary(seed=1, reps=10, quantiles=None)
    metrics = [DeterministicMetrics.RelativeMean(bootstrap=boot)]
    assert _can_use_vectorized_engine(boot, metrics) is False


def test_engine_flag_falls_back_for_gumboot(monkeypatch):
    """Gumboot must always use the legacy path, even with the flag enabled."""
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    boot = Bootstrappers.Gumboot(seed=1, reps=10, quantiles=None)
    metrics = [DeterministicMetrics.RelativeMean(bootstrap=boot)]
    assert _can_use_vectorized_engine(boot, metrics) is False


def test_mixed_group_enters_vectorized_engine(monkeypatch):
    """A group with SOME covered metrics must still use the vectorized engine.

    This assertion is inverted from the version that shipped, which required
    every metric in the group to have a kernel. That was a coarse proxy for
    "an uncovered metric is never computed by a kernel" -- and it cost a mixed
    group ~8x, since bootstrap_group_key excludes metric class and so groups
    are heterogeneous by design. The property is now enforced structurally
    (a class absent from the registry can only reach the scalar-closure branch
    of the dispatch), and asserted directly by the equivalence tests below,
    which are strictly stronger than the proxy was.
    """
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    boot = Bootstrappers.Stationary(seed=1, reps=10, quantiles=None)
    metrics = [
        DeterministicMetrics.RelativeMean(bootstrap=boot),
        DeterministicMetrics.SpearmanCorrelation(bootstrap=boot),  # not in registry
    ]
    assert _can_use_vectorized_engine(boot, metrics) is True


def test_singleton_group_uses_vectorized_engine(monkeypatch):
    """A lone bootstrapped metric must reach the engine, and match legacy.

    format.py used to route groups of one through ``boot.func(ref)``, which
    never consults the gate -- so the flag bought a single-metric request
    nothing at all. Groups of one now take the shared path like any other.
    """
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine

    n, reps = 55, 120
    p = _random_series(n, seed=610)
    s = _random_series(n, seed=611, loc=9, scale=3)

    boot = Bootstrappers.Stationary(
        seed=808, reps=reps, block_size=5, quantiles=[0.05, 0.95]
    )

    def build():
        return [DeterministicMetrics.KlingGuptaEfficiency(
            output_field_name="kge", bootstrap=boot)]

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    assert _can_use_vectorized_engine(boot, build()) is True
    vectorized_result = create_shared_bootstrap_func(build())(p, s)

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "legacy")
    legacy_result = create_shared_bootstrap_func(build())(p, s)

    assert set(legacy_result.keys()) == set(vectorized_result.keys())
    for key in legacy_result:
        assert vectorized_result[key] == pytest.approx(
            legacy_result[key], rel=1e-9, abs=1e-12
        )


def test_engine_flag_falls_back_when_no_metric_is_covered(monkeypatch):
    """With nothing covered there is no work for the engine; use bs.apply."""
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    boot = Bootstrappers.Stationary(seed=1, reps=10, quantiles=None)
    metrics = [DeterministicMetrics.SpearmanCorrelation(bootstrap=boot)]
    assert _can_use_vectorized_engine(boot, metrics) is False


@pytest.mark.parametrize("transform", [None, "log"])
@pytest.mark.parametrize("quantiles", [[0.05, 0.95], None])
def test_mixed_group_matches_legacy_end_to_end(
    monkeypatch, transform, quantiles
):
    """A group mixing covered and uncovered metrics must match legacy exactly.

    This is the test the per-metric fallback exists to make pass: the kernels
    handle what they can on the batched matrices while SpearmanCorrelation and
    MeanError go through their own scalar closures, all on one index matrix.
    """
    n, reps = 60, 200
    p = _random_series(n, seed=210)
    s = _random_series(n, seed=211, loc=9.5, scale=4)

    boot = Bootstrappers.Stationary(
        seed=4242, reps=reps, block_size=6, quantiles=quantiles
    )

    def build():
        return [
            DeterministicMetrics.RelativeMean(
                output_field_name="rm", bootstrap=boot, transform=transform),
            DeterministicMetrics.NashSutcliffeEfficiency(
                output_field_name="nse", bootstrap=boot, transform=transform),
            DeterministicMetrics.SpearmanCorrelation(      # no kernel
                output_field_name="spearman", bootstrap=boot,
                transform=transform),
            DeterministicMetrics.KlingGuptaEfficiency(
                output_field_name="kge", bootstrap=boot, transform=transform),
            DeterministicMetrics.MeanError(               # no kernel
                output_field_name="me", bootstrap=boot, transform=transform),
        ]

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "legacy")
    legacy_result = create_shared_bootstrap_func(build())(p, s)

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    vectorized_result = create_shared_bootstrap_func(build())(p, s)

    assert set(legacy_result.keys()) == set(vectorized_result.keys())
    for key in legacy_result:
        expected, actual = legacy_result[key], vectorized_result[key]
        if quantiles is None:
            np.testing.assert_allclose(
                actual, expected, rtol=1e-9, atol=1e-12, equal_nan=True
            )
        else:
            assert actual == pytest.approx(expected, rel=1e-9, abs=1e-12)


def test_fallback_metric_sees_same_draws_as_kernel(monkeypatch):
    """A covered metric and an uncovered clone of it must agree exactly.

    Direct assertion that the shared bootstrap is still shared. Rather than
    lean on a formula identity between two different metrics, this subclasses
    RelativeBias so the formula is identical by construction, and leaves the
    subclass out of the registry. One copy is then routed through the kernel
    and the other through the per-rep fallback loop. If the two subsets saw
    different draws, the columns would diverge.
    """
    n, reps = 50, 150
    p = _random_series(n, seed=310)
    s = _random_series(n, seed=311, loc=9, scale=3)

    class RelativeBiasClone(DeterministicMetrics.RelativeBias):
        """Same formula, deliberately absent from VECTORIZED_METRIC_FUNCS."""

    assert "RelativeBiasClone" not in VECTORIZED_METRIC_FUNCS

    boot = Bootstrappers.Stationary(
        seed=515, reps=reps, block_size=5, quantiles=None
    )
    metrics = [
        DeterministicMetrics.RelativeBias(
            output_field_name="via_kernel", bootstrap=boot),
        RelativeBiasClone(
            output_field_name="via_fallback", bootstrap=boot),
    ]

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine
    assert _can_use_vectorized_engine(boot, metrics) is True

    result = create_shared_bootstrap_func(metrics)(p, s)
    np.testing.assert_allclose(
        result["via_fallback"], result["via_kernel"],
        rtol=1e-9, atol=1e-12, equal_nan=True,
    )


@pytest.mark.parametrize("max_cells", [1, 7, 10**9])
def test_rep_chunking_is_bit_identical(max_cells):
    """Chunking the index matrix must not perturb the RNG stream."""
    from teehr.metrics.bootstrap_funcs import _make_bs_object

    n, reps = 40, 60
    p = _random_series(n, seed=410)
    s = _random_series(n, seed=411, loc=8, scale=2)

    boot = Bootstrappers.Stationary(
        seed=717, reps=reps, block_size=4, quantiles=[0.1, 0.9]
    )
    metrics = [
        DeterministicMetrics.RelativeMean(
            output_field_name="rm", bootstrap=boot),
        DeterministicMetrics.MeanError(
            output_field_name="me", bootstrap=boot),
    ]
    funcs = [m.func(m) for m in metrics]

    def run(cells):
        bs = _make_bs_object(boot, (p, s))
        return compute_vectorized_shared_bootstrap(
            metrics, funcs, (p, s), bs, reps, boot.quantiles,
            max_matrix_cells=cells,
        )

    reference = run(10**9)
    actual = run(max_cells)
    assert set(actual) == set(reference)
    for key in reference:
        assert actual[key] == pytest.approx(reference[key], rel=0, abs=0)


def test_resample_args_matches_arch_resample():
    """resample_args must reproduce arch's _resample: values, index, type."""
    from arch.bootstrap import StationaryBootstrap
    from teehr.metrics.vectorized_bootstrap_funcs import resample_args

    n, reps = 12, 4
    p = pd.Series(np.arange(n, dtype=float) + 1.0)
    s = pd.Series((np.arange(n, dtype=float) + 1.0) * 10)

    seen = []

    def probe(*args):
        seen.append(tuple(args))
        return np.asarray([0.0])

    StationaryBootstrap(3, p, s, seed=42).apply(probe, reps)
    # apply() evaluates the func once on the un-resampled data to infer shape
    # and discards it, consuming no RNG; the draws start at index 1.
    drawn = seen[1:]

    bs = StationaryBootstrap(3, p, s, seed=42)
    for rep in range(reps):
        indices = np.asarray(bs.update_indices())
        mine = resample_args((p, s), indices)
        for got, expected in zip(mine, drawn[rep]):
            assert type(got) is type(expected)
            np.testing.assert_array_equal(got.values, expected.values)
            assert list(got.index) == list(expected.index)


def test_end_to_end_reps_1000_scale(monkeypatch):
    """Sanity check at production-scale reps that nothing degrades numerically."""
    n = 45
    reps = 1000
    p = _random_series(n, seed=100)
    s = _random_series(n, seed=101, loc=8, scale=2.5)

    boot = Bootstrappers.Stationary(seed=99, reps=reps, block_size=4, quantiles=[0.025, 0.975])
    metrics = [
        DeterministicMetrics.RelativeMean(output_field_name="rm", bootstrap=boot),
        DeterministicMetrics.RelativeMedian(output_field_name="rmed", bootstrap=boot),
        DeterministicMetrics.RelativeStandardDeviation(output_field_name="rstd", bootstrap=boot),
        DeterministicMetrics.RelativeBias(output_field_name="rbias", bootstrap=boot),
        DeterministicMetrics.NashSutcliffeEfficiency(output_field_name="nse", bootstrap=boot),
        DeterministicMetrics.KlingGuptaEfficiency(output_field_name="kge", bootstrap=boot),
        DeterministicMetrics.PearsonCorrelation(output_field_name="pearson", bootstrap=boot),
    ]

    monkeypatch.delenv("TEEHR_BOOTSTRAP_ENGINE", raising=False)
    legacy_result = create_shared_bootstrap_func(metrics)(p, s)

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    vectorized_result = create_shared_bootstrap_func(metrics)(p, s)

    for key in legacy_result:
        assert vectorized_result[key] == pytest.approx(legacy_result[key], rel=1e-8, abs=1e-11)


def test_shared_bootstrap_keys_match_static_derivation(monkeypatch):
    """Both engines must emit exactly the keys `derive_map_key_list` predicts.

    Unpacking builds its output columns from statically derived keys, and
    `F.col(map).getItem(missing_key)` yields null rather than an error, so a
    drift between producers and derivation would silently produce all-null
    quantile columns.
    """
    n = 60
    p = _random_series(n, seed=20)
    s = _random_series(n, seed=21, loc=9.0, scale=3.5)

    boot = Bootstrappers.Stationary(
        seed=321, reps=50, block_size=5, quantiles=[0.05, 0.5, 0.95]
    )
    metrics = [
        DeterministicMetrics.KlingGuptaEfficiency(bootstrap=boot),
        DeterministicMetrics.NashSutcliffeEfficiency(bootstrap=boot),
    ]
    expected = set()
    for metric in metrics:
        expected.update(derive_map_key_list(metric))

    monkeypatch.delenv("TEEHR_BOOTSTRAP_ENGINE", raising=False)
    assert set(create_shared_bootstrap_func(metrics)(p, s).keys()) == expected

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    assert set(create_shared_bootstrap_func(metrics)(p, s).keys()) == expected
