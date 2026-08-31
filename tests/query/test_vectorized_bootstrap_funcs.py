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

from teehr import DeterministicMetrics
from teehr.metrics.bootstrap_funcs import (
    _make_bs_object,
    create_shared_bootstrap_func,
)
from teehr.metrics.models.bootstrap import Bootstrappers
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


def test_engine_flag_falls_back_for_unvectorized_metric(monkeypatch):
    """A metric without a vectorized kernel must force the legacy fallback."""
    from teehr.metrics.bootstrap_funcs import _can_use_vectorized_engine

    monkeypatch.setenv("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    boot = Bootstrappers.Stationary(seed=1, reps=10, quantiles=None)
    metrics = [
        DeterministicMetrics.RelativeMean(bootstrap=boot),
        DeterministicMetrics.SpearmanCorrelation(bootstrap=boot),  # not in registry
    ]
    assert _can_use_vectorized_engine(boot, metrics) is False


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
