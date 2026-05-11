"""Unit tests for bootstrap helper edge-case handling."""

import numpy as np
import pandas as pd

from teehr.metrics.bootstrap_funcs import _make_bs_object, _optimal_block_size
from teehr.metrics.models.bootstrap import Bootstrappers


def test_optimal_block_size_short_series_fallback():
    """Short finite series should use the safe fallback block size."""
    assert _optimal_block_size(np.array([1.0, 2.0]), method="stationary") == 2
    assert _optimal_block_size(np.array([1.0, np.nan, 2.0]), method="circular") == 2
    assert _optimal_block_size(np.array([np.nan, np.nan]), method="stationary") == 2
    assert _optimal_block_size(np.array([]), method="circular") == 2


def test_optimal_block_size_handles_optimal_block_length_exception(monkeypatch):
    """Errors from optimal_block_length should not propagate to callers."""
    import arch.bootstrap as arch_bootstrap

    def _raise(_):
        raise ValueError("boom")

    monkeypatch.setattr(arch_bootstrap, "optimal_block_length", _raise)
    assert _optimal_block_size(np.array([1.0, 2.0, 3.0]), method="stationary") == 2


def test_optimal_block_size_handles_nan_estimates(monkeypatch):
    """Estimates with NaN values should gracefully fall back to default block size."""
    import arch.bootstrap as arch_bootstrap

    def _nan_df(_):
        return pd.DataFrame({"stationary": [np.nan], "circular": [np.nan]})

    monkeypatch.setattr(arch_bootstrap, "optimal_block_length", _nan_df)
    assert _optimal_block_size(np.array([1.0, 2.0, 3.0]), method="stationary") == 2
    assert _optimal_block_size(np.array([1.0, 2.0, 3.0]), method="circular") == 2


def test_optimal_block_size_handles_missing_expected_columns(monkeypatch):
    """Unexpected result columns should use fallback block size."""
    import arch.bootstrap as arch_bootstrap

    def _unexpected_df(_):
        return pd.DataFrame({"foo": [3.0], "bar": [4.0]})

    monkeypatch.setattr(arch_bootstrap, "optimal_block_length", _unexpected_df)
    assert _optimal_block_size(np.array([1.0, 2.0, 3.0]), method="stationary") == 2
    assert _optimal_block_size(np.array([1.0, 2.0, 3.0]), method="circular") == 2


def test_make_bs_object_stationary_none_block_size_short_series():
    """Stationary bootstrap object creation should work with auto block size."""
    boot = Bootstrappers.Stationary(seed=1, block_size=None, quantiles=None, reps=10)
    args = (pd.Series([1.0, 2.0]), pd.Series([2.0, 3.0]))

    bs = _make_bs_object(boot, args)
    assert type(bs).__name__ == "StationaryBootstrap"


def test_make_bs_object_circular_none_block_size_short_series():
    """Circular bootstrap object creation should work with auto block size."""
    boot = Bootstrappers.CircularBlock(seed=1, block_size=None, quantiles=None, reps=10)
    args = (pd.Series([1.0, 2.0]), pd.Series([2.0, 3.0]))

    bs = _make_bs_object(boot, args)
    assert type(bs).__name__ == "CircularBlockBootstrap"
