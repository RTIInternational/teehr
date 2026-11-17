"""Metric definitions against independent references (issue #873)."""

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from teehr.metrics.models.deterministic import (
    NashSutcliffeEfficiency,
    NormalizedNashSutcliffeEfficiency,
    SpearmanCorrelation,
)


def _kernel(model):
    return model.func(model)


@pytest.mark.parametrize(
    ("p", "s"),
    [
        ([10.0, 20.0], [12.0, 18.0]),
        ([-1.0, 1.0, -2.0, 2.0], [0.5, 1.5, -1.0, 2.5]),
        ([0.0, 5.0, 0.0, 10.0], [1.0, 4.0, 0.5, 11.0]),
        (list(np.random.default_rng(1).gamma(2, 30, 200)),
         list(np.random.default_rng(2).gamma(2, 30, 200))),
    ],
)
def test_spearman_matches_scipy(p, s):
    got = _kernel(SpearmanCorrelation())(pd.Series(p), pd.Series(s))
    assert got == pytest.approx(stats.spearmanr(p, s).statistic, rel=1e-12)
    assert -1.0 <= got <= 1.0


def test_nse_is_defined_when_observations_sum_to_zero():
    p, s = pd.Series([-1.0, 1.0, -2.0, 2.0]), pd.Series([0.5, 1.5, -1.0, 2.5])
    expect = 1 - np.sum((p - s) ** 2) / np.sum((p - p.mean()) ** 2)
    assert _kernel(NashSutcliffeEfficiency())(p, s) == pytest.approx(expect)
    assert _kernel(NormalizedNashSutcliffeEfficiency())(p, s) == pytest.approx(
        1 / (2 - expect)
    )


def test_nse_is_undefined_for_constant_observations():
    p, s = pd.Series([3.0, 3.0, 3.0]), pd.Series([1.0, 2.0, 3.0])
    assert np.isnan(_kernel(NashSutcliffeEfficiency())(p, s))
