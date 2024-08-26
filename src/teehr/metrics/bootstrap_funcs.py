"""Contains functions for bootstrap calculations for use in Spark queries."""
from typing import Dict, Callable

import pandas as pd
import numpy as np
from arch.bootstrap import (
    StationaryBootstrap,
    CircularBlockBootstrap,
)

from teehr.models.metrics.metric_models import MetricsBasemodel
from teehr.metrics.gumboots_bootstrap import GumBootsBootstrap


# NOTE: All functions need to return the same signature.
# What if the user specifies a different number of quantiles??
# We need some generic way to return the data.
def _calculate_quantiles(
    output_field_name: str,
    results: np.ndarray,
    quantiles: list
) -> Dict:
    """Unpack the bootstrap results."""
    values = np.quantile(results, quantiles)
    quantiles = [f"{output_field_name}_{str(i)}" for i in quantiles]
    d = dict(zip(quantiles, values))
    return d


def create_circularblock_func(model: MetricsBasemodel) -> Callable:
    """Create the CircularBlock bootstrap function."""
    def bootstrap_func(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap function."""
        bs = CircularBlockBootstrap(
            model.bootstrap.block_size,
            p,
            s,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )
        results = bs.apply(model.func, model.bootstrap.reps)
        return _calculate_quantiles(
            model.output_field_name,
            results,
            model.bootstrap.quantiles,
        )
    return bootstrap_func


def create_gumboots_func(model: MetricsBasemodel) -> Callable:
    """Create the GumBoots bootstrap function."""
    def bootstrap_func(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap function."""
        bs = GumBootsBootstrap(
            model.bootstrap.block_size,
            p,
            s,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )
        results = bs.apply(
            model.func,
            model.bootstrap.reps,
            model.bootstrap.time_field_name
        )
        return _calculate_quantiles(
            model.output_field_name,
            results,
            model.bootstrap.quantiles,
        )
    return bootstrap_func


def create_stationary_func(model: MetricsBasemodel) -> Callable:
    """Create the Stationary bootstrap function."""
    def bootstrap_func(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap function."""
        bs = StationaryBootstrap(
            model.bootstrap.block_size,
            p,
            s,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )
        results = bs.apply(model.func, model.bootstrap.reps)
        return _calculate_quantiles(
            model.output_field_name,
            results,
            model.bootstrap.quantiles,
        )
    return bootstrap_func
