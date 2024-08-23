"""Contains UDFs for bootstrap calculations for use in Spark queries."""
from typing import Dict, Callable, Any

import pandas as pd
import numpy as np
from arch.bootstrap import (
    StationaryBootstrap,
    CircularBlockBootstrap,
)

# from teehr.models.metrics.metrics_models import MetricsBasemodel  # circular
from teehr.metrics.gumboots_bootstrap import GumBootsBootstrap


# NOTE: All UDFs need to return the same signature.
# What if the user specifies a different number of quantiles??
# We need some generic way to return the data.
def _unpack_bootstrap_results(
    output_field_name: str,
    results: np.ndarray,
    quantiles: list
) -> Dict:
    """Unpack the bootstrap results."""
    values = np.quantile(results, quantiles)
    quantiles = [f"{output_field_name}_{str(i)}" for i in quantiles]
    d = dict(zip(quantiles, values))
    return d


def create_circularblock_udf(model: Any) -> Callable:
    """Create the CircularBlock bootstrap UDF."""
    def bootstrap_udf(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap UDF."""
        bs = CircularBlockBootstrap(
            model.bootstrap.block_size,
            p,
            s,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )
        results = bs.apply(model.func, model.bootstrap.reps)
        return _unpack_bootstrap_results(
            model.output_field_name,
            results,
            model.bootstrap.quantiles,
        )
    return bootstrap_udf


def create_gumboots_udf(model: Any) -> Callable:
    """Create the GumBoots bootstrap UDF."""
    def bootstrap_udf(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap UDF."""
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
        return _unpack_bootstrap_results(
            model.output_field_name,
            results,
            model.bootstrap.quantiles,
        )
    return bootstrap_udf


def create_stationary_udf(model: Any) -> Callable:
    """Create the Stationary bootstrap UDF."""
    def bootstrap_udf(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap UDF."""
        bs = StationaryBootstrap(
            model.bootstrap.block_size,
            p,
            s,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )
        results = bs.apply(model.func, model.bootstrap.reps)
        return _unpack_bootstrap_results(
            model.output_field_name,
            results,
            model.bootstrap.quantiles,
        )
    return bootstrap_udf
