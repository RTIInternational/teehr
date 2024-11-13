"""Contains functions for bootstrap calculations for use in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np
from arch.bootstrap import (
    StationaryBootstrap,
    CircularBlockBootstrap,
)

from teehr.models.metrics.metric_models import MetricsBasemodel
from teehr.metrics.gumboot_bootstrap import GumbootBootstrap

logger = logging.getLogger(__name__)


def _calculate_quantiles(
    output_field_name: str,
    results: np.ndarray,
    quantiles: list
) -> Dict:
    """Calculate quantile values of the bootstrap results."""
    values = np.quantile(results, quantiles)
    quantiles = [f"{output_field_name}_{str(i)}" for i in quantiles]
    d = dict(zip(quantiles, values))
    return d


def create_circularblock_func(model: MetricsBasemodel) -> Callable:
    """Create the CircularBlock bootstrap function."""
    logger.debug("Building the Circular Block bootstrap func.")

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
        if model.bootstrap.quantiles is not None:
            return _calculate_quantiles(
                model.output_field_name,
                results,
                model.bootstrap.quantiles,
            )
        else:
            return results.ravel()
    return bootstrap_func


def create_gumboot_func(model: MetricsBasemodel) -> Callable:
    """Create the Gumboot bootstrap function."""
    logger.debug("Building the Gumboot bootstrap func.")

    def bootstrap_func(p: pd.Series, s: pd.Series, vt: pd.Series) -> Dict:
        """Bootstrap function."""
        bs = GumbootBootstrap(
            p,
            s,
            value_time=vt,
            seed=model.bootstrap.seed,
            water_year_month=model.bootstrap.water_year_month,
            boot_year_file=model.bootstrap.boot_year_file
        )
        results = bs.apply(
            model.func,
            model.bootstrap.reps
        )
        if model.bootstrap.quantiles is not None:
            return _calculate_quantiles(
                model.output_field_name,
                results,
                model.bootstrap.quantiles,
            )
        else:
            return results.ravel()
    return bootstrap_func


def create_stationary_func(model: MetricsBasemodel) -> Callable:
    """Create the Stationary bootstrap function."""
    logger.debug("Building the Stationary bootstrap func.")

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
        if model.bootstrap.quantiles is not None:
            return _calculate_quantiles(
                model.output_field_name,
                results,
                model.bootstrap.quantiles,
            )
        else:
            return results.ravel()
    return bootstrap_func
