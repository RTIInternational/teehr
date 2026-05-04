"""Contains functions for bootstrap calculations for use in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np

from teehr.models.metrics.basemodels import MetricsBasemodel

logger = logging.getLogger(__name__)


def _optimal_block_size(data: np.ndarray, method: str) -> int:
    """Compute optimal block size using Politis-White-Patton algorithm.

    Parameters
    ----------
    data : np.ndarray
        1-D array of the primary metric input series.
    method : str
        ``"circular"`` returns ``b_cb``; ``"stationary"`` returns ``b_sb``.

    Returns
    -------
    int
        Optimal block size, minimum 2.
    """
    from arch.bootstrap import optimal_block_length

    result = optimal_block_length(data)
    if method == "circular":
        candidates = ("b_cb", "circular")
    else:
        candidates = ("b_sb", "stationary")

    col = next((c for c in candidates if c in result.columns), None)
    if col is None:
        raise ValueError(
            "optimal_block_length result did not contain expected columns for "
            f"method={method}: {result.columns.tolist()}"
        )

    block = int(np.ceil(result[col].iloc[0]))
    return max(block, 2)


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
    """Create the CircularBlock bootstrap function.

    If ``model.bootstrap.block_size`` is ``None``, the block size is estimated
    using ``arch.bootstrap.optimal_block_length`` (``b_cb`` column).
    """
    logger.debug("Building the Circular Block bootstrap func.")

    # lazy import to improve performance
    from arch.bootstrap import CircularBlockBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        block_size = model.bootstrap.block_size
        if block_size is None:
            block_size = _optimal_block_size(
                np.asarray(args[0], dtype=float), method="circular"
            )
            logger.debug(
                f"CircularBlock: auto block_size={block_size}"
            )
        bs = CircularBlockBootstrap(
            block_size,
            *args,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )

        results = bs.apply(
            model.func(model),
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


def create_gumboot_func(model: MetricsBasemodel) -> Callable:
    """Create the Gumboot bootstrap function."""
    logger.debug("Building the Gumboot bootstrap func.")

    # lazy import to improve performance
    from teehr.metrics.gumboot_bootstrap import GumbootBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        # value_time is always appended last when required by bootstrap config.
        vt = args[-1]
        metric_args = args[:-1]
        bs = GumbootBootstrap(
            *metric_args,
            value_time=vt,
            seed=model.bootstrap.seed,
            water_year_month=model.bootstrap.water_year_month,
            boot_year_file=model.bootstrap.boot_year_file
        )

        results = bs.apply(
            model.func(model),
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
    """Create the Stationary bootstrap function.

    If ``model.bootstrap.block_size`` is ``None``, the block size is estimated
    using ``arch.bootstrap.optimal_block_length`` (``b_sb`` column).
    """
    logger.debug("Building the Stationary bootstrap func.")

    # lazy import to improve performance
    from arch.bootstrap import StationaryBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        block_size = model.bootstrap.block_size
        if block_size is None:
            block_size = _optimal_block_size(
                np.asarray(args[0], dtype=float), method="stationary"
            )
            logger.debug(
                f"Stationary: auto block_size={block_size}"
            )
        bs = StationaryBootstrap(
            block_size,
            *args,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )

        results = bs.apply(
            model.func(model),
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
