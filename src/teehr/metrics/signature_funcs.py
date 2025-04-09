"""Signature functions."""
import pandas as pd
import numpy as np

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.metrics.basemodels import TransformEnum
from typing import Callable, Optional
import logging
logger = logging.getLogger(__name__)


def _transform(
        p: pd.Series,
        model: MetricsBasemodel,
        t: Optional[pd.Series] = None
) -> tuple:
    """Apply timeseries transform for metrics calculations."""
    # Apply transform
    if model.transform is not None:
        match model.transform:
            case TransformEnum.log:
                logger.debug("Applying log transform")
                p = np.log(p)
            case TransformEnum.sqrt:
                logger.debug("Applying square root transform")
                p = np.sqrt(p)
            case TransformEnum.square:
                logger.debug("Applying square transform")
                p = np.square(p)
            case TransformEnum.cube:
                logger.debug("Applying cube transform")
                p = np.power(p, 3)
            case TransformEnum.exp:
                logger.debug("Applying exponential transform")
                p = np.exp(p)
            case TransformEnum.inv:
                logger.debug("Applying inverse transform")
                p = 1.0 / p
            case TransformEnum.abs:
                logger.debug("Applying absolute value transform")
                p = np.abs(p)
            case _:
                raise ValueError(
                    f"Unsupported transform: {model.transform}"
                )
    else:
        logger.debug("No transform specified, using original values")

    # Remove invalid values and align series
    if t is not None:
        if isinstance(t, pd.Series):
            valid_mask = np.isfinite(p)
            p = p[valid_mask]
            t = t[valid_mask]
        else:
            raise TypeError(
                f"Invalid type for t: {type(t)}. Expected pd.Series."
            )
    else:
        valid_mask = np.isfinite(p)
        p = p[valid_mask]

    if t is not None:
        return p, t
    else:
        return p


def mvt_wrapper(model: MetricsBasemodel) -> Callable:
    """Create max_value_time metric function."""
    logger.debug("Building the max_value_time metric function")

    def max_value_time(
        p: pd.Series,
        value_time: pd.Series
    ) -> pd.Timestamp:
        """Max value time."""
        p, value_time = _transform(p, model, value_time)
        return value_time[p.idxmax()]

    return max_value_time


def variance_wrapper(model: MetricsBasemodel) -> Callable:
    """Create variance metric function."""
    logger.debug("Building the variance metric function")

    def variance(p: pd.Series) -> float:
        """Variance."""
        p = _transform(p, model)
        return np.var(p)

    return variance


def count_wrapper(model: MetricsBasemodel) -> Callable:
    """Create count metric function."""
    logger.debug("Building the count metric function")

    def count(p: pd.Series) -> float:
        """Count."""
        p = _transform(p, model)
        return len(p)

    return count


def min_wrapper(model: MetricsBasemodel) -> Callable:
    """Create minimum metric function."""
    logger.debug("Building the minimum metric function")

    def minimum(p: pd.Series) -> float:
        """Minimum."""
        p = _transform(p, model)
        return np.min(p)

    return minimum


def max_wrapper(model: MetricsBasemodel) -> Callable:
    """Create maximum metric function."""
    logger.debug("Building the maximum metric function")

    def maximum(p: pd.Series) -> float:
        """Maximum."""
        p = _transform(p, model)
        return np.max(p)

    return maximum


def avg_wrapper(model: MetricsBasemodel) -> Callable:
    """Create average metric function."""
    logger.debug("Building the average metric function")

    def average(p: pd.Series) -> float:
        """Average."""
        p = _transform(p, model)
        return np.mean(p)

    return average


def sum_wrapper(model: MetricsBasemodel) -> Callable:
    """Create sum metric function."""
    logger.debug("Building the sum metric function")

    def sum(p: pd.Series) -> float:
        """Sum."""
        p = _transform(p, model)
        return np.sum(p)

    return sum
