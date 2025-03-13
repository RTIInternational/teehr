"""Signature functions."""
import pandas as pd
import numpy as np

from teehr.models.metrics.basemodels import MetricsBasemodel
from typing import Callable
import logging
logger = logging.getLogger(__name__)


def mvt_wrapper(model: MetricsBasemodel) -> Callable:
    """Create max_value_time metric function."""
    logger.debug("Building the max_value_time metric function")

    def max_value_time(
        p: pd.Series,
        value_time: pd.Series
    ) -> pd.Timestamp:
        """Max value time."""
        return value_time[p.idxmax()]

    return max_value_time


def variance_wrapper(model: MetricsBasemodel) -> Callable:
    """Create variance metric function."""
    logger.debug("Building the variance metric function")

    def variance(p: pd.Series) -> float:
        """Variance."""
        return np.var(p)

    return variance


def count_wrapper(model: MetricsBasemodel) -> Callable:
    """Create count metric function."""
    logger.debug("Building the count metric function")

    def count(p: pd.Series) -> float:
        """Count."""
        return len(p)

    return count


def min_wrapper(model: MetricsBasemodel) -> Callable:
    """Create minimum metric function."""
    logger.debug("Building the minimum metric function")

    def minimum(p: pd.Series) -> float:
        """Minimum."""
        return np.min(p)

    return minimum


def max_wrapper(model: MetricsBasemodel) -> Callable:
    """Create maximum metric function."""
    logger.debug("Building the maximum metric function")

    def maximum(p: pd.Series) -> float:
        """Maximum."""
        return np.max(p)

    return maximum


def avg_wrapper(model: MetricsBasemodel) -> Callable:
    """Create average metric function."""
    logger.debug("Building the average metric function")

    def average(p: pd.Series) -> float:
        """Average."""
        return np.mean(p)

    return average


def sum_wrapper(model: MetricsBasemodel) -> Callable:
    """Create sum metric function."""
    logger.debug("Building the sum metric function")

    def sum(p: pd.Series) -> float:
        """Sum."""
        return np.sum(p)

    return sum
