"""Signature functions."""
import pandas as pd
import numpy as np


def max_value_time(
    p: pd.Series,
    value_time: pd.Series
) -> pd.Timestamp:
    """Max value time."""
    return value_time[p.idxmax()]


def variance(p: pd.Series) -> float:
    """Variance."""
    return np.var(p)


def count(p: pd.Series) -> float:
    """Count."""
    return len(p)


def minimum(p: pd.Series) -> float:
    """Minimum."""
    return np.min(p)


def maximum(p: pd.Series) -> float:
    """Maximum."""
    return np.max(p)


def average(p: pd.Series) -> float:
    """Average."""
    return np.mean(p)


def sum(p: pd.Series) -> float:
    """Sum."""
    return np.sum(p)