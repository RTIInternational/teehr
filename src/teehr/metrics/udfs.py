"""Contains UDFs for use in Spark queries."""
import numpy as np
import pandas as pd


def kling_gupta_efficiency(p: pd.Series, s: pd.Series) -> float:
    """KGE metric for comparing two timeseries."""
    if len(p) == 0 or len(s) == 0:
        return np.nan
    if np.sum(p) == 0 or np.sum(s) == 0:
        return np.nan
    if np.std(s) == 0 or np.std(p) == 0:
        return np.nan

    # Pearson correlation coefficient
    linear_correlation = np.corrcoef(s, p)[0,1]

    # Relative variability
    relative_variability = np.std(s) / np.std(p)

    # Relative mean
    relative_mean = np.mean(s) / np.mean(p)

    # Scaled Euclidean distance
    euclidean_distance = np.sqrt(
        (1.0 * (linear_correlation - 1.0)) ** 2.0 +
        (1.0  * (relative_variability - 1.0)) ** 2.0 +
        (1.0  * (relative_mean - 1.0)) ** 2.0
    )

    # Return KGE
    return 1.0 - euclidean_distance
