"""Contains UDFs for metric calculations for use in Spark queries."""
import numpy as np
import numpy.typing as npt
import pandas as pd


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


def variance(p: pd.Series) -> float:
    """Variance."""
    return np.var(p)


def mean_error(p: pd.Series, s: pd.Series) -> float:
    """Mean Error."""
    difference = s - p
    return np.sum(difference)/len(p)


def relative_bias(p: pd.Series, s: pd.Series) -> float:
    """Relative Bias."""
    difference = s - p
    return np.sum(difference)/np.sum(p)


def mean_absolute_relative_error(p: pd.Series, s: pd.Series) -> float:
    """Absolute Relative Error."""
    absolute_difference = np.abs(s - p)
    return np.sum(absolute_difference)/np.sum(p)


def multiplicative_bias(p: pd.Series, s: pd.Series) -> float:
    """Multiplicative Bias."""
    return np.mean(s)/np.mean(p)


def pearson_correlation(p: pd.Series, s: pd.Series) -> float:
    """Pearson Correlation Coefficient."""
    return np.corrcoef(s, p)[0][1]


def r_squared(p: pd.Series, s: pd.Series) -> float:
    """R-squared."""
    pearson_correlation_coefficient = np.corrcoef(s, p)[0][1]
    return np.power(pearson_correlation_coefficient, 2)


def max_value_delta(p: pd.Series, s: pd.Series) -> float:
    """Max value delta."""
    return np.max(s) - np.max(p)


def annual_peak_relative_bias(
    p: pd.Series,
    s: pd.Series,
    value_time: pd.Series
) -> float:
    """Annual peak relative bias."""
    df = pd.DataFrame(
        {
            "primary_value": p,
            "secondary_value": s,
            "value_time": value_time
        }
    )
    primary_yearly_max_values = df.groupby(
        df.value_time.dt.year
    ).primary_value.max()
    secondary_yearly_max_values = df.groupby(
        df.value_time.dt.year
    ).secondary_value.max()
    return np.sum(
        secondary_yearly_max_values
        - primary_yearly_max_values
        ) / np.sum(primary_yearly_max_values)


def spearman_correlation(p: pd.Series, s: pd.Series) -> float:
    """Spearman Rank Correlation Coefficient."""
    primary_rank = p.rank()
    secondary_rank = s.rank()
    count = len(p)
    return 1 - (
        6 * np.sum(np.abs(primary_rank - secondary_rank)**2)
        / (count * (count**2 - 1))
        )


def _mean_error(
    y_true: npt.ArrayLike,
    y_pred: npt.ArrayLike,
    power: float = 1.0,
    root: bool = False
) -> float:
    """Mean error."""
    me = np.sum(np.abs(np.subtract(y_true, y_pred)) ** power) / len(y_true)

    # Return mean error, optionally return root mean error
    if root:
        return np.sqrt(me)
    return me


def nash_sutcliffe_efficiency(p: pd.Series, s: pd.Series) -> float:
    """Nash-Sutcliffe Efficiency."""
    if len(p) == 0 or len(s) == 0:
        return np.nan
    if np.sum(p) == 0 or np.sum(s) == 0:
        return np.nan
    numerator = np.sum(np.subtract(p, s) ** 2)
    denominator = np.sum(np.subtract(p, np.mean(p)) ** 2)
    if numerator == np.nan or denominator  == np.nan:
        return np.nan
    if denominator  == 0:
        return np.nan
    return 1.0 - numerator/denominator


def nash_sutcliffe_efficiency_normalized(p: pd.Series, s: pd.Series) -> float:
    """Normalized Nash-Sutcliffe Efficiency."""
    if len(p) == 0 or len(s) == 0:
        return np.nan
    if np.sum(p) == 0 or np.sum(s) == 0:
        return np.nan
    numerator = np.sum(np.subtract(p, s) ** 2)
    denominator = np.sum(np.subtract(p, np.mean(p)) ** 2)
    if numerator == np.nan or denominator  == np.nan:
        return np.nan
    if denominator  == 0:
        return np.nan
    return 1.0 / (1.0 + numerator/denominator)


def kling_gupta_efficiency(p: pd.Series, s: pd.Series) -> float:
    """Kling-Gupta Efficiency (2009)."""
    # if len(p) == 0 or len(s) == 0:
    #     return np.nan
    # if np.sum(p) == 0 or np.sum(s) == 0:
    #     return np.nan
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


def kling_gupta_efficiency_mod1(p: pd.Series, s: pd.Series) -> float:
    """Kling-Gupta Efficiency - modified 1 (2012)."""
    # if len(p) == 0 or len(s) == 0:
    #     return np.nan
    # if np.sum(p) == 0 or np.sum(s) == 0:
    #     return np.nan
    # if np.min(p) == np.max(p) == 0 or np.min(s) == np.max(s) == 0:
    #     return np.nan
    if np.std(s) == 0 or np.std(p) == 0:
        return np.nan

    # Pearson correlation coefficient (same as kge)
    linear_correlation = np.corrcoef(s, p)[0, 1]

    # Variability_ratio
    variability_ratio = (
        (np.std(s) / np.mean(s))
        / (np.std(p) / np.mean(p))
    )

    # Relative mean (same as kge)
    relative_mean = (np.mean(s) / np.mean(p))

    # Scaled Euclidean distance
    euclidean_distance = np.sqrt(
        ((linear_correlation - 1.0)) ** 2.0 +
        ((variability_ratio - 1.0)) ** 2.0 +
        ((relative_mean - 1.0)) ** 2.0
        )

    return 1.0 - euclidean_distance


def kling_gupta_efficiency_mod2(p: pd.Series, s: pd.Series) -> float:
    """Kling-Gupta Efficiency - modified 2 (2021)."""
    # if len(p) == 0 or len(s) == 0:
    #     return np.nan
    # if np.sum(p) == 0 or np.sum(s) == 0:
    #     return np.nan
    if np.std(s) == 0 or np.std(p) == 0:
        return np.nan

    # Pearson correlation coefficient (same as kge)
    linear_correlation = np.corrcoef(s, p)[0, 1]

    # Relative variability (same as kge)
    relative_variability = (np.std(s) / np.std(p))

    # bias component
    bias_component = (
        ((np.mean(s) - np.mean(p)) ** 2)
        /
        (np.std(p) ** 2)
    )

    # Scaled Euclidean distance
    euclidean_distance = np.sqrt(
        ((linear_correlation - 1.0)) ** 2.0 +
        ((relative_variability - 1.0)) ** 2.0 +
        bias_component
        )

    return 1.0 - euclidean_distance


def mean_absolute_error(p: pd.Series, s: pd.Series) -> float:
    """Mean absolute error."""
    return _mean_error(p, s)


def mean_squared_error(p: pd.Series, s: pd.Series) -> float:
    """Mean squared error."""
    return _mean_error(p, s, power=2.0)


def root_mean_squared_error(p: pd.Series, s: pd.Series) -> float:
    """Root mean squared error."""
    return _mean_error(p, s, power=2.0, root=True)


def root_mean_standard_deviation_ratio(p: pd.Series, s: pd.Series) -> float:
    """Root mean standard deviation ratio."""
    rmse = root_mean_squared_error(p, s)
    obs_std_dev = np.std(p)
    return rmse / obs_std_dev


# Time-based Metrics
def max_value_timedelta(
    p: pd.Series,
    s: pd.Series,
    value_time: pd.Series
) -> float:
    """Max value time delta."""
    p_max_time = value_time[p.idxmax()]
    s_max_time = value_time[s.idxmax()]

    td = s_max_time - p_max_time

    return td.total_seconds()


def max_value_time(
    p: pd.Series,
    value_time: pd.Series
) -> pd.Timestamp:
    """Max value time."""
    return value_time[p.idxmax()]


# def secondary_max_value_time(
#     s: pd.Series,
#     value_time: pd.Series
# ) -> pd.Timestamp:
#     """Secondary max value time."""
#     return value_time[s.idxmax()]
