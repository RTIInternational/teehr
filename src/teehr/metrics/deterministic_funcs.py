"""Contains UDFs for deterministic metric calculations in Spark queries."""
import numpy as np
import numpy.typing as npt
import pandas as pd

from teehr.models.metrics.basemodels import MetricsBasemodel
from typing import Callable
import logging
logger = logging.getLogger(__name__)


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


def _root_mean_squared_error(p: pd.Series, s: pd.Series) -> float:
    """Root mean squared error."""
    return _mean_error(p, s, power=2.0, root=True)


def me_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the mean_error metric function."""
    logger.debug("Building the mean_error metric function")

    def mean_error(p: pd.Series, s: pd.Series) -> float:
        """Mean Error."""
        difference = s - p
        return np.sum(difference)/len(p)

    return mean_error


def rb_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the relative_bias metric function."""
    logger.debug("Building the relative_bias metric function")

    def relative_bias(p: pd.Series, s: pd.Series) -> float:
        """Relative Bias."""
        difference = s - p
        return np.sum(difference)/np.sum(p)

    return relative_bias


def mare_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the mean_absolute_relative_error metric function."""
    logger.debug("Building the mean_absolute_relative_error metric function")

    def mean_absolute_relative_error(p: pd.Series, s: pd.Series) -> float:
        """Absolute Relative Error."""
        absolute_difference = np.abs(s - p)
        return np.sum(absolute_difference)/np.sum(p)

    return mean_absolute_relative_error


def mb_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the multiplicative_bias metric function."""
    logger.debug("Building the multiplicative_bias metric function")

    def multiplicative_bias(p: pd.Series, s: pd.Series) -> float:
        """Multiplicative Bias."""
        return np.mean(s)/np.mean(p)

    return multiplicative_bias


def pc_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the pearson_correlation metric function."""
    logger.debug("Building the pearson_correlation metric function")

    def pearson_correlation(p: pd.Series, s: pd.Series) -> float:
        """Pearson Correlation Coefficient."""
        return np.corrcoef(s, p)[0][1]

    return pearson_correlation


def r_squared_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the R-squared metric function."""
    logger.debug("Building the R-squared metric function")

    def r_squared(p: pd.Series, s: pd.Series) -> float:
        """R-squared."""
        pearson_correlation_coefficient = np.corrcoef(s, p)[0][1]
        return np.power(pearson_correlation_coefficient, 2)

    return r_squared


def mvd_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the max_value_delta metric function."""
    logger.debug("Building the max_value_delta metric function")

    def max_value_delta(p: pd.Series, s: pd.Series) -> float:
        """Max value delta."""
        return np.max(s) - np.max(p)

    return max_value_delta


def aprb_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the annual_peak_relative_bias metric function."""
    logger.debug("Building the annual_peak_relative_bias metric function")

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

    return annual_peak_relative_bias


def spearman_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the Spearman metric function."""
    logger.debug("Building the spearman_correlation metric function")

    def spearman_correlation(p: pd.Series, s: pd.Series) -> float:
        """Spearman Rank Correlation Coefficient."""
        primary_rank = p.rank()
        secondary_rank = s.rank()
        count = len(p)
        return 1 - (
            6 * np.sum(np.abs(primary_rank - secondary_rank)**2)
            / (count * (count**2 - 1))
            )

    return spearman_correlation


def nse_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the nash_sutcliffe_efficiency metric function."""
    logger.debug("Building the nash_sutcliffe_efficiency metric function")

    def nash_sutcliffe_efficiency(p: pd.Series, s: pd.Series) -> float:
        """Nash-Sutcliffe Efficiency."""
        if len(p) == 0 or len(s) == 0:
            return np.nan
        if np.sum(p) == 0 or np.sum(s) == 0:
            return np.nan
        numerator = np.sum(np.subtract(p, s) ** 2)
        denominator = np.sum(np.subtract(p, np.mean(p)) ** 2)
        if numerator == np.nan or denominator == np.nan:
            return np.nan
        if denominator == 0:
            return np.nan
        return 1.0 - numerator/denominator

    return nash_sutcliffe_efficiency


def nse_norm_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the nash_sutcliffe_efficiency_normalized metric function."""
    logger.debug(
        "Building the nash_sutcliffe_efficiency_normalized metric function"
        )

    def nash_sutcliffe_efficiency_normalized(p: pd.Series, s: pd.Series) -> float:
        """Normalized Nash-Sutcliffe Efficiency."""
        if len(p) == 0 or len(s) == 0:
            return np.nan
        if np.sum(p) == 0 or np.sum(s) == 0:
            return np.nan
        numerator = np.sum(np.subtract(p, s) ** 2)
        denominator = np.sum(np.subtract(p, np.mean(p)) ** 2)
        if numerator == np.nan or denominator == np.nan:
            return np.nan
        if denominator == 0:
            return np.nan
        return 1.0 / (1.0 + numerator/denominator)

    return nash_sutcliffe_efficiency_normalized


def kge_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the kling_gupta_efficiency metric function."""
    logger.debug("Building the kling_gupta_efficiency metric function")

    def kling_gupta_efficiency(p: pd.Series,
                               s: pd.Series,
                               ) -> float:
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
            (model.sr * ((linear_correlation - 1.0) ** 2.0)) +
            (model.sa * ((relative_variability - 1.0) ** 2.0)) +
            (model.sb * ((relative_mean - 1.0) ** 2.0))
        )

        # Return KGE
        return 1.0 - euclidean_distance

    return kling_gupta_efficiency


def kge_mod1_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the kling_gupta_efficiency_mod1 metric function."""
    logger.debug("Building the kling_gupta_effiency_mod1 metric function")

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
            (model.sr * ((linear_correlation - 1.0) ** 2.0)) +
            (model.sa * ((variability_ratio - 1.0) ** 2.0)) +
            (model.sb * ((relative_mean - 1.0) ** 2.0))
        )

        return 1.0 - euclidean_distance

    return kling_gupta_efficiency_mod1


def kge_mod2_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the kling_gupta_efficiency_mod2 metric function."""
    logger.debug("Building the kling_gupta_efficiency_mod2 metric function")

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
            (model.sr * ((linear_correlation - 1.0) ** 2.0)) +
            (model.sa * ((relative_variability - 1.0) ** 2.0)) +
            (model.sb * bias_component)
        )

        return 1.0 - euclidean_distance

    return kling_gupta_efficiency_mod2


def mae_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the mean_absolute_error metric function."""
    logger.debug("Building the mean_absolute_error metric function")

    def mean_absolute_error(p: pd.Series, s: pd.Series) -> float:
        """Mean absolute error."""
        return _mean_error(p, s)

    return mean_absolute_error


def mse_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the mean_squared_error metric function."""
    logger.debug("Building the mean_squared_error metric function")

    def mean_squared_error(p: pd.Series, s: pd.Series) -> float:
        """Mean squared error."""
        return _mean_error(p, s, power=2.0)

    return mean_squared_error


def rmse_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the root_mean_squared_error metric function."""
    logger.debug("Building the root_mean_squared_error metric function")

    def root_mean_squared_error(p: pd.Series, s: pd.Series) -> float:
        """Root mean squared error."""
        return _mean_error(p, s, power=2.0, root=True)

    return root_mean_squared_error


def rmsdr_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the root_mean_standard_deviation_ratio metric function."""
    logger.debug(
        "Building the root_mean_standard_deviation_ratio metric function"
        )

    def root_mean_standard_deviation_ratio(p: pd.Series, s: pd.Series) -> float:
        """Root mean standard deviation ratio."""
        rmse = _root_mean_squared_error(p, s)
        obs_std_dev = np.std(p)
        return rmse / obs_std_dev

    return root_mean_standard_deviation_ratio


# Time-based Metrics
def mvtd_wrapper(model: MetricsBasemodel) -> Callable:
    """Create the max_value_timedelta metric function."""
    logger.debug("Building the max_value_timedelta metric function")

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

    return max_value_timedelta
