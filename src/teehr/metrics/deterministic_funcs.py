"""Contains UDFs for deterministic metric calculations in Spark queries."""
import numpy as np
import numpy.typing as npt
import pandas as pd
from scipy.stats import rankdata

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.metrics.basemodels import TransformEnum

from typing import Callable, Optional
import logging
logger = logging.getLogger(__name__)

EPSILON = 1e-6  # Small constant to avoid division by zero


def _transform(
        p: pd.Series,
        s: pd.Series,
        model: MetricsBasemodel,
        t: Optional[pd.Series] = None
) -> tuple:
    """Apply timeseries transform for metrics calculations."""
    # Apply transform
    if model.transform is not None:
        match model.transform:
            case TransformEnum.log:
                if model.add_epsilon:
                    logger.debug(
                        "Applying epsilon before log transform"
                    )
                    p = p + EPSILON
                    s = s + EPSILON
                logger.debug("Applying log transform")
                p = np.log(p)
                s = np.log(s)
            case TransformEnum.sqrt:
                logger.debug("Applying square root transform")
                p = np.sqrt(p)
                s = np.sqrt(s)
            case TransformEnum.square:
                logger.debug("Applying square transform")
                p = np.square(p)
                s = np.square(s)
            case TransformEnum.cube:
                logger.debug("Applying cube transform")
                p = np.power(p, 3)
                s = np.power(s, 3)
            case TransformEnum.exp:
                logger.debug("Applying exponential transform")
                p = np.exp(p)
                s = np.exp(s)
            case TransformEnum.inv:
                if model.add_epsilon:
                    logger.debug(
                        "Applying epsilon before inverse transform"
                    )
                    p = p + EPSILON
                    s = s + EPSILON
                logger.debug("Applying inverse transform")
                p = 1.0 / p
                s = 1.0 / s
            case TransformEnum.abs:
                logger.debug("Applying absolute value transform")
                p = np.abs(p)
                s = np.abs(s)
            case _:
                raise ValueError(
                    f"Unsupported transform: {model.transform}"
                )
    else:
        logger.debug("No transform specified, using original values")

    # Remove invalid values and align series
    if t is not None:
        if isinstance(t, pd.Series):
            valid_mask = np.isfinite(p) & np.isfinite(s)
            p = p[valid_mask]
            s = s[valid_mask]
            t = t[valid_mask]
        else:
            raise TypeError(
                "t must be a pandas Series, not {type(t)}"
            )
    else:
        valid_mask = np.isfinite(p) & np.isfinite(s)
        p = p[valid_mask]
        s = s[valid_mask]

    if t is not None:
        return p, s, t
    else:
        return p, s


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


def mean_error(model: MetricsBasemodel) -> Callable:
    """Create the Mean Error metric function.

    :math:`Mean\\ Error=\\frac{\\sum(sec-prim)}{count}`
    """ # noqa
    logger.debug("Building the mean_error metric function")

    def mean_error_inner(p: pd.Series, s: pd.Series) -> float:
        """Mean Error."""
        p, s = _transform(p, s, model)
        difference = s - p
        return np.sum(difference)/len(p)

    return mean_error_inner


def relative_bias(model: MetricsBasemodel) -> Callable:
    """Create the Relative Bias metric function.

    :math:`Relative\\ Bias=\\frac{\\sum(sec-prim)}{\\sum(prim)}`
    """ # noqa
    logger.debug("Building the relative_bias metric function")

    def relative_bias_inner(p: pd.Series, s: pd.Series) -> float:
        """Relative Bias."""
        p, s = _transform(p, s, model)
        difference = s - p
        if model.add_epsilon:
            result = np.sum(difference)/(np.sum(p) + EPSILON)
        else:
            result = np.sum(difference)/np.sum(p)

        return result

    return relative_bias_inner


def mean_absolute_relative_error(model: MetricsBasemodel) -> Callable:
    """Create the Absolute Relative Error metric function.

    :math:`Relative\\ MAE=\\frac{\\sum|sec-prim|}{\\sum(prim)}`
    """ # noqa
    logger.debug("Building the mean_absolute_relative_error metric function")

    def mean_absolute_relative_error_inner(p: pd.Series,
                                           s: pd.Series) -> float:
        """Absolute Relative Error."""
        p, s = _transform(p, s, model)
        absolute_difference = np.abs(s - p)
        if model.add_epsilon:
            result = np.sum(absolute_difference)/(np.sum(p) + EPSILON)
        else:
            result = np.sum(absolute_difference)/np.sum(p)

        return result

    return mean_absolute_relative_error_inner


def multiplicative_bias(model: MetricsBasemodel) -> Callable:
    """Create the Multiplicative Bias metric function.

    :math:`Mult.\\ Bias=\\frac{\\mu_{sec}}{\\mu_{prim}}`
    """ # noqa
    logger.debug("Building the multiplicative_bias metric function")

    def multiplicative_bias_inner(p: pd.Series, s: pd.Series) -> float:
        """Multiplicative Bias."""
        p, s = _transform(p, s, model)
        if model.add_epsilon:
            result = np.mean(s)/(np.mean(p) + EPSILON)
        else:
            result = np.mean(s)/np.mean(p)

        return result

    return multiplicative_bias_inner


def pearson_correlation(model: MetricsBasemodel) -> Callable:
    """Create the Pearson Correlation Coefficient metric function.

    :math:`r=r(sec, prim)`
    """ # noqa
    logger.debug("Building the pearson_correlation metric function")

    def pearson_correlation_inner(p: pd.Series, s: pd.Series) -> float:
        """Pearson Correlation Coefficient."""
        p, s = _transform(p, s, model)

        if model.add_epsilon:
            # Calculate covariance between p and s
            numerator = np.cov(p, s)[0, 1]

            # Calculate standard deviations and multiply them
            denominator = np.nanstd(p) * np.nanstd(s) + EPSILON

            # Calculate correlation coefficient
            result = numerator / denominator

        else:
            result = np.corrcoef(s, p)[0][1]

        return result

    return pearson_correlation_inner


def variability_ratio(model: MetricsBasemodel) -> Callable:
    """Create the Variability Ratio metric function.

    :math:`VR=\\frac{\\sigma_{sec}}{\\sigma_{prim}}`
    """ # noqa
    logger.debug("Building the variability_ratio metric function")

    def variability_ratio_inner(p: pd.Series, s: pd.Series) -> float:
        """Variability Ratio."""
        p, s = _transform(p, s, model)
        if model.add_epsilon:
            result = (np.std(s))/(np.std(p) + EPSILON)
        else:
            result = np.std(s)/np.std(p)

        return result

    return variability_ratio_inner


def r_squared(model: MetricsBasemodel) -> Callable:
    """Create the R-squared metric function.

    :math:`r^2=r(sec, prim)^2`
    """ # noqa
    logger.debug("Building the R-squared metric function")

    def r_squared_inner(p: pd.Series, s: pd.Series) -> float:
        """R-squared."""
        p, s = _transform(p, s, model)

        if model.add_epsilon:
            # Calculate covariance between p and s
            numerator = np.cov(p, s)[0, 1]

            # Calculate standard deviations and multiply them
            denominator = np.nanstd(p) * np.nanstd(s) + EPSILON

            # Calculate correlation coefficient and square it
            pearson_correlation_coefficient = numerator / denominator
            result = np.power(pearson_correlation_coefficient, 2)

        else:
            pearson_correlation_coefficient = np.corrcoef(s, p)[0][1]
            result = np.power(pearson_correlation_coefficient, 2)

        return result

    return r_squared_inner


def max_value_delta(model: MetricsBasemodel) -> Callable:
    """Create the max_value_delta metric function.

    :math:`mvd=max(value_{sec})-max(value_{prim})`
    """ # noqa
    logger.debug("Building the max_value_delta metric function")

    def max_value_delta_inner(p: pd.Series, s: pd.Series) -> float:
        """Max value delta."""
        p, s = _transform(p, s, model)
        return np.max(s) - np.max(p)

    return max_value_delta_inner


def annual_peak_relative_bias(model: MetricsBasemodel) -> Callable:
    """Create the annual_peak_relative_bias metric function.

    :math:`Ann\\ PF\\ Bias=\\frac{\\sum(ann.\\ peak_{sec}-ann.\\ peak_{prim})}{\\sum(ann.\\ peak_{prim})}`
    """ # noqa
    logger.debug("Building the annual_peak_relative_bias metric function")

    def annual_peak_relative_bias_inner(
        p: pd.Series,
        s: pd.Series,
        value_time: pd.Series
    ) -> float:
        """Annual peak relative bias."""
        p, s, value_time = _transform(p, s, model, value_time)
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
        if model.add_epsilon:
            result = np.sum(
                secondary_yearly_max_values
                - primary_yearly_max_values
                ) / (np.sum(primary_yearly_max_values) + EPSILON)
        else:
            result = np.sum(
                secondary_yearly_max_values
                - primary_yearly_max_values
                ) / np.sum(primary_yearly_max_values)

        return result

    return annual_peak_relative_bias_inner


def spearman_correlation(model: MetricsBasemodel) -> Callable:
    """Create the Spearman metric function.

    :math:`r_s=1-\\frac{6*\\sum|rank_{prim}-rank_{sec}|^2}{count(count^2-1)}`
    """ # noqa
    logger.debug("Building the spearman_correlation metric function")

    def spearman_correlation_inner(p: pd.Series, s: pd.Series) -> float:
        """Spearman Rank Correlation Coefficient."""
        p, s = _transform(p, s, model)

        # calculate ranks (average method for ties)
        primary_ranks = rankdata(p, method='average')
        secondary_ranks = rankdata(s, method='average')

        # calculate covariance between p_rank and s_rank
        covariance = np.cov(primary_ranks, secondary_ranks)[0, 1]

        # calculate standard deviations of ranks
        std_primary = np.std(primary_ranks)
        std_secondary = np.std(secondary_ranks)

        if model.add_epsilon:
            result = covariance / (std_primary * std_secondary + EPSILON)
        else:
            result = covariance / (std_primary * std_secondary)

        return result

    return spearman_correlation_inner


def nash_sutcliffe_efficiency(model: MetricsBasemodel) -> Callable:
    """Create the nash_sutcliffe_efficiency metric function.

    :math:`NSE=1-\\frac{\\sum(prim-sec)^2}{\\sum(prim-\\mu_{prim}^2)}`
    """ # noqa
    logger.debug("Building the nash_sutcliffe_efficiency metric function")

    def nash_sutcliffe_efficiency_inner(p: pd.Series, s: pd.Series) -> float:
        """Nash-Sutcliffe Efficiency."""
        if len(p) == 0 or len(s) == 0:
            return np.nan
        if np.sum(p) == 0 or np.sum(s) == 0:
            return np.nan

        p, s = _transform(p, s, model)

        numerator = np.sum(np.subtract(p, s) ** 2)
        if model.add_epsilon:
            denominator = np.sum(np.subtract(p, np.mean(p)) ** 2) + EPSILON
        else:
            denominator = np.sum(np.subtract(p, np.mean(p)) ** 2)
        if numerator == np.nan or denominator == np.nan:
            return np.nan
        if denominator == 0:
            return np.nan
        return 1.0 - numerator/denominator

    return nash_sutcliffe_efficiency_inner


def nash_sutcliffe_efficiency_normalized(model: MetricsBasemodel) -> Callable:
    """Create the nash_sutcliffe_efficiency_normalized metric function.

    :math:`NNSE=\\frac{1}{(2-NSE)}`
    """ # noqa
    logger.debug(
        "Building the nash_sutcliffe_efficiency_normalized metric function"
        )

    def nash_sutcliffe_efficiency_normalized_inner(p: pd.Series,
                                                   s: pd.Series
                                                   ) -> float:
        """Apply normalized Nash-Sutcliffe Efficiency."""
        if len(p) == 0 or len(s) == 0:
            return np.nan
        if np.sum(p) == 0 or np.sum(s) == 0:
            return np.nan

        p, s = _transform(p, s, model)

        numerator = np.sum(np.subtract(p, s) ** 2)
        if model.add_epsilon:
            denominator = np.sum(np.subtract(p, np.mean(p)) ** 2) + EPSILON
        else:
            denominator = np.sum(np.subtract(p, np.mean(p)) ** 2)
        if numerator == np.nan or denominator == np.nan:
            return np.nan
        if denominator == 0:
            return np.nan
        return 1.0 / (1.0 + numerator/denominator)

    return nash_sutcliffe_efficiency_normalized_inner


def kling_gupta_efficiency(model: MetricsBasemodel) -> Callable:
    """Create the kling_gupta_efficiency metric function.

    :math:`KGE=1-\\sqrt{(r(sec, prim)-1)^2+(\\frac{\\sigma_{sec}}{\\sigma_{prim}}-1)^2+(\\frac{\\mu_{sec}}{\\mu_{sec}/\\mu_{prim}}-1)^2}`
    """ # noqa
    logger.debug("Building the kling_gupta_efficiency metric function")

    def kling_gupta_efficiency_inner(p: pd.Series,
                                     s: pd.Series,
                                     ) -> float:
        """Kling-Gupta Efficiency (2009)."""
        if np.std(s) == 0 or np.std(p) == 0:
            return np.nan

        p, s = _transform(p, s, model)

        # Pearson correlation coefficient
        linear_correlation = np.corrcoef(s, p)[0, 1]

        # Relative variability
        if model.add_epsilon:
            relative_variability = np.std(s) / (np.std(p) + EPSILON)
        else:
            relative_variability = np.std(s) / np.std(p)

        # Relative mean
        if model.add_epsilon:
            relative_mean = np.mean(s) / (np.mean(p) + EPSILON)
        else:
            relative_mean = np.mean(s) / np.mean(p)

        # Scaled Euclidean distance
        euclidean_distance = np.sqrt(
            (model.sr * ((linear_correlation - 1.0) ** 2.0)) +
            (model.sa * ((relative_variability - 1.0) ** 2.0)) +
            (model.sb * ((relative_mean - 1.0) ** 2.0))
        )

        # Return KGE
        return 1.0 - euclidean_distance

    return kling_gupta_efficiency_inner


def kling_gupta_efficiency_mod1(model: MetricsBasemodel) -> Callable:
    """Create the kling_gupta_efficiency_mod1 metric function.

    :math:`KGE'=1-\\sqrt{(r(sec, prim)-1)^2+(\\frac{\\sigma_{sec}/\\mu_{sec}}{\\sigma_{prim}/\\mu_{prim}}-1)^2+(\\frac{\\mu_{sec}}{\\mu_{sec}/\\mu_{prim}}-1)^2}`
    """ # noqa
    logger.debug("Building the kling_gupta_effiency_mod1 metric function")

    def kling_gupta_efficiency_mod1_inner(p: pd.Series, s: pd.Series) -> float:
        """Kling-Gupta Efficiency - modified 1 (2012)."""
        if np.std(s) == 0 or np.std(p) == 0:
            return np.nan

        p, s = _transform(p, s, model)

        # Pearson correlation coefficient (same as kge)
        linear_correlation = np.corrcoef(s, p)[0, 1]

        # Variability_ratio
        if model.add_epsilon:
            variability_ratio = (
                (np.std(s) / (np.mean(s) + EPSILON))
                / (np.std(p) / (np.mean(p) + EPSILON))
            )
        else:
            variability_ratio = (
                (np.std(s) / np.mean(s))
                / (np.std(p) / np.mean(p))
            )
        # Relative mean (same as kge)
        if model.add_epsilon:
            relative_mean = (np.mean(s) / (np.mean(p) + EPSILON))
        else:
            relative_mean = (np.mean(s) / np.mean(p))

        # Scaled Euclidean distance
        euclidean_distance = np.sqrt(
            (model.sr * ((linear_correlation - 1.0) ** 2.0)) +
            (model.sa * ((variability_ratio - 1.0) ** 2.0)) +
            (model.sb * ((relative_mean - 1.0) ** 2.0))
        )

        return 1.0 - euclidean_distance

    return kling_gupta_efficiency_mod1_inner


def kling_gupta_efficiency_mod2(model: MetricsBasemodel) -> Callable:
    """Create the kling_gupta_efficiency_mod2 metric function.

    :math:`KGE''=1-\\sqrt{(r(sec, prim)-1)^2+(\\frac{\\sigma_{sec}}{\\sigma_{prim}}-1)^2+\\frac{(\\mu_{sec}-\\mu_{prim})^2}{\\sigma_{prim}^2}}`
    """ # noqa
    logger.debug("Building the kling_gupta_efficiency_mod2 metric function")

    def kling_gupta_efficiency_mod2_inner(p: pd.Series, s: pd.Series) -> float:
        """Kling-Gupta Efficiency - modified 2 (2021)."""
        if np.std(s) == 0 or np.std(p) == 0:
            return np.nan

        p, s = _transform(p, s, model)

        # Pearson correlation coefficient (same as kge)
        linear_correlation = np.corrcoef(s, p)[0, 1]

        # Relative variability (same as kge)
        if model.add_epsilon:
            relative_variability = (np.std(s) / (np.std(p) + EPSILON))
        else:
            relative_variability = (np.std(s) / np.std(p))

        # bias component
        if model.add_epsilon:
            bias_component = (
                ((np.mean(s) - np.mean(p)) ** 2)
                /
                ((np.std(p) ** 2) + EPSILON)
            )
        else:
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

    return kling_gupta_efficiency_mod2_inner


def mean_absolute_error(model: MetricsBasemodel) -> Callable:
    """Create the mean_absolute_error metric function.

    :math:`MAE=\\frac{\\sum|sec-prim|}{count}`
    """ # noqa
    logger.debug("Building the mean_absolute_error metric function")

    def mean_absolute_error_inner(p: pd.Series, s: pd.Series) -> float:
        """Mean absolute error."""
        p, s = _transform(p, s, model)
        return _mean_error(p, s)

    return mean_absolute_error_inner


def mean_squared_error(model: MetricsBasemodel) -> Callable:
    """Create the mean_squared_error metric function.

    :math:`MSE=\\frac{\\sum(sec-prim)^2}{count}`
    """ # noqa
    logger.debug("Building the mean_squared_error metric function")

    def mean_squared_error_inner(p: pd.Series, s: pd.Series) -> float:
        """Mean squared error."""
        p, s = _transform(p, s, model)
        return _mean_error(p, s, power=2.0)

    return mean_squared_error_inner


def root_mean_squared_error(model: MetricsBasemodel) -> Callable:
    """Create the root_mean_squared_error metric function.

    :math:`RMSE=\\sqrt{\\frac{\\sum(sec-prim)^2}{count}}`
    """ # noqa
    logger.debug("Building the root_mean_squared_error metric function")

    def root_mean_squared_error_inner(p: pd.Series, s: pd.Series) -> float:
        """Root mean squared error."""
        p, s = _transform(p, s, model)
        return _mean_error(p, s, power=2.0, root=True)

    return root_mean_squared_error_inner


def root_mean_standard_deviation_ratio(model: MetricsBasemodel) -> Callable:
    """Create the root_mean_standard_deviation_ratio metric function.

    :math:`RMSE_{ratio}=\\frac{RMSE}{\\sigma_{obs}}`
    """ # noqa
    logger.debug(
        "Building the root_mean_standard_deviation_ratio metric function"
        )

    def root_mean_standard_deviation_ratio_inner(p: pd.Series,
                                                 s: pd.Series
                                                 ) -> float:
        """Root mean standard deviation ratio."""
        p, s = _transform(p, s, model)
        rmse = _root_mean_squared_error(p, s)
        obs_std_dev = np.std(p)
        if model.add_epsilon:
            result = rmse / (obs_std_dev + EPSILON)
        else:
            result = rmse / obs_std_dev

        return result

    return root_mean_standard_deviation_ratio_inner


# Time-based Metrics
def max_value_timedelta(model: MetricsBasemodel) -> Callable:
    """Create the max_value_timedelta metric function.

    :math:`mvtd=max\\_value\\_time_{sec}-max\\_value\\_time_{prim}`
    """ # noqa
    logger.debug("Building the max_value_timedelta metric function")

    def max_value_timedelta_inner(
        p: pd.Series,
        s: pd.Series,
        value_time: pd.Series
    ) -> float:
        """Max value time delta."""
        p, s, value_time = _transform(p, s, model, value_time)
        p_max_time = value_time[p.idxmax()]
        s_max_time = value_time[s.idxmax()]

        td = s_max_time - p_max_time

        return td.total_seconds()

    return max_value_timedelta_inner
