"""Pandas Userdefined aggregate functions for use in Spark."""
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np
import numpy.typing as npt


@pandas_udf("float")
def primary_count(p: pd.Series, s: pd.Series) -> float:
    return len(p)


@pandas_udf("float")
def secondary_count(p: pd.Series, s: pd.Series) -> float:
    return len(s)


@pandas_udf("float")
def primary_minimum(p: pd.Series, s: pd.Series) -> float:
    return np.min(p)


@pandas_udf("float")
def secondary_minimum(p: pd.Series, s: pd.Series) -> float:
    return np.min(s)


@pandas_udf("float")
def primary_maximum(p: pd.Series, s: pd.Series) -> float:
    return np.max(p)


@pandas_udf("float")
def secondary_maximum(p: pd.Series, s: pd.Series) -> float:
    return np.max(s)


@pandas_udf("float")
def primary_average(p: pd.Series, s: pd.Series) -> float:
    return np.mean(p)


@pandas_udf("float")
def secondary_average(p: pd.Series, s: pd.Series) -> float:
    return np.mean(s)


@pandas_udf("float")
def primary_sum(p: pd.Series, s: pd.Series) -> float:
    return np.sum(p)


@pandas_udf("float")
def secondary_sum(p: pd.Series, s: pd.Series) -> float:
    return np.sum(s)


@pandas_udf("float")
def primary_variance(p: pd.Series, s: pd.Series) -> float:
    return np.var(p)


@pandas_udf("float")
def secondary_variance(p: pd.Series, s: pd.Series) -> float:
    return np.var(s)


@pandas_udf("float")
def mean_error(p: pd.Series, s: pd.Series) -> float:
    difference = s - p
    return np.sum(difference)/len(p)


@pandas_udf("float")
def relative_bias(p: pd.Series, s: pd.Series) -> float:
    difference = s - p
    return np.sum(difference)/np.sum(p)


@pandas_udf("float")
def mean_absolute_relative_error(p: pd.Series, s: pd.Series) -> float:
    absolute_difference = np.abs(s - p)
    return np.sum(absolute_difference)/np.sum(p)


@pandas_udf("float")
def multiplicative_bias(p: pd.Series, s: pd.Series) -> float:
    return np.mean(s)/np.mean(p)


@pandas_udf("float")
def pearson_correlation(p: pd.Series, s: pd.Series) -> float:
    return np.corrcoef(s, p)[0][1]


@pandas_udf("float")
def r_squared(p: pd.Series, s: pd.Series) -> float:
    pearson_correlation_coefficient = np.corrcoef(s, p)[0][1]
    return np.power(pearson_correlation_coefficient, 2)


@pandas_udf("float")
def max_value_delta(p: pd.Series, s: pd.Series) -> float:
    return np.max(s) - np.max(p)


@pandas_udf("float")
def annual_peak_relative_bias(
    p: pd.Series,
    s: pd.Series,
    value_time: pd.Series
) -> float:
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


@pandas_udf("float")
def spearman_correlation(p: pd.Series, s: pd.Series) -> float:
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

    me = np.sum(np.abs(np.subtract(y_true, y_pred)) ** power) / len(y_true)

    # Return mean error, optionally return root mean error
    if root:
        return np.sqrt(me)
    return me


@pandas_udf("float")
def nash_sutcliffe_efficiency(p: pd.Series, s: pd.Series) -> float:
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


@pandas_udf("float")
def nash_sutcliffe_efficiency_normalized(p: pd.Series, s: pd.Series) -> float:
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


@pandas_udf("float")
def kling_gupta_efficiency(p: pd.Series, s: pd.Series) -> float:
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


@pandas_udf("float")
def kling_gupta_efficiency_mod1(p: pd.Series, s: pd.Series) -> float:
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


@pandas_udf("float")
def kling_gupta_efficiency_mod2(p: pd.Series, s: pd.Series) -> float:
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


@pandas_udf("float")
def mean_absolute_error(p: pd.Series, s: pd.Series) -> float:
    return _mean_error(p, s)


@pandas_udf("float")
def mean_squared_error(p: pd.Series, s: pd.Series) -> float:
    return _mean_error(p, s, power=2.0)


@pandas_udf("float")
def root_mean_squared_error(p: pd.Series, s: pd.Series) -> float:
    return _mean_error(p, s, power=2.0, root=True)
