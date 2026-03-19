"""Classes representing available performance metrics.

This module defines deterministic metrics using a compact class-variable pattern.
Each metric class inherits common fields from DeterministicBasemodel and only
specifies metric-specific defaults via class variables.
"""
from pyspark.sql import types as T

from pydantic import Field
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.basemodels import (
    DeterministicBasemodel,
    ThresholdBasemodel,
)
from teehr.metrics import deterministic_funcs as metric_funcs


# =============================================================================
# Standard Deterministic Metrics (primary_value, secondary_value inputs)
# =============================================================================

class MeanError(DeterministicBasemodel):
    """Mean Error: average difference between secondary and primary values."""

    default_output_field_name = "mean_error"
    default_func = metric_funcs.mean_error
    default_attrs = tma.ME_ATTRS


class RelativeBias(DeterministicBasemodel):
    """Relative Bias: sum of differences divided by sum of primary values."""

    default_output_field_name = "relative_bias"
    default_func = metric_funcs.relative_bias
    default_attrs = tma.RBIAS_ATTRS


class MultiplicativeBias(DeterministicBasemodel):
    """Multiplicative Bias: ratio of secondary mean to primary mean."""

    default_output_field_name = "multiplicative_bias"
    default_func = metric_funcs.multiplicative_bias
    default_attrs = tma.MULTBIAS_ATTRS


class MeanSquareError(DeterministicBasemodel):
    """Mean Square Error: average of squared differences."""

    default_output_field_name = "mean_square_error"
    default_func = metric_funcs.mean_squared_error
    default_attrs = tma.MSE_ATTRS


class RootMeanSquareError(DeterministicBasemodel):
    """Root Mean Square Error: square root of mean squared error."""

    default_output_field_name = "root_mean_square_error"
    default_func = metric_funcs.root_mean_squared_error
    default_attrs = tma.RMSE_ATTRS


class MeanAbsoluteError(DeterministicBasemodel):
    """Mean Absolute Error: average of absolute differences."""

    default_output_field_name = "mean_absolute_error"
    default_func = metric_funcs.mean_absolute_error
    default_attrs = tma.MAE_ATTRS


class MeanAbsoluteRelativeError(DeterministicBasemodel):
    """Mean Absolute Relative Error: sum of absolute differences / sum of primary."""

    default_output_field_name = "mean_absolute_relative_error"
    default_func = metric_funcs.mean_absolute_relative_error
    default_attrs = tma.RMAE_ATTRS


class PearsonCorrelation(DeterministicBasemodel):
    """Pearson Correlation Coefficient: linear correlation between series."""

    default_output_field_name = "pearson_correlation"
    default_func = metric_funcs.pearson_correlation
    default_attrs = tma.PEARSON_ATTRS


class VariabilityRatio(DeterministicBasemodel):
    """Variability Ratio: ratio of secondary std dev to primary std dev."""

    default_output_field_name = "variability_ratio"
    default_func = metric_funcs.variability_ratio
    default_attrs = tma.VR_ATTRS


class Rsquared(DeterministicBasemodel):
    """Coefficient of Determination: square of Pearson correlation."""

    default_output_field_name = "r_squared"
    default_func = metric_funcs.r_squared
    default_attrs = tma.R2_ATTRS


class NashSutcliffeEfficiency(DeterministicBasemodel):
    """Nash-Sutcliffe Efficiency: 1 - (MSE / variance of primary)."""

    default_output_field_name = "nash_sutcliffe_efficiency"
    default_func = metric_funcs.nash_sutcliffe_efficiency
    default_attrs = tma.NSE_ATTRS


class NormalizedNashSutcliffeEfficiency(DeterministicBasemodel):
    """Normalized Nash-Sutcliffe Efficiency: 1 / (2 - NSE)."""

    default_output_field_name = "nash_sutcliffe_efficiency_normalized"
    default_func = metric_funcs.nash_sutcliffe_efficiency_normalized
    default_attrs = tma.NNSE_ATTRS


class SpearmanCorrelation(DeterministicBasemodel):
    """Spearman Rank Correlation Coefficient: rank-based correlation."""

    default_output_field_name = "spearman_correlation"
    default_func = metric_funcs.spearman_correlation
    default_attrs = tma.SPEARMAN_R_ATTRS


class MaxValueDelta(DeterministicBasemodel):
    """Max Value Delta: difference between maximum values."""

    default_output_field_name = "max_value_delta"
    default_func = metric_funcs.max_value_delta
    default_attrs = tma.MAX_VALUE_DELTA_ATTRS


class RootMeanStandardDeviationRatio(DeterministicBasemodel):
    """Root Mean Standard Deviation Ratio (RSR): RMSE / std dev of primary."""

    default_output_field_name = "root_mean_standard_deviation_ratio"
    default_func = metric_funcs.root_mean_standard_deviation_ratio
    default_attrs = tma.RSR_ATTRS


# =============================================================================
# Kling-Gupta Efficiency Variants (with scaling factors)
# =============================================================================

class KlingGuptaEfficiency(DeterministicBasemodel):
    """Kling-Gupta Efficiency (original formulation).

    Additional Parameters
    ---------------------
    sr : float
        Scaling factor for correlation component, by default 1.0.
    sa : float
        Scaling factor for variability component, by default 1.0.
    sb : float
        Scaling factor for bias component, by default 1.0.
    """

    default_output_field_name = "kling_gupta_efficiency"
    default_func = metric_funcs.kling_gupta_efficiency
    default_attrs = tma.KGE_ATTRS

    sr: float = Field(default=1.0)
    sa: float = Field(default=1.0)
    sb: float = Field(default=1.0)


class KlingGuptaEfficiencyMod1(DeterministicBasemodel):
    """Kling-Gupta Efficiency - modified 1 (2012).

    Additional Parameters
    ---------------------
    sr : float
        Scaling factor for correlation component, by default 1.0.
    sa : float
        Scaling factor for variability component, by default 1.0.
    sb : float
        Scaling factor for bias component, by default 1.0.
    """

    default_output_field_name = "kling_gupta_efficiency_mod1"
    default_func = metric_funcs.kling_gupta_efficiency_mod1
    default_attrs = tma.KGE1_ATTRS

    sr: float = Field(default=1.0)
    sa: float = Field(default=1.0)
    sb: float = Field(default=1.0)


class KlingGuptaEfficiencyMod2(DeterministicBasemodel):
    """Kling-Gupta Efficiency - modified 2 (2021).

    Additional Parameters
    ---------------------
    sr : float
        Scaling factor for correlation component, by default 1.0.
    sa : float
        Scaling factor for variability component, by default 1.0.
    sb : float
        Scaling factor for bias component, by default 1.0.
    """

    default_output_field_name = "kling_gupta_efficiency_mod2"
    default_func = metric_funcs.kling_gupta_efficiency_mod2
    default_attrs = tma.KGE2_ATTRS

    sr: float = Field(default=1.0)
    sa: float = Field(default=1.0)
    sb: float = Field(default=1.0)


# =============================================================================
# Metrics Requiring value_time Field
# =============================================================================

class MaxValueTimeDelta(DeterministicBasemodel):
    """Max Value Time Delta: time difference between max value occurrences."""

    default_output_field_name = "max_value_time_delta"
    default_func = metric_funcs.max_value_timedelta
    default_attrs = tma.MAX_VALUE_TIMEDELTA_ATTRS
    default_input_field_names = ["primary_value", "secondary_value", "value_time"]


class AnnualPeakRelativeBias(DeterministicBasemodel):
    """Annual Peak Relative Bias: bias computed on annual peak values."""

    default_output_field_name = "annual_peak_flow_bias"
    default_func = metric_funcs.annual_peak_relative_bias
    default_attrs = tma.ANNUAL_PEAK_RBIAS_ATTRS
    default_input_field_names = ["primary_value", "secondary_value", "value_time"]


# =============================================================================
# Threshold-Based Metrics (require threshold_field_name)
# =============================================================================

class ConfusionMatrix(ThresholdBasemodel):
    """Confusion Matrix: TP, TN, FP, FN counts based on threshold exceedance.

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "confusion_matrix"
    default_func = metric_funcs.confusion_matrix
    default_attrs = tma.CM_ATTRS

    return_type: T.DataType = Field(
        default=T.MapType(T.StringType(), T.IntegerType()), frozen=True
    )


class FalseAlarmRatio(ThresholdBasemodel):
    """False Alarm Ratio: FP / (TP + FP).

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "false_alarm_ratio"
    default_func = metric_funcs.false_alarm_ratio
    default_attrs = tma.FAR_ATTRS


class ProbabilityOfDetection(ThresholdBasemodel):
    """Probability of Detection (Hit Rate): TP / (TP + FN).

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "probability_of_detection"
    default_func = metric_funcs.probability_of_detection
    default_attrs = tma.POD_ATTRS


class ProbabilityOfFalseDetection(ThresholdBasemodel):
    """Probability of False Detection: FP / (TN + FP).

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "probability_of_false_detection"
    default_func = metric_funcs.probability_of_false_detection
    default_attrs = tma.POFD_ATTRS


class CriticalSuccessIndex(ThresholdBasemodel):
    """Critical Success Index (Threat Score): TP / (TP + FN + FP).

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "critical_success_index"
    default_func = metric_funcs.critical_success_index
    default_attrs = tma.CSI_ATTRS


class SuccessRatio(ThresholdBasemodel):
    """Success Ratio: TP + TN / (TP + FP + FN + TN).

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "success_ratio"
    default_func = metric_funcs.success_ratio
    default_attrs = tma.SR_ATTRS


class FrequencyBiasIndex(ThresholdBasemodel):
    """Frequency Bias Index: (TP + FP) / (TP + FN).

    Additional Parameters
    ---------------------
    threshold_field_name : str
        Field name containing location-specific threshold values.
    """

    default_output_field_name = "frequency_bias_index"
    default_func = metric_funcs.frequency_bias_index
    default_attrs = tma.FBIAS_ATTRS


# =============================================================================
# Container Class for Discovery
# =============================================================================

class DeterministicMetrics:
    """Define and customize deterministic metrics.

    Notes
    -----
    Deterministic metrics compare two timeseries, typically primary
    ("observed") vs. secondary ("modeled") values. Available metrics:

    **Error Metrics:**
    - MeanError, MeanSquareError, RootMeanSquareError
    - MeanAbsoluteError, MeanAbsoluteRelativeError

    **Bias Metrics:**
    - RelativeBias, MultiplicativeBias, AnnualPeakRelativeBias

    **Correlation Metrics:**
    - PearsonCorrelation, SpearmanCorrelation, Rsquared

    **Efficiency Metrics:**
    - NashSutcliffeEfficiency, NormalizedNashSutcliffeEfficiency
    - KlingGuptaEfficiency, KlingGuptaEfficiencyMod1, KlingGuptaEfficiencyMod2

    **Threshold-Based Metrics:**
    - ConfusionMatrix, FalseAlarmRatio, ProbabilityOfDetection
    - ProbabilityOfFalseDetection, CriticalSuccessIndex

    **Other:**
    - MaxValueDelta, MaxValueTimeDelta
    - RootMeanStandardDeviationRatio

    Example
    -------
    >>> from teehr import DeterministicMetrics
    >>> kge = DeterministicMetrics.KlingGuptaEfficiency(transform="log", add_epsilon=True)
    >>> rmse = DeterministicMetrics.RootMeanSquareError()
    """

    # Error metrics
    MeanError = MeanError
    MeanSquareError = MeanSquareError
    RootMeanSquareError = RootMeanSquareError
    MeanAbsoluteError = MeanAbsoluteError
    MeanAbsoluteRelativeError = MeanAbsoluteRelativeError

    # Bias metrics
    RelativeBias = RelativeBias
    MultiplicativeBias = MultiplicativeBias
    AnnualPeakRelativeBias = AnnualPeakRelativeBias

    # Correlation metrics
    PearsonCorrelation = PearsonCorrelation
    SpearmanCorrelation = SpearmanCorrelation
    Rsquared = Rsquared

    # Efficiency metrics
    NashSutcliffeEfficiency = NashSutcliffeEfficiency
    NormalizedNashSutcliffeEfficiency = NormalizedNashSutcliffeEfficiency
    KlingGuptaEfficiency = KlingGuptaEfficiency
    KlingGuptaEfficiencyMod1 = KlingGuptaEfficiencyMod1
    KlingGuptaEfficiencyMod2 = KlingGuptaEfficiencyMod2

    # Peak/max value metrics
    MaxValueDelta = MaxValueDelta
    MaxValueTimeDelta = MaxValueTimeDelta
    RootMeanStandardDeviationRatio = RootMeanStandardDeviationRatio

    # Threshold-based metrics
    ConfusionMatrix = ConfusionMatrix
    FalseAlarmRatio = FalseAlarmRatio
    ProbabilityOfDetection = ProbabilityOfDetection
    ProbabilityOfFalseDetection = ProbabilityOfFalseDetection
    CriticalSuccessIndex = CriticalSuccessIndex
    SuccessRatio = SuccessRatio
    FrequencyBiasIndex = FrequencyBiasIndex
