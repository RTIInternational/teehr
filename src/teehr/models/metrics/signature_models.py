"""Signature models for metrics.

This module defines signature metrics (single-field statistics) using a compact
class-variable pattern. Each metric class inherits common fields from
SignatureBasemodel and only specifies metric-specific defaults via class variables.
"""
from typing import ClassVar, Dict, List

from pydantic import Field
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.basemodels import SignatureBasemodel
from teehr.metrics import signature_funcs as sig_funcs


# =============================================================================
# Basic Statistics (single field: primary_value)
# =============================================================================

class Count(SignatureBasemodel):
    """Count: number of non-null values in the series."""

    default_output_field_name: ClassVar[str] = "count"
    default_func = sig_funcs.count
    default_attrs: ClassVar[Dict] = tma.COUNT_ATTRS


class Minimum(SignatureBasemodel):
    """Minimum: smallest value in the series."""

    default_output_field_name: ClassVar[str] = "minimum"
    default_func = sig_funcs.minimum
    default_attrs: ClassVar[Dict] = tma.MINIMUM_ATTRS


class Maximum(SignatureBasemodel):
    """Maximum: largest value in the series."""

    default_output_field_name: ClassVar[str] = "maximum"
    default_func = sig_funcs.maximum
    default_attrs: ClassVar[Dict] = tma.MAXIMUM_ATTRS


class Average(SignatureBasemodel):
    """Average: arithmetic mean of the series."""

    default_output_field_name: ClassVar[str] = "average"
    default_func = sig_funcs.average
    default_attrs: ClassVar[Dict] = tma.AVERAGE_ATTRS


class Sum(SignatureBasemodel):
    """Sum: total of all values in the series."""

    default_output_field_name: ClassVar[str] = "sum"
    default_func = sig_funcs.sum
    default_attrs: ClassVar[Dict] = tma.SUM_ATTRS


class Variance(SignatureBasemodel):
    """Variance: statistical variance of the series."""

    default_output_field_name: ClassVar[str] = "variance"
    default_func = sig_funcs.variance
    default_attrs: ClassVar[Dict] = tma.VARIANCE_ATTRS


# =============================================================================
# Signatures Requiring Additional Fields
# =============================================================================

class MaxValueTime(SignatureBasemodel):
    """Max Value Time: timestamp when the maximum value occurs.

    This signature requires both primary_value and value_time fields.
    """

    default_output_field_name: ClassVar[str] = "max_value_time"
    default_func = sig_funcs.max_value_time
    default_attrs: ClassVar[Dict] = tma.MAX_VAL_TIME_ATTRS
    default_input_field_names: ClassVar[List[str]] = ["primary_value", "value_time"]

    return_type: str = Field(default="timestamp", frozen=True)


class FlowDurationCurveSlope(SignatureBasemodel):
    """Flow Duration Curve Slope: slope of the FDC between quantiles.

    Additional Parameters
    ---------------------
    lower_quantile : float
        The lower exceedance probability quantile, by default 0.25.
    upper_quantile : float
        The upper exceedance probability quantile, by default 0.85.
    as_percentile : bool
        Whether to express exceedance probability as percentile (0-100)
        or fraction (0-1), by default False.
    """

    default_output_field_name: ClassVar[str] = "flow_duration_curve_slope"
    default_func = sig_funcs.flow_duration_curve_slope
    default_attrs: ClassVar[Dict] = tma.FDC_SLOPE_ATTRS

    lower_quantile: float = Field(default=0.25)
    upper_quantile: float = Field(default=0.85)
    as_percentile: bool = Field(default=False)


# =============================================================================
# Container Class for Discovery
# =============================================================================

class Signatures:
    """Define and customize signature metrics (single-field statistics).

    Notes
    -----
    Signatures operate on a single field (typically primary_value) to
    characterize timeseries properties. Available signatures:

    **Basic Statistics:**
    - Count, Minimum, Maximum, Average, Sum, Variance

    **Time-Based:**
    - MaxValueTime

    **Hydrologic:**
    - FlowDurationCurveSlope

    Example
    -------
    >>> from teehr import Signatures
    >>> avg = Signatures.Average()
    >>> fdc = Signatures.FlowDurationCurveSlope(lower_quantile=0.33, upper_quantile=0.66)
    """

    # Basic statistics
    Count = Count
    Minimum = Minimum
    Maximum = Maximum
    Average = Average
    Sum = Sum
    Variance = Variance

    # Time-based
    MaxValueTime = MaxValueTime

    # Hydrologic signatures
    FlowDurationCurveSlope = FlowDurationCurveSlope
