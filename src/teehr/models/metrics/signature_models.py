"""Signature models for metrics."""
from typing import List, Dict, Callable, Union

from pydantic import Field
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.basemodels import (
    TransformEnum,
    DeterministicBasemodel,
    BootstrapBasemodel
)
from teehr.metrics import signature_funcs as sig_funcs
from teehr.models.str_enum import StrEnum


class Count(DeterministicBasemodel):
    """Count.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "primary_count".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.count`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    output_field_name: str = Field(default="count")
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    func: Callable = Field(default=sig_funcs.count, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.COUNT_ATTRS, frozen=True)


class Minimum(DeterministicBasemodel):
    """Minimum.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "primary_minimum".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.minimum`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="minimum")
    func: Callable = Field(default=sig_funcs.minimum, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.MINIMUM_ATTRS, frozen=True)


class Maximum(DeterministicBasemodel):
    """Maximum.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "maximum".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.maximum`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="maximum")
    func: Callable = Field(default=sig_funcs.maximum, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.MAXIMUM_ATTRS, frozen=True)


class Average(DeterministicBasemodel):
    """Average.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "average".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.average`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="average")
    func: Callable = Field(default=sig_funcs.average, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.AVERAGE_ATTRS, frozen=True)


class Sum(DeterministicBasemodel):
    """Sum.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "sum".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.sum`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="sum")
    func: Callable = Field(default=sig_funcs.sum, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.SUM_ATTRS, frozen=True)


class Variance(DeterministicBasemodel):
    """Variance.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "variance".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.variance`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="variance")
    func: Callable = Field(default=sig_funcs.variance, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.VARIANCE_ATTRS, frozen=True)


class MaxValueTime(DeterministicBasemodel):
    """Max Value Time.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "max_value_time".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.max_value_time`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_time")
    func: Callable = Field(default=sig_funcs.max_value_time, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.MAX_VAL_TIME_ATTRS, frozen=True)
    return_type: str = Field(default="timestamp", frozen=True)


class FlowDurationCurveSlope(DeterministicBasemodel):
    """Flow Duration Curve Slope.

    Parameters
    ----------
    bootstrap : BootstrapBasemodel
        The bootstrap model, by default None.
    add_epsilon: bool
        Whether to add epsilon to avoid issues with certain transforms or
        division by zero, by default False.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "flow_duration_curve".
    func : Callable
        The function to apply to the data, by default
        :func:`signature_funcs.flow_duration_curve`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    lower_quantile : float
        The lower quantile for slope calculation, by default 0.25.
    upper_quantile : float
        The upper quantile for slope calculation, by default 0.85.
    as_percentile : bool
        Whether calculate slope using exceedance_probability as a percentile
        (0-100) or a fraction (0-1), by default False.
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="flow_duration_curve_slope")
    func: Callable = Field(default=sig_funcs.flow_duration_curve_slope,
                           frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    lower_quantile: float = Field(default=0.25)
    upper_quantile: float = Field(default=0.85)
    as_percentile: bool = Field(default=False)
    attrs: Dict = Field(default=tma.FDC_SLOPE_ATTRS, frozen=True)


class Signatures:
    """Define and customize signatures.

    Notes
    -----
    Signatures operate on a single field.  Available
    signatures are:

    - Average
    - Count
    - MaxValueTime
    - Maximum
    - Minimum
    - Sum
    - Variance
    - FlowDurationCurveSlope
    """

    Average = Average
    Count = Count
    MaxValueTime = MaxValueTime
    Maximum = Maximum
    Minimum = Minimum
    Sum = Sum
    Variance = Variance
    FlowDurationCurveSlope = FlowDurationCurveSlope
