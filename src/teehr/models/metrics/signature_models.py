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


class COUNT(DeterministicBasemodel):
    """Count.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "primary_count".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.count`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    output_field_name: str = Field(default="count")
    func: Callable = sig_funcs.count
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.COUNT_ATTRS, frozen=True)


class MINIMUM(DeterministicBasemodel):
    """Minimum.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "primary_minimum".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.minimum`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="minimum")
    func: Callable = sig_funcs.minimum
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.MINIMUM_ATTRS, frozen=True)


class MAXIMUM(DeterministicBasemodel):
    """Maximum.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "maximum".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.maximum`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="maximum")
    func: Callable = sig_funcs.maximum
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.MAXIMUM_ATTRS, frozen=True)


class AVERAGE(DeterministicBasemodel):
    """Average.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "average".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.average`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="average")
    func: Callable = sig_funcs.average
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.AVERAGE_ATTRS, frozen=True)


class SUM(DeterministicBasemodel):
    """Sum.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "sum".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.sum`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="sum")
    func: Callable = sig_funcs.sum
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.SUM_ATTRS, frozen=True)


class VARIANCE(DeterministicBasemodel):
    """Variance.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "variance".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.variance`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="variance")
    func: Callable = sig_funcs.variance
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.VARIANCE_ATTRS, frozen=True)


class MAX_VALUE_TIME(DeterministicBasemodel):
    """Max Value Time.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "max_value_time".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.max_value_time`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_time")
    func: Callable = sig_funcs.max_value_time
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.MAX_VAL_TIME_ATTRS, frozen=True)


class SignatureMetrics:
    """Define and customize performance metrics."""

    Average = AVERAGE
    Count = COUNT
    MaxValueTime = MAX_VALUE_TIME
    Maximum = MAXIMUM
    Minimum = MINIMUM
    Sum = SUM
    Variance = VARIANCE
