"""Classes representing available performance metrics."""
from typing import List, Dict, Callable, Union

from pydantic import Field
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.basemodels import (
    TransformEnum,
    DeterministicBasemodel,
    BootstrapBasemodel
)
from teehr.metrics import deterministic_funcs as metric_funcs
from teehr.models.str_enum import StrEnum


class MeanError(DeterministicBasemodel):
    """Mean Error.

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
        The output field name, by default
        "mean_error".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.mean_error.`
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_error")
    func: Callable = Field(metric_funcs.mean_error, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.ME_ATTRS, frozen=True)


class RelativeBias(DeterministicBasemodel):
    """Relative Bias.

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
        The output field name, by default
        "relative_bias".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.relative_bias`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="relative_bias")
    func: Callable = Field(metric_funcs.relative_bias, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RBIAS_ATTRS, frozen=True)


class MultiplicativeBias(DeterministicBasemodel):
    """Multiplicative Bias.

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
        The output field name, by default
        "multiplicative_bias".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.multiplicative_bias.`
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="multiplicative_bias")
    func: Callable = Field(metric_funcs.multiplicative_bias, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MULTBIAS_ATTRS, frozen=True)


class MeanSquareError(DeterministicBasemodel):
    """Mean Square Error.

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
        The output field name, by default
        "mean_squared_error".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.mean_squared_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_square_error")
    func: Callable = Field(metric_funcs.mean_squared_error, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MSE_ATTRS, frozen=True)


class RootMeanSquareError(DeterministicBasemodel):
    """Root Mean Squared Error.

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
        The output field name, by default
        "root_mean_square_error".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.root_mean_square_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_square_error")
    func: Callable = Field(metric_funcs.root_mean_squared_error, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMSE_ATTRS, frozen=True)


class MeanAbsoluteError(DeterministicBasemodel):
    """Mean Absolute Error.

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
        The output field name, by default
        "mean_absolute_error".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.mean_absolute_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_error")
    func: Callable = Field(metric_funcs.mean_absolute_error, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MAE_ATTRS, frozen=True)


class MeanAbsoluteRelativeError(DeterministicBasemodel):
    """Relative Mean Absolute Error.

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
        The output field name, by default
        "mean_absolute_relative_error".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.mean_absolute_relative_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_relative_error")
    func: Callable = Field(metric_funcs.mean_absolute_relative_error,
                           frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMAE_ATTRS, frozen=True)


class PearsonCorrelation(DeterministicBasemodel):
    """Pearson Correlation.

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
        The output field name, by default
        "pearson_correlation".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.pearson_correlation`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="pearson_correlation")
    func: Callable = Field(metric_funcs.pearson_correlation, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.PEARSON_ATTRS, frozen=True)


class VariabilityRatio(DeterministicBasemodel):
    """Variability Ratio.

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
        The output field name, by default "variability_ratio".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.variability_ratio`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="variability_ratio")
    func: Callable = Field(metric_funcs.variability_ratio, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.VR_ATTRS, frozen=True)


class Rsquared(DeterministicBasemodel):
    """Coefficient of Determination.

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
        The output field name, by default
        "r_squared".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.r_squared`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="r_squared")
    func: Callable = Field(metric_funcs.r_squared, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.R2_ATTRS, frozen=True)


class NashSutcliffeEfficiency(DeterministicBasemodel):
    """Nash-Sutcliffe Efficiency.

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
        The output field name, by default
        "nash_sutcliffe_efficiency".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.nash_sutcliffe_efficiency`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="nash_sutcliffe_efficiency")
    func: Callable = Field(metric_funcs.nash_sutcliffe_efficiency, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NSE_ATTRS, frozen=True)


class NormalizedNashSutcliffeEfficiency(DeterministicBasemodel):
    """Normalized Nash-Sutcliffe Efficiency.

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
        The output field name, by default
        "nash_sutcliffe_efficiency_normalized".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.nash_sutcliffe_efficiency_normalized`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(
        default="nash_sutcliffe_efficiency_normalized"
    )
    func: Callable = Field(metric_funcs.nash_sutcliffe_efficiency_normalized,
                           frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NNSE_ATTRS, frozen=True)


class KlingGuptaEfficiency(DeterministicBasemodel):
    """Kling-Gupta Efficiency.

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
        The output field name, by default "kling_gupta_efficiency".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.kling_gupta_efficiency`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    sr : float
        The scaling factor for the correlation component, by default 1.0.
    sa : float
        The scaling factor for the variability component, by default 1.0.
    sb : float
        The scaling factor for the bias component, by default 1.0.
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency")
    func: Callable = Field(metric_funcs.kling_gupta_efficiency, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    sr: float = Field(default=1.0)
    sa: float = Field(default=1.0)
    sb: float = Field(default=1.0)
    attrs: Dict = Field(default=tma.KGE_ATTRS, frozen=True)


class KlingGuptaEfficiencyMod1(DeterministicBasemodel):
    """Kling-Gupta Efficiency - modified 1 (2012).

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
        The output field name, by default "kling_gupta_efficiency_mod1".
    func : Callable
        The function to apply to the data, by default
        :func:`kling_gupta_efficiency_mod1`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    sr : float
        The scaling factor for the correlation component, by default 1.0.
    sa : float
        The scaling factor for the variability component, by default 1.0.
    sb : float
        The scaling factor for the bias component, by default 1.0.
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod1")
    func: Callable = Field(metric_funcs.kling_gupta_efficiency_mod1,
                           frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    sr: float = Field(default=1.0)
    sa: float = Field(default=1.0)
    sb: float = Field(default=1.0)
    attrs: Dict = Field(default=tma.KGE1_ATTRS, frozen=True)


class KlingGuptaEfficiencyMod2(DeterministicBasemodel):
    """Kling-Gupta Efficiency - modified 2 (2021).

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
        The output field name, by default "kling_gupta_efficiency_mod2".
    func : Callable
        The function to apply to the data, by default
        :func:`kling_gupta_efficiency_mod2`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    sr : float
        The scaling factor for the correlation component, by default 1.0.
    sa : float
        The scaling factor for the variability component, by default 1.0.
    sb : float
        The scaling factor for the bias component, by default 1.0.
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod2")
    func: Callable = Field(metric_funcs.kling_gupta_efficiency_mod2,
                           frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    sr: float = Field(default=1.0)
    sa: float = Field(default=1.0)
    sb: float = Field(default=1.0)
    attrs: Dict = Field(default=tma.KGE2_ATTRS, frozen=True)


class SpearmanCorrelation(DeterministicBasemodel):
    """Spearman Rank Correlation Coefficient.

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
        The output field name, by default "spearman_correlation".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.spearman_correlation`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="spearman_correlation")
    func: Callable = Field(metric_funcs.spearman_correlation, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.SPEARMAN_R_ATTRS, frozen=True)


class MaxValueDelta(DeterministicBasemodel):
    """Max Value Delta.

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
        The output field name, by default "max_value_delta".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.max_value_delta`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_delta")
    func: Callable = Field(metric_funcs.max_value_delta, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MAX_VALUE_DELTA_ATTRS, frozen=True)


class MaxValueTimeDelta(DeterministicBasemodel):
    """Max Value Time Delta.

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
        The output field name, by default "max_value_timedelta".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.max_value_timedelta`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default
        ["primary_value", "secondary_value", "value_time"].
    attrs : Dict
        The static attributes for the metric.
    """

    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_time_delta")
    func: Callable = Field(metric_funcs.max_value_timedelta, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.MAX_VALUE_TIMEDELTA_ATTRS, frozen=True)


class AnnualPeakRelativeBias(DeterministicBasemodel):
    """Annual Peak Relative Bias.

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
        The output field name, by default "annual_peak_relative_bias".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.annual_peak_relative_bias`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default
        ["primary_value", "secondary_value", "value_time"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="annual_peak_flow_bias")
    func: Callable = Field(metric_funcs.annual_peak_relative_bias, frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.ANNUAL_PEAK_RBIAS_ATTRS, frozen=True)


class RootMeanStandardDeviationRatio(DeterministicBasemodel):
    """Root Mean Standard Deviation Ratio.

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
        The output field name, by default "root_mean_standard_deviation_ratio".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.root_mean_standard_deviation_ratio`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default
        ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    add_epsilon: bool = Field(default=False)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_standard_deviation_ratio")  # noqa: E501
    func: Callable = Field(metric_funcs.root_mean_standard_deviation_ratio,
                           frozen=True)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RSR_ATTRS, frozen=True)


class DeterministicMetrics:
    """Define and customize determinisitic metrics.

    Notes
    -----
    Deterministic metrics compare two timeseries, typically primary
    ("observed") vs. secondary ("modeled") values. Available metrics include:

    - AnnualPeakRelativeBias
    - KlingGuptaEfficiency
    - KlingGuptaEfficiencyMod1
    - KlingGuptaEfficiencyMod2
    - MaxValueDelta
    - MaxValueTimeDelta
    - MeanError
    - MeanAbsoluteError
    - MeanAbsoluteRelativeError
    - MeanSquareError
    - MultiplicativeBias
    - PearsonCorrelation
    - NashSutcliffeEfficiency
    - NormalizedNashSutcliffeEfficiency
    - RelativeBias
    - RootMeanSquareError
    - Rsquared
    - SpearmanCorrelation
    - RootMeanStandardDeviationRatio
    """

    AnnualPeakRelativeBias = AnnualPeakRelativeBias
    KlingGuptaEfficiency = KlingGuptaEfficiency
    KlingGuptaEfficiencyMod1 = KlingGuptaEfficiencyMod1
    KlingGuptaEfficiencyMod2 = KlingGuptaEfficiencyMod2
    MaxValueDelta = MaxValueDelta
    MaxValueTimeDelta = MaxValueTimeDelta
    MeanError = MeanError
    MeanAbsoluteError = MeanAbsoluteError
    MeanAbsoluteRelativeError = MeanAbsoluteRelativeError
    MeanSquareError = MeanSquareError
    MultiplicativeBias = MultiplicativeBias
    PearsonCorrelation = PearsonCorrelation
    VariabilityRatio = VariabilityRatio
    NashSutcliffeEfficiency = NashSutcliffeEfficiency
    NormalizedNashSutcliffeEfficiency = NormalizedNashSutcliffeEfficiency
    RelativeBias = RelativeBias
    RootMeanSquareError = RootMeanSquareError
    Rsquared = Rsquared
    SpearmanCorrelation = SpearmanCorrelation
    RootMeanStandardDeviationRatio = RootMeanStandardDeviationRatio
