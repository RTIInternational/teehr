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


class ME(DeterministicBasemodel):
    """Mean Error.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_error")
    func: Callable = metric_funcs.mean_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.ME_ATTRS, frozen=True)


class REL_BIAS(DeterministicBasemodel):
    """Relative Bias.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="relative_bias")
    func: Callable = metric_funcs.relative_bias
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RBIAS_ATTRS, frozen=True)


class MULT_BIAS(DeterministicBasemodel):
    """Multiplicative Bias.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="multiplicative_bias")
    func: Callable = metric_funcs.multiplicative_bias
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MULTBIAS_ATTRS, frozen=True)


class MSE(DeterministicBasemodel):
    """Mean Square Error.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_square_error")
    func: Callable = metric_funcs.mean_squared_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MSE_ATTRS, frozen=True)


class RMSE(DeterministicBasemodel):
    """Root Mean Squared Error.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_square_error")
    func: Callable = metric_funcs.root_mean_squared_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMSE_ATTRS, frozen=True)


class MAE(DeterministicBasemodel):
    """Mean Absolute Error.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_error")
    func: Callable = metric_funcs.mean_absolute_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MAE_ATTRS, frozen=True)


class REL_MAE(DeterministicBasemodel):
    """Relative Mean Absolute Error.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_relative_error")
    func: Callable = metric_funcs.mean_absolute_relative_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMAE_ATTRS, frozen=True)


class PEARSON_R(DeterministicBasemodel):
    """Pearson Correlation.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="pearson_correlation")
    func: Callable = metric_funcs.pearson_correlation
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.PEARSON_ATTRS, frozen=True)


class R2(DeterministicBasemodel):
    """Coefficient of Determination.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="r_squared")
    func: Callable = metric_funcs.r_squared
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.R2_ATTRS, frozen=True)


class NSE(DeterministicBasemodel):
    """Nash-Sutcliffe Efficiency.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="nash_sutcliffe_efficiency")
    func: Callable = metric_funcs.nash_sutcliffe_efficiency
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NSE_ATTRS, frozen=True)


class NNSE(DeterministicBasemodel):
    """Normalized Nash-Sutcliffe Efficiency.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(
        default="nash_sutcliffe_efficiency_normalized"
    )
    func: Callable = metric_funcs.nash_sutcliffe_efficiency_normalized
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NNSE_ATTRS, frozen=True)


class KGE(DeterministicBasemodel):
    """Kling-Gupta Efficiency.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "kling_gupta_efficiency".
    func : Callable
        The function to apply to the data, by default
        :func:`deterministic_funcs.kling_gupta_efficiency`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency")
    func: Callable = metric_funcs.kling_gupta_efficiency
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE_ATTRS, frozen=True)


class KGE_Mod1(DeterministicBasemodel):
    """Kling-Gupta Efficiency - modified 1 (2012).

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "kling_gupta_efficiency_mod1".
    func : Callable
        The function to apply to the data, by default
        :func:`kling_gupta_efficiency_mod1`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod1")
    func: Callable = metric_funcs.kling_gupta_efficiency_mod1
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE1_ATTRS, frozen=True)


class KGE_Mod2(DeterministicBasemodel):
    """Kling-Gupta Efficiency - modified 2 (2021).

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "kling_gupta_efficiency_mod2".
    func : Callable
        The function to apply to the data, by default
        :func:`kling_gupta_efficiency_mod2`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: BootstrapBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod2")
    func: Callable = metric_funcs.kling_gupta_efficiency_mod2
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE2_ATTRS, frozen=True)


class SPEARMAN_R(DeterministicBasemodel):
    """Spearman Rank Correlation Coefficient.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="spearman_correlation")
    func: Callable = metric_funcs.spearman_correlation
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.SPEARMAN_R_ATTRS, frozen=True)


class MAX_VALUE_DELTA(DeterministicBasemodel):
    """Max Value Delta.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_delta")
    func: Callable = metric_funcs.max_value_delta
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MAX_VALUE_DELTA_ATTRS, frozen=True)


class MAX_VALUE_TIME_DELTA(DeterministicBasemodel):
    """Max Value Time Delta.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_time_delta")
    func: Callable = metric_funcs.max_value_timedelta
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.MAX_VALUE_TIMEDELTA_ATTRS, frozen=True)


class ANNUAL_PEAK_RBIAS(DeterministicBasemodel):
    """Annual Peak Relative Bias.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="annual_peak_flow_bias")
    func: Callable = metric_funcs.annual_peak_relative_bias
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.ANNUAL_PEAK_RBIAS_ATTRS, frozen=True)


class RSR(DeterministicBasemodel):
    """Root Mean Standard Deviation Ratio.

    Parameters
    ----------
    bootstrap : DeterministicBasemodel
        The bootstrap model, by default None.
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
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_standard_deviation_ratio")  # noqa: E501
    func: Callable = metric_funcs.root_mean_standard_deviation_ratio
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RSR_ATTRS, frozen=True)


class DeterministicMetrics():
    """Define and customize performance metrics."""

    AnnualPeakRelativeBias = ANNUAL_PEAK_RBIAS
    KlingGuptaEfficiency = KGE
    KlingGuptaEfficiencyMod1 = KGE_Mod1
    KlingGuptaEfficiencyMod2 = KGE_Mod2
    MaxValueDelta = MAX_VALUE_DELTA
    MaxValueTimeDelta = MAX_VALUE_TIME_DELTA
    MeanError = ME
    MeanAbsoluteError = MAE
    MeanAbsoluteRelativeError = REL_MAE
    MeanSquareError = MSE
    MultiplicativeBias = MULT_BIAS
    PearsonCorrelation = PEARSON_R
    NashSutcliffeEfficiency = NSE
    NormalizedNashSutcliffeEfficiency = NNSE
    RelativeBias = REL_BIAS
    RootMeanSquareError = RMSE
    Rsquared = R2
    SpearmanCorrelation = SPEARMAN_R
    RootMeanStandardDeviationRatio = RSR
