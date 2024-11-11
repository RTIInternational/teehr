"""Classes representing available performance metrics."""
from typing import List, Dict, Callable, Union

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.metric_enums import (
    TransformEnum
)
from teehr.metrics import metric_funcs as metric_funcs
from teehr.models.str_enum import StrEnum


class MetricsBasemodel(PydanticBaseModel):
    """Metrics Basemodel configuration."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )


class ME(MetricsBasemodel):
    """Mean Error.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "mean_error".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.mean_error.`
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_error")
    func: Callable = metric_funcs.mean_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.ME_ATTRS, frozen=True)


class REL_BIAS(MetricsBasemodel):
    """Relative Bias.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "relative_bias".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.relative_bias`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="relative_bias")
    func: Callable = metric_funcs.relative_bias
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RBIAS_ATTRS, frozen=True)


class MULT_BIAS(MetricsBasemodel):
    """Multiplicative Bias.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "multiplicative_bias".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.multiplicative_bias.`
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="multiplicative_bias")
    func: Callable = metric_funcs.multiplicative_bias
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MULTBIAS_ATTRS, frozen=True)


class MSE(MetricsBasemodel):
    """Mean Square Error.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "mean_squared_error".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.mean_squared_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_square_error")
    func: Callable = metric_funcs.mean_squared_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MSE_ATTRS, frozen=True)


class RMSE(MetricsBasemodel):
    """Root Mean Squared Error.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "root_mean_square_error".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.root_mean_square_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_square_error")
    func: Callable = metric_funcs.root_mean_squared_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMSE_ATTRS, frozen=True)


class MAE(MetricsBasemodel):
    """Mean Absolute Error.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "mean_absolute_error".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.mean_absolute_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_error")
    func: Callable = metric_funcs.mean_absolute_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MAE_ATTRS, frozen=True)


class REL_MAE(MetricsBasemodel):
    """Relative Mean Absolute Error.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "mean_absolute_relative_error".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.mean_absolute_relative_error`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_relative_error")
    func: Callable = metric_funcs.mean_absolute_relative_error
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMAE_ATTRS, frozen=True)


class PEARSON_R(MetricsBasemodel):
    """Pearson Correlation.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "pearson_correlation".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.pearson_correlation`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="pearson_correlation")
    func: Callable = metric_funcs.pearson_correlation
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.PEARSON_ATTRS, frozen=True)


class R2(MetricsBasemodel):
    """Coefficient of Determination.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "r_squared".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.r_squared`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="r_squared")
    func: Callable = metric_funcs.r_squared
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.R2_ATTRS, frozen=True)


class NSE(MetricsBasemodel):
    """Nash-Sutcliffe Efficiency.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "nash_sutcliffe_efficiency".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.nash_sutcliffe_efficiency`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="nash_sutcliffe_efficiency")
    func: Callable = metric_funcs.nash_sutcliffe_efficiency
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NSE_ATTRS, frozen=True)


class NNSE(MetricsBasemodel):
    """Normalized Nash-Sutcliffe Efficiency.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default
        "nash_sutcliffe_efficiency_normalized".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.nash_sutcliffe_efficiency_normalized`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(
        default="nash_sutcliffe_efficiency_normalized"
    )
    func: Callable = metric_funcs.nash_sutcliffe_efficiency_normalized
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NNSE_ATTRS, frozen=True)


class KGE(MetricsBasemodel):
    """Kling-Gupta Efficiency.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "kling_gupta_efficiency".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.kling_gupta_efficiency`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency")
    func: Callable = metric_funcs.kling_gupta_efficiency
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE_ATTRS, frozen=True)


class KGE_Mod1(MetricsBasemodel):
    """Kling-Gupta Efficiency - modified 1 (2012).

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod1")
    func: Callable = metric_funcs.kling_gupta_efficiency_mod1
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE1_ATTRS, frozen=True)


class KGE_Mod2(MetricsBasemodel):
    """Kling-Gupta Efficiency - modified 2 (2021).

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod2")
    func: Callable = metric_funcs.kling_gupta_efficiency_mod2
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE2_ATTRS, frozen=True)


class SPEARMAN_R(MetricsBasemodel):
    """Spearman Rank Correlation Coefficient.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "spearman_correlation".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.spearman_correlation`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="spearman_correlation")
    func: Callable = metric_funcs.spearman_correlation
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.SPEARMAN_R_ATTRS, frozen=True)


class COUNT(MetricsBasemodel):
    """Count.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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
    func: Callable = metric_funcs.count
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.COUNT_ATTRS, frozen=True)


class MINIMUM(MetricsBasemodel):
    """Minimum.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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
    func: Callable = metric_funcs.minimum
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.MINIMUM_ATTRS, frozen=True)


class MAXIMUM(MetricsBasemodel):
    """Maximum.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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
    func: Callable = metric_funcs.maximum
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.MAXIMUM_ATTRS, frozen=True)


class AVERAGE(MetricsBasemodel):
    """Average.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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
    func: Callable = metric_funcs.average
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.AVERAGE_ATTRS, frozen=True)


class SUM(MetricsBasemodel):
    """Sum.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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
    func: Callable = metric_funcs.sum
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.SUM_ATTRS, frozen=True)


class VARIANCE(MetricsBasemodel):
    """Variance.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="variance")
    func: Callable = metric_funcs.variance
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.VARIANCE_ATTRS, frozen=True)


class MAX_VALUE_DELTA(MetricsBasemodel):
    """Max Value Delta.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "max_value_delta".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.max_value_delta`.
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


class MAX_VALUE_TIME_DELTA(MetricsBasemodel):
    """Max Value Time Delta.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "max_value_timedelta".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.max_value_timedelta`.
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


class MAX_VALUE_TIME(MetricsBasemodel):
    """Max Value Time.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
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
    func: Callable = metric_funcs.max_value_time
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.MAX_VAL_TIME_ATTRS, frozen=True)


class ANNUAL_PEAK_RBIAS(MetricsBasemodel):
    """Annual Peak Relative Bias.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "annual_peak_relative_bias".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.annual_peak_relative_bias`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default
        ["primary_value", "secondary_value", "value_time"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="annual_peak_flow_bias")
    func: Callable = metric_funcs.annual_peak_relative_bias
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.ANNUAL_PEAK_RBIAS_ATTRS, frozen=True)


class RSR(MetricsBasemodel):
    """Root Mean Standard Deviation Ratio.

    Parameters
    ----------
    bootstrap : MetricsBasemodel
        The bootstrap model, by default None.
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    output_field_name : str
        The output field name, by default "root_mean_standard_deviation_ratio".
    func : Callable
        The function to apply to the data, by default
        :func:`metric_funcs.root_mean_standard_deviation_ratio`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default
        ["primary_value", "secondary_value"].
    attrs : Dict
        The static attributes for the metric.
    """

    bootstrap: MetricsBasemodel = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_standard_deviation_ratio")
    func: Callable = metric_funcs.root_mean_standard_deviation_ratio
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RSR_ATTRS, frozen=True)


class Metrics():
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
    Average = AVERAGE
    Count = COUNT
    MaxValueTime = MAX_VALUE_TIME
    Maximum = MAXIMUM
    Minimum = MINIMUM
    Sum = SUM
    Variance = VARIANCE
    NashSutcliffeEfficiency = NSE
    NormalizedNashSutcliffeEfficiency = NNSE
    RelativeBias = REL_BIAS
    RootMeanSquareError = RMSE
    Rsquared = R2
    SpearmanCorrelation = SPEARMAN_R
    RootMeanStandardDeviationRatio = RSR
