"""Metric Query Models."""
from typing import List, Optional, Dict, Callable
try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:  # pragma: no cover
    from enum import Enum  # pragma: no cover

    class StrEnum(str, Enum):  # pragma: no cover
        """Enum with string values."""

        pass  # pragma: no cover

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict

import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.metric_enums import (
    BootstrapMethodEnum,
    TransformEnum
)
from teehr.metrics import udfs as udfs
from teehr.models.dataset.table_enums import (
    JoinedTimeseriesFields
)


class MetricsBasemodel(PydanticBaseModel):
    """Metrics Basemodel configuration."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        # extra='forbid'  # raise an error if extra fields are passed
    )


class Bootstrap(MetricsBasemodel):
    """Bootstrap configuration.

    This will depend on the library used to calculate the bootstrap.

    scipy: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html

    gumboot (R): https://cran.r-project.org/web/packages/gumboot/gumboot.pdf

    arch: https://bashtage.github.io/arch/index.html

    Use instances of arch Bootstrap classes here?
    (StationaryBootstrap, CircularBlockBootstrap)
    """

    method: BootstrapMethodEnum = Field(default="percentile")
    num_samples: int = Field(default=1000)
    seed: Optional[int] = Field(default=None)
    quantiles: Optional[List[float]] = Field(default=None)


class ME(MetricsBasemodel):
    """Mean Error."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_error")
    func: Callable = udfs.mean_error
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.ME_ATTRS, frozen=True)


class REL_BIAS(MetricsBasemodel):
    """Relative Bias."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="relative_bias")
    func: Callable = udfs.relative_bias
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RBIAS_ATTRS, frozen=True)


class MULT_BIAS(MetricsBasemodel):
    """Multiplicative Bias."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="multiplicative_bias")
    func: Callable = udfs.multiplicative_bias
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MULTBIAS_ATTRS, frozen=True)


class MSE(MetricsBasemodel):
    """Mean Square Error."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_square_error")
    func: Callable = udfs.mean_squared_error
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MSE_ATTRS, frozen=True)


class RMSE(MetricsBasemodel):
    """Root Mean Squared Error."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="root_mean_square_error")
    func: Callable = udfs.root_mean_squared_error
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMSE_ATTRS, frozen=True)


class MAE(MetricsBasemodel):
    """Mean Absolute Error."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_error")
    func: Callable = udfs.mean_absolute_error
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.MAE_ATTRS, frozen=True)


class REL_MAE(MetricsBasemodel):
    """Relative Mean Absolute Error."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="mean_absolute_relative_error")
    func: Callable = udfs.mean_absolute_relative_error
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.RMAE_ATTRS, frozen=True)


class PEARSON_R(MetricsBasemodel):
    """Pearson Correlation."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="pearson_correlation")
    func: Callable = udfs.pearson_correlation
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.PEARSON_ATTRS, frozen=True)


class R2(MetricsBasemodel):
    """Coefficient of Determination."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="r_squared")
    func: Callable = udfs.r_squared
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.R2_ATTRS, frozen=True)


class NSE(MetricsBasemodel):
    """Nash-Sutcliffe Efficiency."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="nash_sutcliffe_efficiency")
    func: Callable = udfs.nash_sutcliffe_efficiency
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NSE_ATTRS, frozen=True)


class NNSE(MetricsBasemodel):
    """Normalized Nash-Sutcliffe Efficiency."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(
        default="nash_sutcliffe_efficiency_normalized"
    )
    func: Callable = udfs.nash_sutcliffe_efficiency_normalized
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.NNSE_ATTRS, frozen=True)


class KGE(MetricsBasemodel):
    """Kling-Gupta Efficiency."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency")
    func: Callable = udfs.kling_gupta_efficiency  # List of UDFs?
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE_ATTRS, frozen=True)


class KGE_Mod1(MetricsBasemodel):
    """Kling Gupta Efficiency - modified 1 (2012)."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod1")
    func: Callable = udfs.kling_gupta_efficiency_mod1
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE1_ATTRS, frozen=True)


class KGE_Mod2(MetricsBasemodel):
    """Kling Gupta Efficiency - modified 2 (2021)."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency_mod2")
    func: Callable = udfs.kling_gupta_efficiency_mod2
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.KGE2_ATTRS, frozen=True)


class SPEARMAN_R(MetricsBasemodel):
    """Spearman Rank Correlation Coefficient."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="spearman_correlation")
    func: Callable = udfs.spearman_correlation
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value", "secondary_value"]
    )
    attrs: Dict = Field(default=tma.SPEARMAN_R_ATTRS, frozen=True)


class PRIMARY_COUNT(MetricsBasemodel):
    """Primary Count."""

    output_field_name: str = Field(default="primary_count")
    func: Callable = udfs.primary_count
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.PRIMARY_COUNT_ATTRS, frozen=True)


class SECONDARY_COUNT(MetricsBasemodel):
    """Secondary Count."""

    output_field_name: str = Field(default="secondary_count")
    func: Callable = udfs.secondary_count
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["secondary_value"]
    )
    attrs: Dict = Field(default=tma.SECONDARY_COUNT_ATTRS, frozen=True)


class PRIMARY_MINIMUM(MetricsBasemodel):
    """Primary Minimum."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="primary_minimum")
    func: Callable = udfs.primary_minimum
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.PRIMARY_MINIMUM_ATTRS, frozen=True)


class SECONDARY_MINIMUM(MetricsBasemodel):
    """Secondary Minimum."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="secondary_minimum")
    func: Callable = udfs.secondary_minimum
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["secondary_value"]
    )
    attrs: Dict = Field(default=tma.SECONDARY_MINIMUM_ATTRS, frozen=True)


class PRIMARY_MAXIMUM(MetricsBasemodel):
    """Primary Maximum."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="primary_maximum")
    func: Callable = udfs.primary_maximum
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.PRIMARY_MAXIMUM_ATTRS, frozen=True)


class SECONDARY_MAXIMUM(MetricsBasemodel):
    """Secondary Maximum."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="secondary_maximum")
    func: Callable = udfs.secondary_maximum
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["secondary_value"]
    )
    attrs: Dict = Field(default=tma.SECONDARY_MAXIMUM_ATTRS, frozen=True)


class PRIMARY_AVERAGE(MetricsBasemodel):
    """Primary Average."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="primary_average")
    func: Callable = udfs.primary_average
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.PRIMARY_AVERAGE_ATTRS, frozen=True)


class SECONDARY_AVERAGE(MetricsBasemodel):
    """Secondary Average."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="secondary_average")
    func: Callable = udfs.secondary_average
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["secondary_value"]
    )
    attrs: Dict = Field(default=tma.SECONDARY_AVERAGE_ATTRS, frozen=True)


class PRIMARY_SUM(MetricsBasemodel):
    """Primary Sum."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="primary_sum")
    func: Callable = udfs.primary_sum
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.PRIMARY_SUM_ATTRS, frozen=True)


class SECONDARY_SUM(MetricsBasemodel):
    """Secondary Sum."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="secondary_sum")
    func: Callable = udfs.secondary_sum
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["secondary_value"]
    )
    attrs: Dict = Field(default=tma.SECONDARY_SUM_ATTRS, frozen=True)


class PRIMARY_VARIANCE(MetricsBasemodel):
    """Primary Variance."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="primary_variance")
    func: Callable = udfs.primary_variance
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.PRIMARY_VARIANCE_ATTRS, frozen=True)


class SECONDARY_VARIANCE(MetricsBasemodel):
    """Secondary Variance."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="secondary_variance")
    func: Callable = udfs.secondary_variance
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["secondary_value"]
    )
    attrs: Dict = Field(default=tma.SECONDARY_VARIANCE_ATTRS, frozen=True)


class MAX_VALUE_DELTA(MetricsBasemodel):
    """Max Value Delta."""

    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="max_value_delta")
    func: Callable = udfs.max_value_delta
    attrs: Dict = Field(default=tma.MAX_VALUE_DELTA_ATTRS, frozen=True)


# class MAX_VALUE_TIME_DELTA(MetricsBasemodel):
#     """Max Value Time Delta."""

#     transform: TransformEnum = Field(default=None)
#     output_field_name: str = Field(default="max_value_time_delta")
#     func: Callable = udfs # TODO
#     attrs: Dict = Field(default=tma.MAX_VALUE_TIMEDELTA_ATTRS, frozen=True)


# class PRIMARY_MAX_VALUE_TIME(MetricsBasemodel):
#     """Primary Max Value Time."""

#     transform: TransformEnum = Field(default=None)
#     output_field_name: str = Field(default="primary_max_value_time")
#     attrs: Dict = Field(default=tma.PRIMARY_MAX_VAL_TIME_ATTRS, frozen=True)


# class SECONDARY_MAX_VALUE_TIME(MetricsBasemodel):
#     """Secondary Max Value Time."""

#     transform: TransformEnum = Field(default=None)
#     output_field_name: str = Field(default="secondary_max_value_time")
#     attrs: Dict = Field(default=tma.SECONDARY_MAX_VAL_TIME_ATTRS, frozen=True)


class ANNUAL_PEAK_RBIAS(MetricsBasemodel):
    """Annual Peak Relative Bias."""

    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="annual_peak_flow_bias")
    func: Callable = udfs.annual_peak_relative_bias
    input_field_names: List[JoinedTimeseriesFields] = Field(
        default=["primary_value"]
    )
    attrs: Dict = Field(default=tma.ANNUAL_PEAK_RBIAS_ATTRS, frozen=True)


class Metrics():
    """Define and customize performance metrics."""

    AnnualPeakRelativeBias = ANNUAL_PEAK_RBIAS
    KlingGuptaEfficiency = KGE
    KlingGuptaEfficiencyMod1 = KGE_Mod1
    KlingGuptaEfficiencyMod2 = KGE_Mod2
    # MaxValueDelta = MAX_VALUE_DELTA
    # MaxValueTimeDelta = MAX_VALUE_TIME_DELTA
    MeanError = ME
    MeanAbsoluteError = MAE
    MeanAbsoluteRelativeError = REL_MAE
    MeanSquareError = MSE
    MultiplicativeBias = MULT_BIAS
    PearsonCorrelation = PEARSON_R
    PrimaryAverage = PRIMARY_AVERAGE
    PrimaryCount = PRIMARY_COUNT
    # PrimaryMaxValueTime = PRIMARY_MAX_VALUE_TIME
    PrimaryMaximum = PRIMARY_MAXIMUM
    PrimaryMinimum = PRIMARY_MINIMUM
    PrimarySum = PRIMARY_SUM
    PrimaryVariance = PRIMARY_VARIANCE
    NashSutcliffeEfficiency = NSE
    NormalizedNashSutcliffeEfficiency = NNSE
    RelativeBias = REL_BIAS
    RootMeanSquareError = RMSE
    Rsquared = R2
    SecondaryAverage = SECONDARY_AVERAGE
    SecondaryCount = SECONDARY_COUNT
    SecondaryMaximum = SECONDARY_MAXIMUM
    # SecondaryMaxValueTime = SECONDARY_MAX_VALUE_TIME
    SecondaryMinimum = SECONDARY_MINIMUM
    SecondarySum = SECONDARY_SUM
    SecondaryVariance = SECONDARY_VARIANCE
    SpearmanCorrelation = SPEARMAN_R
