"""Enums and Basemodels for metric classes."""
from typing import Union, Callable, List, Dict, Any, ClassVar

from teehr.models.str_enum import StrEnum
from teehr.querying.utils import unpack_sdf_dict_columns

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict, model_validator
from pyspark.sql import types as T


# Pydantic BaseModel configurations
class MetricsBasemodel(PydanticBaseModel):
    """Metrics Basemodel configuration."""

    return_type: Union[str, T.ArrayType, T.MapType] = Field(default=None)
    unpack_results: bool = Field(default=False)
    unpack_function: Callable = Field(default=None)
    reference_configuration: str = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )


class ProbabilisticBasemodel(MetricsBasemodel):
    """Probabilistic Basemodel configuration."""

    @model_validator(mode="before")
    def update_return_type(cls, values):
        """Update the return type based on the summary function."""
        if values.get("summary_func") is None:
            values["return_type"] = T.ArrayType(T.FloatType())
        elif values.get("summary_func") is not None:
            values["return_type"] = "float"
        return values


class BootstrapBasemodel(PydanticBaseModel):
    """Bootstrap Basemodel configuration."""

    return_type: Union[str, T.ArrayType, T.MapType] = Field(default=None)
    reps: int = 1000
    seed: Union[int, None] = None
    quantiles: Union[List[float], None] = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )

    @model_validator(mode="before")
    def update_return_type(cls, values):
        """Update the return type based on the quantiles."""
        if values.get("quantiles") is None:
            values["return_type"] = T.ArrayType(T.FloatType())
        elif values.get("quantiles") is not None:
            values["return_type"] = T.MapType(T.StringType(), T.FloatType())
        return values


class DeterministicBasemodel(MetricsBasemodel):
    """Deterministic Basemodel configuration.

    This base class provides common fields for all deterministic metrics.
    Subclasses should define class-level defaults for metric-specific values.

    Class Variables (to be defined by subclasses)
    ---------------------------------------------
    default_output_field_name : str
        Default output field name for this metric.
    default_func : Callable
        Default function to compute this metric.
    default_attrs : Dict
        Default static attributes for this metric.
    default_input_field_names : List[str]
        Default input field names (override for signatures).
    """

    # Common fields with defaults - users can override at instantiation
    bootstrap: Any = Field(default=None)  # BootstrapBasemodel, but avoid circular import
    add_epsilon: bool = Field(default=False)
    transform: Any = Field(default=None)  # TransformEnum, set below after enum defined

    # Metric-specific fields - subclasses define defaults via class variables
    output_field_name: str = Field(default=None)
    func: Callable = Field(default=None)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(default=None)
    attrs: Dict = Field(default=None)

    # Base class defaults
    unpack_function: Callable = Field(default=unpack_sdf_dict_columns)
    return_type: Union[str, T.ArrayType, T.MapType] = Field(default="float")

    # Class-level defaults (subclasses override these) - ClassVar tells Pydantic to skip
    default_output_field_name: ClassVar[str] = None
    default_func: ClassVar[Callable] = None
    default_attrs: ClassVar[Dict] = None
    default_input_field_names: ClassVar[List[str]] = ["primary_value", "secondary_value"]

    @model_validator(mode="before")
    @classmethod
    def apply_class_defaults(cls, values: Dict) -> Dict:
        """Apply class-level defaults for metric-specific fields."""
        if values.get("output_field_name") is None and cls.default_output_field_name:
            values["output_field_name"] = cls.default_output_field_name
        if values.get("func") is None and cls.default_func:
            values["func"] = cls.default_func
        if values.get("attrs") is None and cls.default_attrs:
            values["attrs"] = cls.default_attrs
        if values.get("input_field_names") is None and cls.default_input_field_names:
            values["input_field_names"] = list(cls.default_input_field_names)
        return values


class SignatureBasemodel(DeterministicBasemodel):
    """Signature Basemodel configuration.

    Signatures operate on a single field (primary_value by default).
    """

    default_input_field_names: ClassVar[List[str]] = ["primary_value"]


class ThresholdBasemodel(DeterministicBasemodel):
    """Threshold-based metric Basemodel configuration.

    For metrics that require a threshold field (e.g., confusion matrix,
    false alarm ratio, probability of detection).
    """

    threshold_field_name: str = Field(default=None)


# Enums
class Operators(StrEnum):
    """Operators for filters."""

    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class ValueTypeEnum(StrEnum):
    """Value types."""

    Deterministic = "Deterministic"
    Categorical = "Categorical"
    Signature = "Signature"
    Probabilistic = "Probabilistic"
    Time = "Time"


class BootstrapMethodEnum(StrEnum):
    """Bootstrap methods."""

    percentile = "percentile"
    bias_corrected = "bias_corrected"
    bias_corrected_percentile = "bias_corrected_percentile"
    t = "t"
    pivot = "pivot"


class TransformEnum(StrEnum):
    """Transform methods."""

    log = "log"
    sqrt = "sqrt"
    square = "square"
    cube = "cube"
    exp = "exp"
    inv = "inv"
    abs = "abs"
    none = "none"


class CRPSEstimators(StrEnum):
    """CRPS Estimators."""

    pwm = "pwm"
    nrg = "nrg"
    fair = "fair"


class MetricCategories(StrEnum):
    """Metric categories."""

    Deterministic = "Deterministic"
    Categorical = "Categorical"
    Signature = "Signature"
    Probabilistic = "Probabilistic"
