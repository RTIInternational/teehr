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
    """Probabilistic Basemodel configuration.

    This base class provides common fields for all probabilistic metrics.
    Subclasses should define metric-specific fields and defaults.

    Parameters
    ----------
    transform : TransformEnum
        The transformation to apply to the data, by default None.
    backend : str
        The backend to use, by default "numba". Can be ("numba" or "numpy").
    summary_func : Callable
        The function to apply to the results, by default None.
    primary_field_name : Union[str, StrEnum]
        Field name for the observed/primary value column.
    secondary_field_name : Union[str, StrEnum]
        Field name for the forecast/secondary value column.
    member_field_name : Union[str, StrEnum]
        Field name for the ensemble member column.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]], None]
        Optional legacy override for positional metric inputs.
    """

    transform: Any = Field(default=None)  # TransformEnum, set below after enum defined
    backend: str = Field(default="numba")
    summary_func: Union[Callable, None] = Field(default=None)
    primary_field_name: Union[str, StrEnum] = Field(default="primary_value")
    secondary_field_name: Union[str, StrEnum] = Field(default="secondary_value")
    member_field_name: Union[str, StrEnum] = Field(default="member")
    input_field_names: Union[
        str,
        StrEnum,
        List[Union[str, StrEnum]],
        None,
    ] = Field(default=None)

    @model_validator(mode="before")
    def update_return_type(cls, values):
        """Update the return type based on the summary function."""
        if values.get("summary_func") is None:
            values["return_type"] = T.ArrayType(T.FloatType())
        elif values.get("summary_func") is not None:
            values["return_type"] = "float"
        return values

    @model_validator(mode="after")
    def build_input_field_names(self):
        """Compose input_field_names from explicit field names by default."""
        if self.input_field_names is None:
            self.input_field_names = [
                self.primary_field_name,
                self.secondary_field_name,
                self.member_field_name,
            ]
        return self

    def get_input_field_names(self) -> List[Union[str, StrEnum]]:
        """Return concrete ordered input field names for downstream calls."""
        if self.input_field_names is None:
            names: List[Union[str, StrEnum]] = [
                self.primary_field_name,
                self.secondary_field_name,
                self.member_field_name,
            ]
        elif isinstance(self.input_field_names, list):
            names = list(self.input_field_names)
        else:
            names = [self.input_field_names]

        # keep order, drop duplicates
        deduped = list(dict.fromkeys(names))
        self.input_field_names = deduped
        return deduped


class BootstrapBasemodel(PydanticBaseModel):
    """Bootstrap Basemodel configuration.

    This base class provides common fields for all bootstrap methods.
    Subclasses should define metric-specific fields and defaults.

    Parameters
    ----------
    reps : int
        The number of bootstrap replications, by default 1000.
    seed : Union[int, None]
        The seed for the random number generator, by default None.
    quantiles : Union[List[float], None]
        The quantiles to calculate from the bootstrap results,
        by default None.
    name : str
        The name of the bootstrap method, by default None.
    include_value_time : bool
        Whether to include the value_time series in the bootstrapping
        function, by default False.
    func : Callable
        The wrapper to generate the bootstrapping function,
        by default None.
    """

    return_type: Union[str, T.ArrayType, T.MapType] = Field(default=None)
    reps: int = 1000
    seed: Union[int, None] = None
    quantiles: Union[List[float], None] = None
    name: str = Field(default=None)
    include_value_time: bool = Field(default=False)
    func: Callable = Field(default=None)

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
    default_primary_field_name : str
        Default primary field name.
    default_secondary_field_name : Union[str, None]
        Default secondary field name.
    default_value_time_field_name : Union[str, None]
        Default value_time field name when required.
    """

    # Common fields with defaults - users can override at instantiation
    bootstrap: Any = Field(default=None)  # BootstrapBasemodel, but avoid circular import
    add_epsilon: bool = Field(default=False)
    transform: Any = Field(default=None)  # TransformEnum, set below after enum defined

    # Metric-specific fields - subclasses define defaults via class variables
    output_field_name: str = Field(default=None)
    func: Callable = Field(default=None)
    primary_field_name: Union[str, StrEnum, None] = Field(default=None)
    secondary_field_name: Union[str, StrEnum, None] = Field(default=None)
    value_time_field_name: Union[str, StrEnum, None] = Field(default=None)
    threshold_field_name: Union[str, StrEnum, None] = Field(default=None)
    input_field_names: Union[
        str,
        StrEnum,
        List[Union[str, StrEnum]],
        None,
    ] = Field(default=None)
    attrs: Dict = Field(default=None)

    # Base class defaults
    unpack_function: Callable = Field(default=unpack_sdf_dict_columns)
    return_type: Union[str, T.ArrayType, T.MapType] = Field(default="float")

    # Class-level defaults (subclasses override these) - ClassVar tells Pydantic to skip
    default_output_field_name: ClassVar[str] = None
    default_func: ClassVar[Callable] = None
    default_attrs: ClassVar[Dict] = None
    default_primary_field_name: ClassVar[Union[str, None]] = "primary_value"
    default_secondary_field_name: ClassVar[Union[str, None]] = "secondary_value"
    default_value_time_field_name: ClassVar[Union[str, None]] = None

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
        if values.get("primary_field_name") is None:
            values["primary_field_name"] = cls.default_primary_field_name
        if values.get("secondary_field_name") is None:
            values["secondary_field_name"] = cls.default_secondary_field_name
        if values.get("value_time_field_name") is None:
            values["value_time_field_name"] = cls.default_value_time_field_name
        return values

    @model_validator(mode="after")
    def build_input_field_names(self):
        """Compose input_field_names from explicit field names by default."""
        if self.input_field_names is None:
            fields = []
            for fld in [
                self.primary_field_name,
                self.secondary_field_name,
                self.value_time_field_name,
            ]:
                if fld is not None:
                    fields.append(fld)

            if self.attrs is not None and self.attrs.get("requires_threshold_field", False):
                if self.threshold_field_name is not None and self.threshold_field_name not in fields:
                    fields.append(self.threshold_field_name)

            self.input_field_names = fields
        return self

    def get_input_field_names(self) -> List[Union[str, StrEnum]]:
        """Return concrete ordered input field names for downstream calls."""
        if self.input_field_names is None:
            names: List[Union[str, StrEnum]] = []
            for fld in [
                self.primary_field_name,
                self.secondary_field_name,
                self.value_time_field_name,
            ]:
                if fld is not None:
                    names.append(fld)
        elif isinstance(self.input_field_names, list):
            names = list(self.input_field_names)
        else:
            names = [self.input_field_names]

        if self.attrs is not None and self.attrs.get("requires_threshold_field", False):
            if self.threshold_field_name is not None and self.threshold_field_name not in names:
                names.append(self.threshold_field_name)

        # keep order, drop duplicates
        deduped = list(dict.fromkeys(names))
        self.input_field_names = deduped
        return deduped


class SignatureBasemodel(DeterministicBasemodel):
    """Signature Basemodel configuration.

    Signatures operate on a single field (primary_value by default).
    """

    default_primary_field_name: ClassVar[Union[str, None]] = "primary_value"
    default_secondary_field_name: ClassVar[Union[str, None]] = None


class ThresholdBasemodel(DeterministicBasemodel):
    """Threshold-based metric Basemodel configuration.

    For metrics that require a threshold field (e.g., confusion matrix,
    false alarm ratio, probability of detection).
    """

    pass


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
