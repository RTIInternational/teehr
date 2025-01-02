"""Enums and Basemodels for metric classes."""
from typing import Union

from teehr.models.str_enum import StrEnum

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict, model_validator
from pyspark.sql import types as T


# Pydantic BaseModel configurations
class MetricsBasemodel(PydanticBaseModel):
    """Metrics Basemodel configuration."""

    return_type: Union[str, T.ArrayType] = Field(default=None)

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


class DeterministicBasemodel(MetricsBasemodel):
    """Deterministic Basemodel configuration."""

    return_type: Union[str] = Field(default="float")


class BootstrapBasemodel(MetricsBasemodel):
    """Bootstrap Basemodel configuration."""

    # if model.bootstrap.quantiles is None:
    #     return_type = ARRAY_TYPE
    # else:
    #     return_type = DICT_TYPE
    @model_validator(mode="before")
    def update_return_type(cls, values):
        """Update the return type based on the summary function."""
        if values.get("quantiles") is None:
            values["return_type"] = T.ArrayType(T.FloatType())
        elif values.get("quantiles") is not None:
            values["return_type"] = T.MapType(T.StringType(), T.FloatType())
        return values


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