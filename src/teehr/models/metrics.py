"""Metric Query Models."""
from typing import List, Optional, Dict
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
    """

    method: BootstrapMethodEnum = Field(default="percentile")
    num_samples: int = Field(default=1000)
    seed: Optional[int] = Field(default=None)
    quantiles: Optional[List[float]] = Field(default=None)


KGE_ATTRIBUTES = {
    "short_name": "KGE",
    "display_name": "Kling-Gupta Efficiency",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "version": "v0.3.13"
}


class KGE(MetricsBasemodel):
    """Kling-Gupta Efficiency."""

    # User-defined properties of the metric.
    bootstrap: Bootstrap = Field(default=None)
    transform: TransformEnum = Field(default=None)
    output_field_name: str = Field(default="kling_gupta_efficiency")
    # Static, database properties of the metric.
    attrs: Dict = Field(default=KGE_ATTRIBUTES, frozen=True)

    # @computed_field
    # @property
    # def attrs(self) -> Dict:
    #     """Initialize the KGE attributes. Could do one at a time."""
    #     return KGE_ATTRIBUTES


class RMSE(MetricsBasemodel):
    """Root Mean Squared Error."""

    short_name: str = "RMSE"
    display_name: str = "Root Mean Squared Error"
    value_type: str = "continuous"
    range: List[float] = [0.0, None]
    target: float = 0.0
    transform: Optional[str] = None
    bootstrap: Optional[bool] = None
    quantiles: Optional[List[float]] = None


class Metrics():
    """Define and customize performance metrics."""

    KGE = KGE
    RMSE = RMSE
